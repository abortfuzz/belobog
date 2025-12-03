use std::{collections::BTreeSet, marker::PhantomData, str::FromStr};

use belobog_fork::ObjectTypesStore;
use libafl::{
    mutators::{MutationResult, Mutator},
    state::HasRand,
};
use libafl_bolts::{Named, rands::Rand};
use log::debug;
use move_vm_types::values::Value;
use sui_types::{
    Identifier,
    object::Owner,
    storage::ObjectStore,
    transaction::{Argument, CallArg, Command, ObjectArg},
};

use crate::{
    input::SuiInput,
    meta::{HasCaller, HasFuzzMetadata},
    mutation_utils::MutableValue,
    object_sampler::try_construct_args_from_db,
    type_utils::TypeUtils,
};

pub struct HavocMuator<I, S, T> {
    pub ph: PhantomData<(I, S)>,
    pub db: T,
}

impl<I, S, T> HavocMuator<I, S, T> {
    pub fn new(db: T) -> Self {
        Self {
            ph: PhantomData,
            db,
        }
    }
}

impl<I, S, T> Named for HavocMuator<I, S, T> {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("move_fuzz_mutator")
    }
}

impl<I, S, T> HavocMuator<I, S, T> {
    fn mutate_arg(&mut self, state: &mut S, input: &mut I) -> MutationResult
    where
        S: HasRand + HasCaller + HasFuzzMetadata,
        I: SuiInput,
    {
        let ptb = input.ptb_mut();
        if ptb.commands.is_empty() {
            return MutationResult::Skipped;
        }
        for cmd in ptb.commands.iter_mut() {
            let Command::MoveCall(movecall) = cmd else {
                continue;
            };
            if movecall.arguments.is_empty() {
                continue;
            }
            let function = state
                .fuzz_state()
                .get_function(&movecall.package, &movecall.module, &movecall.function)
                .unwrap();
            let arg_idx_candidates = function
                .parameters
                .iter()
                .enumerate()
                .filter_map(|(i, param)| {
                    if param.is_mutable() {
                        // Skip struct parameters
                        Some(i as u16)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            if arg_idx_candidates.is_empty() {
                continue; // No valid arguments to mutate
            }
            // Randomly select an argument index to mutate
            let idx = state.rand_mut().below_or_zero(arg_idx_candidates.len());
            let idx = arg_idx_candidates[idx];
            let arg = &mut movecall.arguments[idx as usize];
            // Only mutate pure values
            if let Argument::Input(input_idx) = arg {
                let input = &mut ptb.inputs[*input_idx as usize];
                match input {
                    CallArg::Pure(value) => {
                        // Mutate the pure value
                        let function = state
                            .fuzz_state()
                            .get_function(&movecall.package, &movecall.module, &movecall.function)
                            .unwrap();
                        let param = &function.parameters[idx as usize];
                        let new_value =
                            Value::simple_deserialize(value, &param.to_type_layout()).unwrap();
                        let mut new_value = MutableValue::new(new_value);
                        new_value.mutate(state, &BTreeSet::new(), false);
                        *value = new_value.value.serialize().unwrap();
                        break; // Mutation successful, exit loop
                    }
                    _ => continue, // Only mutate pure values
                }
            }
        }
        MutationResult::Mutated
    }

    fn mutate_sequence(&mut self, state: &mut S, input: &mut I) -> MutationResult
    where
        I: SuiInput,
        T: ObjectTypesStore + ObjectStore,
        S: HasRand + HasFuzzMetadata,
    {
        let ptb = input.ptb_mut();
        let inc = if ptb.commands.len() <= 1 {
            true
        } else {
            state.rand_mut().below_or_zero(2) == 0
        };
        if !inc {
            let idx = state.rand_mut().below_or_zero(ptb.commands.len());
            ptb.commands.remove(idx);
            return MutationResult::Mutated;
        }
        let mut selected_times = 0;
        let functions = state
            .fuzz_state()
            .iter_target_functions()
            .map(|(addr, mname, module, fname, function)| {
                (
                    *addr,
                    mname.clone(),
                    module.clone(),
                    fname.clone(),
                    function.clone(),
                )
            })
            .collect::<Vec<_>>();
        loop {
            if selected_times >= 10000 {
                panic!("No suitable function found after 10000 attempts");
            }
            selected_times += 1;
            let (addr, mname, _, fname, function) = functions
                .get(state.rand_mut().below_or_zero(functions.len()))
                .expect("No functions available");
            let mut cmd = Command::move_call(
                *addr,
                Identifier::from_str(mname).unwrap(),
                Identifier::from_str(fname).unwrap(),
                vec![],
                Vec::from_iter(
                    (ptb.inputs.len()..ptb.inputs.len() + function.parameters.len())
                        .map(|i| Argument::Input(i as u16)),
                ),
            );
            let Command::MoveCall(movecall) = &mut cmd else {
                panic!("Expected MoveCall command");
            };
            debug!("Mutating function: {}::{}::{}", addr, mname, fname);
            if let Some((object_ids, ty_args)) =
                try_construct_args_from_db(movecall, &self.db, state)
            {
                movecall.type_arguments = ty_args;
                for (i, param) in function.parameters.iter().enumerate() {
                    if let Some(object_id) = object_ids.get(&(i as u16)) {
                        let Argument::Input(idx) = movecall.arguments[i] else {
                            // panic!("Expected input argument for object ID");
                            continue;
                        };
                        let Some(object) = self.db.get_object(object_id) else {
                            debug!("Object ID {:?} not found in DB", object_id);
                            return MutationResult::Skipped;
                        };
                        match object.owner {
                            Owner::AddressOwner(_) | Owner::Immutable => {
                                if idx >= ptb.inputs.len() as u16 {
                                    return MutationResult::Skipped;
                                }
                                ptb.inputs[idx as usize] =
                                    CallArg::Object(ObjectArg::ImmOrOwnedObject((
                                        *object_id,
                                        object.version(),
                                        object.digest(),
                                    )));
                            }
                            Owner::Shared {
                                initial_shared_version,
                            } => {
                                if idx >= ptb.inputs.len() as u16 {
                                    return MutationResult::Skipped;
                                }
                                ptb.inputs[idx as usize] =
                                    CallArg::Object(ObjectArg::SharedObject {
                                        id: *object_id,
                                        initial_shared_version,
                                        mutable: true,
                                    });
                            }
                            _ => {
                                debug!("Unsupported object owner type: {:?}", object.owner);
                                return MutationResult::Skipped;
                            }
                        }
                    } else {
                        if param.is_tx_context() || param.needs_sample() {
                            // Skip tx context parameters
                            ptb.inputs.push(CallArg::Pure(Vec::new()));
                            continue;
                        }
                        let init_value = param.gen_value();
                        ptb.inputs.push(CallArg::Pure(
                            init_value
                                .typed_serialize(&function.parameters[i].to_type_layout())
                                .unwrap(),
                        ));
                    }
                }
                ptb.commands.push(cmd);
                break;
            }
        }
        MutationResult::Mutated
    }
}

impl<I, S, T> Mutator<I, S> for HavocMuator<I, S, T>
where
    I: SuiInput,
    S: HasFuzzMetadata + HasRand,
    T: ObjectTypesStore + ObjectStore,
{
    fn mutate(&mut self, state: &mut S, input: &mut I) -> Result<MutationResult, libafl::Error> {
        let ptb = input.ptb();
        if ptb.commands.is_empty() {
            self.mutate_sequence(state, input);
        } else if state.rand_mut().below_or_zero(100) < 20 {
            self.mutate_sequence(state, input);
        }
        let should_havoc = state.rand_mut().below_or_zero(100) < 60;
        let havoc_times = if should_havoc {
            state.rand_mut().below_or_zero(10) + 1
        } else {
            1
        };
        let mut res = MutationResult::Skipped;
        for _ in 0..havoc_times {
            let result = self.mutate_arg(state, input);
            if result == MutationResult::Mutated {
                *input.outcome_mut() = None;
                res = MutationResult::Mutated;
            }
        }
        Ok(res)
    }

    fn post_exec(
        &mut self,
        _state: &mut S,
        _new_corpus_id: Option<libafl::corpus::CorpusId>,
    ) -> Result<(), libafl::Error> {
        Ok(())
    }
}
