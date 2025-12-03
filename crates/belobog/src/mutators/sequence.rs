use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    str::FromStr,
};

use belobog_fork::ObjectTypesStore;
use libafl::{
    HasMetadata,
    mutators::{MutationResult, Mutator},
    state::HasRand,
};
use libafl_bolts::{Named, rands::Rand};
use log::{debug, trace, warn};
use move_core_types::runtime_value::MoveTypeLayout;
use move_vm_types::values::Value;
use sui_json_rpc_types::{
    SuiMoveNormalizedFunction, SuiMoveNormalizedStructType, SuiMoveNormalizedType,
};
use sui_types::{
    Identifier, TypeTag,
    base_types::ObjectID,
    storage::ObjectStore,
    transaction::{Argument, CallArg, Command, ProgrammableMoveCall, ProgrammableTransaction},
    type_input::TypeInput,
};

use crate::{
    r#const::{ADD_MOVECALL_PROB, INIT_FUNCTION_SCORE},
    flash::FlashProvider,
    input::SuiInput,
    meta::{FunctionIdent, FuzzMetadata, HasFuzzMetadata, MutatorKind},
    mutation_utils::MutableValue,
    mutators::{
        HasFlash, StageReplay, StageReplayAction, movecall_is_split, mutate_arg, ptb_fingerprint,
    },
    object_sampler::{
        ConstructResult, ObjectData, gen_type_tag_by_abilities,
        sui_move_ability_set_to_ability_set, try_construct_args, update_score,
    },
    state::HasExtraState,
    type_utils::TypeUtils,
};

pub struct SequenceMutator<I, S, T> {
    pub ph: PhantomData<(I, S)>,
    pub db: T,
    pub flash: Option<FlashProvider>,
    stage: StageReplay,
}

impl<I, S, T> SequenceMutator<I, S, T> {
    pub fn new(db: T) -> Self {
        Self {
            ph: PhantomData,
            db,
            flash: None,
            stage: StageReplay::new(MutatorKind::Sequence),
        }
    }
}

impl<I, S, T> Named for SequenceMutator<I, S, T> {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("sequence_mutator")
    }
}

impl<I, S, T> HasFlash for SequenceMutator<I, S, T> {
    fn flash(&self) -> &Option<FlashProvider> {
        &self.flash
    }
}

fn weighted_sample<'a, T>(items: &'a [T], weights: &[u64], state: &mut impl HasRand) -> &'a T {
    assert_eq!(
        items.len(),
        weights.len(),
        "Items and weights must have the same length"
    );
    assert!(!weights.is_empty(), "Weights cannot be empty");

    let total_weight: u64 = weights.iter().copied().sum();

    if total_weight == 0 {
        panic!("Total weight cannot be zero");
    }

    let random_number = state.rand_mut().below_or_zero(total_weight as usize) + 1;

    let mut cumulative_weight = 0;
    for (i, &weight) in weights.iter().enumerate() {
        cumulative_weight += weight;

        if random_number <= cumulative_weight as usize {
            return &items[i];
        }
    }

    unreachable!();
}

pub fn process_key_store(
    ptb: &mut ProgrammableTransaction,
    state: &impl HasFuzzMetadata,
    db: &impl ObjectStore,
) {
    let object_data = ObjectData::from_ptb(ptb, state, db);
    if !object_data.key_store_objects.is_empty() {
        ptb.inputs.push(CallArg::Pure(
            Value::address(state.fuzz_state().current_sender.into())
                .typed_serialize(&MoveTypeLayout::Address)
                .unwrap(),
        ));
        let to_object_cmd = Command::TransferObjects(
            object_data.key_store_objects,
            Argument::Input(ptb.inputs.len() as u16 - 1),
        );
        ptb.commands.push(to_object_cmd);
    }
}

pub fn remove_process_key_store(ptb: &mut ProgrammableTransaction) {
    if let Some(Command::TransferObjects(_, Argument::Input(_))) = ptb.commands.last() {
        ptb.commands.pop();
        ptb.inputs.pop();
    }
}

pub fn append_function<T, S>(
    db: &T,
    state: &mut S,
    ptb: &mut ProgrammableTransaction,
    function_ident: &FunctionIdent,
    fixed_args: BTreeMap<u16, (Argument, TypeTag)>,
    fixed_ty_args: BTreeMap<u16, TypeTag>,
    used_arguments: &Vec<Argument>,
    disable_split: bool,
    recursion_depth: usize,
) -> Option<(Vec<Argument>, Vec<Argument>)>
// Returns the arguments and returns
where
    T: ObjectTypesStore + ObjectStore,
    S: HasRand + HasFuzzMetadata,
{
    if recursion_depth > 10 {
        debug!(
            "Recursion depth exceeded for function: {:?}, skipping",
            function_ident
        );
        return None;
    }
    let object_data = ObjectData::from_ptb_and_remove_used(ptb, state, db, used_arguments);
    let (addr, mname, fname) = (function_ident.0, &function_ident.1, &function_ident.2);
    let mut cmd = Command::move_call(
        addr,
        Identifier::from_str(mname).unwrap(),
        Identifier::from_str(fname).unwrap(),
        vec![],
        Vec::new(),
    );
    let Command::MoveCall(movecall) = &mut cmd else {
        panic!("Expected MoveCall command");
    };
    debug!("Adding function: {:?}", function_ident);
    let function = state
        .fuzz_state()
        .get_function(&addr, mname, fname)
        .expect(&format!(
            "Function not found: {}::{}::{}",
            addr, mname, fname
        ))
        .clone();
    let mut struct_params = function
        .parameters
        .clone()
        .into_iter()
        .enumerate()
        .filter_map(|(i, p)| {
            if fixed_args.contains_key(&(i as u16)) {
                return None; // Skip fixed arguments
            }
            if p.needs_sample() {
                Some(p)
            } else {
                None // Skip parameters that don't need sampling
            }
        })
        .collect::<Vec<_>>();
    let mut fixed_ty_args = fixed_ty_args;
    for (i, (_, ty_tag)) in fixed_args.iter() {
        if fixed_ty_args.contains_key(i) && fixed_ty_args[i] != *ty_tag {
            panic!("Conflicting type arguments for index {}", i);
        }
        fixed_ty_args.insert(*i, ty_tag.clone());
    }
    let initial_ptb_input_len = ptb.inputs.len();
    let mut inputs = match try_construct_args(
        initial_ptb_input_len,
        &struct_params,
        &function.type_parameters,
        &fixed_ty_args,
        &object_data,
        db,
        state,
    ) {
        ConstructResult::Ok(mut args, ty_args, inputs) => {
            movecall.type_arguments = ty_args;

            let mut inputs = inputs;
            for (i, param) in function.parameters.iter().enumerate() {
                if let Some((arg, _)) = fixed_args.get(&(i as u16)) {
                    movecall.arguments.push(*arg);
                    continue; // Use fixed argument if available
                }
                if param.needs_sample() {
                    movecall.arguments.push(args.remove(0));
                } else {
                    if param.is_tx_context() {
                        // Skip tx context parameters
                        continue;
                    }
                    debug!("Generating initial value for parameter {}: {:?}", i, param);
                    let init_value = param.gen_value();
                    let mut init_value = MutableValue::new(init_value);
                    init_value.mutate(state, &BTreeSet::new(), false);
                    let init_value = init_value.value;
                    inputs.push(CallArg::Pure(
                        init_value
                            .typed_serialize(&function.parameters[i].to_type_layout())
                            .unwrap(),
                    ));
                    movecall.arguments.push(Argument::Input(
                        (ptb.inputs.len() + inputs.len() - 1) as u16,
                    ));
                }
            }
            inputs
        }
        ConstructResult::PartialFound(mut partial_args, ty_args, inputs) => {
            debug!(
                "Partial arguments found for function: {}::{}::{}, got {:?}",
                addr,
                mname,
                fname,
                partial_args
                    .iter()
                    .enumerate()
                    .filter_map(|(i, arg)| arg.and_then(|_| Some(i)))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                partial_args.len(),
                struct_params.len(),
                "Partial arguments length mismatch"
            );
            let mut inputs = inputs;
            let producing_graph = state.fuzz_state().producing_graph.clone();
            let mut type_arguments: BTreeMap<u16, TypeInput> = BTreeMap::new();
            let mut new_used_arguments = partial_args
                .iter()
                .zip(struct_params.iter())
                .filter_map(|(arg, param)| {
                    if let Some(arg) = arg {
                        if matches!(
                            param,
                            SuiMoveNormalizedType::Reference(_)
                                | SuiMoveNormalizedType::MutableReference(_)
                        ) {
                            return None;
                        }
                        Some(arg.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            new_used_arguments.extend(used_arguments.iter().cloned());
            debug!("Used arguments for producing graph: {:?}", used_arguments);
            for (i, param) in function.parameters.iter().enumerate() {
                if let Some((arg, _)) = fixed_args.get(&(i as u16)) {
                    movecall.arguments.push(*arg);
                    continue; // Use fixed argument if available
                }
                if param.needs_sample() {
                    if let Some(arg) = partial_args.remove(0) {
                        struct_params.remove(0);
                        movecall.arguments.push(arg);
                        continue; // Use sampled argument
                    }
                    let mut found_producing = false;
                    let arg_type = struct_params.remove(0).partial_subst(&ty_args);
                    for (producing_type, funcs) in producing_graph.iter() {
                        let Some((arg_ty_arg, mut producing_ty_arg, mapping)) =
                            arg_type.partial_extract_ty_args(producing_type)
                        else {
                            trace!(
                                "Type argument extraction failed for parameter type {:?} and producing type {:?}",
                                arg_type, producing_type
                            );
                            continue;
                        };

                        // except itself
                        let funcs = funcs
                            .iter()
                            .filter(|(f, _)| !(f.0 == addr && &f.1 == mname && &f.2 == fname))
                            .collect::<Vec<_>>();
                        if funcs.is_empty() {
                            debug!(
                                "No producing functions found for type {:?} in {:?}::{:?}",
                                arg_type, addr, mname
                            );
                            continue;
                        }

                        let pre_func = funcs[state.rand_mut().below_or_zero(funcs.len())].clone();
                        debug!(
                            "Using producing function {:?} for argument type {:?}",
                            pre_func, arg_type
                        );
                        debug!(
                            "With arg_ty_arg: {:?}, producing_ty_arg: {:?}, mapping: {:?}",
                            arg_ty_arg, producing_ty_arg, mapping
                        );
                        let mut type_arg_conflict = false;
                        for (self_, other) in mapping.iter() {
                            if let Some(ty_arg) = type_arguments.get(self_) {
                                if let Some(other_ty_arg) = producing_ty_arg.get(other) {
                                    if *ty_arg != other_ty_arg.clone().into() {
                                        debug!(
                                            "Type argument conflict for producing function {:?}: {:?} vs {:?}",
                                            pre_func, ty_arg, other_ty_arg
                                        );
                                        type_arg_conflict = true;
                                        break;
                                    }
                                } else {
                                    producing_ty_arg
                                        .insert(*other, ty_arg.clone().to_type_tag().unwrap());
                                }
                            }
                        }
                        if type_arg_conflict {
                            continue;
                        }
                        if let Some((_, new_rets)) = append_function(
                            db,
                            state,
                            ptb,
                            &pre_func.0,
                            BTreeMap::new(),
                            producing_ty_arg,
                            &new_used_arguments,
                            false,
                            recursion_depth + 1,
                        ) {
                            found_producing = true;
                            let Command::MoveCall(new_movecall) = ptb.commands.last_mut().unwrap()
                            else {
                                panic!("Expected MoveCall command");
                            };
                            let mut ty_args: BTreeMap<u16, TypeInput> = ty_args
                                .clone()
                                .into_iter()
                                .map(|(j, ty_arg)| (j, ty_arg.into()))
                                .collect::<BTreeMap<_, _>>();
                            ty_args.extend(
                                arg_ty_arg
                                    .iter()
                                    .map(|(j, ty_arg)| (*j, ty_arg.clone().into())),
                            );
                            for m in mapping {
                                ty_args
                                    .insert(m.0, new_movecall.type_arguments[m.1 as usize].clone());
                            }

                            type_arguments.extend(ty_args);
                            movecall.arguments.push(new_rets[pre_func.1 as usize]);

                            if !(matches!(
                                param,
                                SuiMoveNormalizedType::Reference(_)
                                    | SuiMoveNormalizedType::MutableReference(_)
                            )) {
                                new_used_arguments.push(new_rets[pre_func.1 as usize].clone());
                            }
                            break;
                        } else {
                            debug!(
                                "Failed to append function for partial argument: {}::{}::{}",
                                pre_func.0.0, pre_func.0.1, pre_func.0.2
                            );
                            return None;
                        }
                    }
                    if !found_producing {
                        trace!(
                            "No producing function found for argument type {:?} in {:?}::{:?}",
                            arg_type, addr, mname
                        );
                        return None;
                    }
                } else {
                    if param.is_tx_context() {
                        // Skip tx context parameters
                        continue;
                    }
                    debug!("Generating initial value for parameter {}: {:?}", i, param);
                    let init_value = param.gen_value();
                    let mut init_value = MutableValue::new(init_value);
                    init_value.mutate(state, &BTreeSet::new(), false);
                    let init_value = init_value.value;
                    inputs.push(CallArg::Pure(
                        init_value
                            .typed_serialize(&function.parameters[i].to_type_layout())
                            .unwrap(),
                    ));
                    movecall.arguments.push(Argument::Input(
                        (initial_ptb_input_len + inputs.len() - 1) as u16,
                    ));
                }
            }
            movecall.type_arguments = function
                .type_parameters
                .iter()
                .enumerate()
                .map(|(j, ability)| {
                    type_arguments.get(&(j as u16)).cloned().unwrap_or(
                        gen_type_tag_by_abilities(
                            &sui_move_ability_set_to_ability_set(ability),
                            state,
                        )
                        .into(),
                    )
                })
                .collect();

            inputs
        }
        ConstructResult::Unsolvable => {
            debug!(
                "Failed to construct arguments for function: {}::{}::{}",
                addr, mname, fname
            );
            return None;
        }
    };

    // after appending sub functions, we need to adjust Input arguments offsets
    let mut remove_idxs = vec![];
    let mut fixed_idxs = vec![];
    for (i, (arg, param)) in movecall
        .arguments
        .iter_mut()
        .zip(function.parameters.iter())
        .enumerate()
    {
        if let Argument::Input(input_idx) = arg {
            if (*input_idx as usize) < initial_ptb_input_len {
                continue; // Skip inputs that are not newly added
            }
            let offset = *input_idx as usize - initial_ptb_input_len;
            let input = &inputs[offset];
            if matches!(input, CallArg::Object(_)) && ptb.inputs.contains(input) {
                if matches!(
                    param,
                    SuiMoveNormalizedType::Reference(_)
                        | SuiMoveNormalizedType::MutableReference(_)
                ) {
                    *input_idx = ptb.inputs.iter().position(|x| x == input).unwrap() as u16;
                    fixed_idxs.push(i);
                    remove_idxs.push(offset);
                    continue;
                } else {
                    return None;
                }
            }
        }
    }
    remove_idxs.sort_unstable();
    remove_idxs.dedup();
    for remove_idx in remove_idxs.iter().rev() {
        inputs.remove(*remove_idx);
    }
    for (i, arg) in movecall.arguments.iter_mut().enumerate() {
        if fixed_idxs.contains(&i) {
            continue;
        }
        if let Argument::Input(input_idx) = arg
            && *input_idx as usize >= initial_ptb_input_len
        {
            let offset = *input_idx as usize - initial_ptb_input_len;
            let adjust = remove_idxs.iter().filter(|&&x| x < offset).count();
            *input_idx -= adjust as u16;
            *input_idx += (ptb.inputs.len() - initial_ptb_input_len) as u16
        }
    }

    if !disable_split {
        // Handle balance parameters
        for (arg, param) in movecall
            .arguments
            .iter_mut()
            .zip(function.parameters.iter())
        {
            if param.is_balance() || param.is_coin() {
                let special_string = if param.is_balance() {
                    "balance"
                } else {
                    "coin"
                };
                debug!(
                    "Handling {} parameter: {:?} for function: {}::{}::{}",
                    special_string, param, addr, mname, fname
                );
                let ty_args_map = movecall
                    .type_arguments
                    .iter()
                    .enumerate()
                    .map(|(j, ty_arg)| (j as u16, ty_arg.clone().to_type_tag().unwrap()))
                    .collect::<BTreeMap<_, _>>();
                let TypeTag::Struct(s) = param.subst(&ty_args_map).unwrap() else {
                    panic!("Expected {special_string} parameter to be a struct");
                };
                let old_arg = std::mem::replace(arg, Argument::Result(ptb.commands.len() as u16));
                let input = CallArg::Pure(
                    SuiMoveNormalizedType::U64
                        .gen_value()
                        .typed_serialize(&SuiMoveNormalizedType::U64.to_type_layout())
                        .unwrap(),
                );
                inputs.push(input);
                let split_cmd = Command::move_call(
                    ObjectID::from_str("0x2").unwrap(),
                    Identifier::from_str(special_string).unwrap(),
                    Identifier::from_str("split").unwrap(),
                    s.type_params.clone(),
                    vec![
                        old_arg,
                        Argument::Input((ptb.inputs.len() + inputs.len()) as u16 - 1),
                    ],
                );
                ptb.commands.push(split_cmd);
            }
        }
    }

    assert_eq!(
        movecall.type_arguments.len(),
        function.type_parameters.len(),
        "Type arguments length mismatch for function: {}::{}::{}",
        addr,
        mname,
        fname
    );

    debug!(
        "Appended function: {:?}, args: {:?}, ty_args: {:?}",
        function_ident, movecall.arguments, movecall.type_arguments
    );

    let arguments = movecall.arguments.clone();
    let returns = if function.return_.len() == 1 {
        vec![Argument::Result(ptb.commands.len() as u16)]
    } else {
        function
            .return_
            .iter()
            .enumerate()
            .map(|(j, _)| Argument::NestedResult(ptb.commands.len() as u16, j as u16))
            .collect::<Vec<_>>()
    };
    ptb.commands.push(cmd);
    ptb.inputs.extend(inputs);
    Some((arguments, returns))
}

impl<I, S, T> SequenceMutator<I, S, T> {
    fn gen_remove_idxes(
        &self,
        meta: &FuzzMetadata,
        ptb: &ProgrammableTransaction,
        idx: u16,
        removed_idxes: &mut Vec<u16>,
    ) {
        if let Some(provider) = &self.flash {
            match provider {
                FlashProvider::Cetus { .. } => {
                    if idx as usize >= ptb.commands.len() - 3 || idx == 0 {
                        // We cannot remove the first or last commands (flash & repay)
                        return;
                    }
                }
                FlashProvider::Nemo { .. } => {
                    if idx as usize >= ptb.commands.len() - 1 || idx <= 1 {
                        return;
                    }
                }
            }
        } else if idx as usize >= ptb.commands.len() {
            return;
        }

        removed_idxes.push(idx);

        let mut deleting_idxes = vec![];
        for (post_idx, post_cmd) in ptb.commands.iter().skip(idx as usize + 1).enumerate() {
            if let Command::MoveCall(movecall) = post_cmd {
                let mut need_remove = false;
                for arg in movecall.arguments.iter() {
                    if matches!(arg, Argument::Result(i) | Argument::NestedResult(i, _) if *i == idx)
                    {
                        need_remove = true;
                        break; // No need to check further arguments
                    }
                    // substitude not implemented yet
                }
                if need_remove {
                    deleting_idxes.push(idx + post_idx as u16 + 1);
                }
            }
        }
        deleting_idxes.sort_unstable();
        deleting_idxes.dedup();
        for idx in deleting_idxes.iter().rev() {
            if removed_idxes.contains(idx) {
                continue;
            }
            self.gen_remove_idxes(meta, ptb, *idx, removed_idxes);
        }

        let cmd = ptb.commands.get(idx as usize).unwrap();
        let Command::MoveCall(movecall) = cmd else {
            return;
        };
        for i in (0..idx).rev() {
            if removed_idxes.contains(&i) {
                continue;
            }
            if ptb
                .commands
                .get(i as usize)
                .is_some_and(|c| matches!(c, Command::MoveCall(mc) if movecall_is_split(mc)))
            {
                self.gen_remove_idxes(meta, ptb, i, removed_idxes);
            } else {
                break;
            }
        }
        let mut deleting_prev_idxes = vec![];
        let function = meta
            .get_function(&movecall.package, &movecall.module, &movecall.function)
            .unwrap();
        for (arg, param) in movecall.arguments.iter().zip(function.parameters.iter()) {
            if matches!(param, SuiMoveNormalizedType::Struct { .. }) && param.is_hot_potato(meta) {
                if let Argument::Result(i) = arg {
                    deleting_prev_idxes.push(*i);
                } else if let Argument::NestedResult(i, _j) = arg {
                    deleting_prev_idxes.push(*i);
                }
            }
        }
        deleting_prev_idxes.sort_unstable();
        deleting_prev_idxes.dedup();
        for i in deleting_prev_idxes.iter().rev() {
            if removed_idxes.contains(i) {
                continue;
            }
            self.gen_remove_idxes(meta, ptb, *i, removed_idxes);
        }

        removed_idxes.sort_unstable();
        removed_idxes.dedup();
    }

    fn remove_command(
        &self,
        meta: &FuzzMetadata,
        ptb: &mut ProgrammableTransaction,
        idx: u16,
    ) -> MutationResult
    where
        I: SuiInput,
        S: HasRand + HasFuzzMetadata,
    {
        debug!("Removing command at index {}", idx);
        let mut removed_idxes = vec![];
        self.gen_remove_idxes(meta, ptb, idx, &mut removed_idxes);
        debug!(
            "Also removing dependent commands at indexes {:?}",
            removed_idxes
        );
        if removed_idxes.len() >= ptb.commands.len() || removed_idxes.is_empty() {
            // We cannot remove all commands
            return MutationResult::Skipped;
        }

        for remove_idx in removed_idxes.iter().rev() {
            ptb.commands.remove(*remove_idx as usize);
        }
        for cmd in ptb.commands.iter_mut() {
            if let Command::MoveCall(movecall) = cmd {
                movecall.arguments.iter_mut().for_each(|arg| match arg {
                    Argument::Result(i) => {
                        let mut shift = 0;
                        for removed_idx in removed_idxes.iter() {
                            if *removed_idx < *i {
                                shift += 1;
                            }
                        }
                        *i -= shift;
                    }
                    Argument::NestedResult(i, _j) => {
                        let mut shift = 0;
                        for removed_idx in removed_idxes.iter() {
                            if *removed_idx < *i {
                                shift += 1;
                            }
                        }
                        *i -= shift;
                    }
                    _ => {}
                });
            }
        }

        MutationResult::Mutated
    }

    fn finish(&self, state: &mut S, ptb: &mut ProgrammableTransaction)
    where
        I: SuiInput,
        S: HasFuzzMetadata + HasRand,
        T: ObjectTypesStore + ObjectStore,
    {
        if let Some(provider) = &self.flash {
            match provider {
                FlashProvider::Cetus { coin_a, coin_b, .. } => {
                    // Ensure repay coins are set empty
                    *provider.repay_coin0(ptb).unwrap() =
                        Argument::Result(ptb.commands.len() as u16 - 3);
                    *provider.repay_coin1(ptb).unwrap() =
                        Argument::Result(ptb.commands.len() as u16 - 2);
                    // Set split coins to to flash coin first, trying to sample from other functions
                    *provider.repay_split_coin0(ptb).unwrap() = Argument::NestedResult(0, 0);
                    *provider.repay_split_coin1(ptb).unwrap() = Argument::NestedResult(0, 1);
                    let mut object_data = ObjectData::from_ptb(ptb, state, &self.db);
                    object_data
                        .existing_objects
                        .iter_mut()
                        .for_each(|(_, objs)| {
                            objs.retain(|(obj, _)| {
                                obj != &Argument::NestedResult(0, 0)
                                    && obj != &Argument::NestedResult(0, 1)
                            });
                        });
                    let coin_a_type = SuiMoveNormalizedType::Struct {
                        inner: Box::new(SuiMoveNormalizedStructType {
                            address: "0x2".into(),
                            module: "balance".into(),
                            name: "Balance".into(),
                            type_arguments: vec![SuiMoveNormalizedType::from_type_tag(coin_a)],
                        }),
                    };
                    let coin_b_type = SuiMoveNormalizedType::Struct {
                        inner: Box::new(SuiMoveNormalizedStructType {
                            address: "0x2".into(),
                            module: "balance".into(),
                            name: "Balance".into(),
                            type_arguments: vec![SuiMoveNormalizedType::from_type_tag(coin_b)],
                        }),
                    };

                    if let ConstructResult::Ok(args, _, _) = try_construct_args(
                        ptb.inputs.len() - 1,
                        &vec![coin_a_type.clone()],
                        &[],
                        &BTreeMap::from([(0, coin_a.clone()), (1, coin_b.clone())]),
                        &object_data,
                        &self.db,
                        state,
                    ) {
                        debug!("Found repay coin0: {:?}", args[0]);
                        *provider.repay_split_coin0(ptb).unwrap() = args[0];
                    }

                    if let ConstructResult::Ok(args, _, _) = try_construct_args(
                        ptb.inputs.len() - 1,
                        &vec![coin_b_type.clone()],
                        &[],
                        &BTreeMap::from([(0, coin_a.clone()), (1, coin_b.clone())]),
                        &object_data,
                        &self.db,
                        state,
                    ) {
                        debug!("Found repay coin1: {:?}", args[0]);
                        *provider.repay_split_coin1(ptb).unwrap() = args[0];
                    }
                }
                FlashProvider::Nemo { .. } => {}
            }
        }
    }

    fn process_balance(&self, ptb: &mut ProgrammableTransaction, state: &impl HasFuzzMetadata)
    where
        T: ObjectTypesStore + ObjectStore,
    {
        let object_data = ObjectData::from_ptb(ptb, state, &self.db);
        for balance in object_data.balances.iter() {
            let (idx, ret_idx) = if let Argument::Result(i) = balance {
                (*i, 0)
            } else if let Argument::NestedResult(i, j) = balance {
                (*i, *j)
            } else {
                panic!("Expected balance argument to be Result or NestedResult");
            };
            let Command::MoveCall(movecall) = &mut ptb.commands[idx as usize] else {
                panic!("Expected MoveCall command");
            };
            let function = state
                .fuzz_state()
                .get_function(&movecall.package, &movecall.module, &movecall.function)
                .unwrap();
            let Some(SuiMoveNormalizedType::Struct { inner }) =
                function.return_.get(ret_idx as usize)
            else {
                panic!("Expected balance return type to be a struct");
            };
            let ty_arg = if let SuiMoveNormalizedType::TypeParameter(j) = inner.type_arguments[0] {
                let ty_arg = &movecall.type_arguments[j as usize];
                ty_arg.to_type_tag().unwrap()
            } else {
                inner.type_arguments[0].subst(&BTreeMap::new()).unwrap()
            };
            let from_balance_cmd = Command::move_call(
                ObjectID::from_str("0x2").unwrap(),
                Identifier::from_str("coin").unwrap(),
                Identifier::from_str("from_balance").unwrap(),
                vec![ty_arg.clone()],
                vec![*balance],
            );
            ptb.commands.push(from_balance_cmd);
        }
    }

    fn remove_process_balance(&self, ptb: &mut ProgrammableTransaction) {
        for i in (0..ptb.commands.len()).rev() {
            if let Command::MoveCall(movecall) = &ptb.commands[i] {
                if movecall.package == ObjectID::from_str("0x2").unwrap()
                    && movecall.module == "coin"
                    && movecall.function == "from_balance"
                {
                    ptb.commands.remove(i);
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    fn process_key_store(&self, ptb: &mut ProgrammableTransaction, state: &impl HasFuzzMetadata)
    where
        T: ObjectTypesStore + ObjectStore,
    {
        process_key_store(ptb, state, &self.db);
    }

    fn remove_process_key_store(&self, ptb: &mut ProgrammableTransaction) {
        remove_process_key_store(ptb);
    }

    fn mutate_sequence(&mut self, state: &mut S, input: &mut I) -> MutationResult
    where
        I: SuiInput,
        T: ObjectTypesStore + ObjectStore,
        S: HasRand + HasFuzzMetadata,
    {
        let ptb = input.ptb_mut();

        let inc = if ptb.commands.len() <= 3 {
            true
        } else {
            // Probability to add a movecall decreases as the sequence length increases
            state.rand_mut().next_float() < ADD_MOVECALL_PROB * 7.0 / ptb.commands.len() as f64
        };
        if !inc {
            let idx = state.rand_mut().below_or_zero(ptb.commands.len()) as u16;
            return self.remove_command(state.fuzz_state(), ptb, idx);
        }
        let mut selected_times = 0;
        let functions = state.fuzz_state().target_functions.clone();
        assert!(!functions.is_empty(), "No target functions available");
        let ptb_snapshot = ptb.clone();
        let mut result = MutationResult::Skipped;
        loop {
            if selected_times >= 10 {
                break;
            }
            selected_times += 1;
            let weights = functions
                .iter()
                .map(|f| {
                    state
                        .fuzz_state()
                        .function_scores
                        .get(f)
                        .cloned()
                        .unwrap_or(0)
                })
                .collect::<Vec<_>>();
            if weights.iter().all(|w| *w == 0) {
                return result;
            }
            for f in functions
                .iter()
                .zip(weights.iter())
                .filter(|(_f, w)| **w > INIT_FUNCTION_SCORE)
                .collect::<Vec<_>>()
            {
                debug!("function: {:?}, weight: {}", f.0, f.1);
            }
            let function = weighted_sample(&functions, &weights, state);
            let (idx, used_arguments) = if let Some(provider) = &self.flash {
                match provider {
                    FlashProvider::Cetus { .. } => {
                        (ptb.commands.len() - 3, vec![Argument::NestedResult(0, 2)])
                    }
                    FlashProvider::Nemo { .. } => {
                        (ptb.commands.len() - 1, vec![Argument::NestedResult(1, 1)])
                    }
                }
            } else {
                (ptb.commands.len(), vec![])
            };
            let template_cmds = ptb.commands.drain(idx..).collect::<Vec<_>>();
            if append_function(
                &self.db,
                state,
                ptb,
                function,
                BTreeMap::new(),
                BTreeMap::new(),
                &used_arguments,
                false,
                0,
            )
            .is_none()
            {
                debug!(
                    "Failed to append function: {:?}, reverting to snapshot",
                    function
                );
                *ptb = ptb_snapshot.clone();
                continue;
            }
            ptb.commands.extend(template_cmds);
            self.finish(state, ptb);
            if ptb
                .commands
                .iter()
                .filter(|cmd| matches!(cmd, Command::MoveCall(mc) if movecall_is_split(mc)))
                .count()
                > 10
            {
                break;
            }
            let object_data = ObjectData::from_ptb(ptb, state, &self.db);

            debug!(
                "loop iteration: {}, commands: {}, hot potatoes: {:?}",
                selected_times,
                ptb.commands.len(),
                object_data.hot_potatoes,
            );
            update_score(&object_data.hot_potatoes, state.fuzz_state_mut());
            if object_data.hot_potatoes.is_empty() {
                result = MutationResult::Mutated;
                break;
            }
        }

        if result == MutationResult::Skipped {
            *ptb = ptb_snapshot;
        }
        let object_data = ObjectData::from_ptb(ptb, state, &self.db);
        update_score(&object_data.hot_potatoes, state.fuzz_state_mut());
        result
    }
}

impl<I, S, T> Mutator<I, S> for SequenceMutator<I, S, T>
where
    I: SuiInput,
    S: HasFuzzMetadata + HasRand + HasMetadata + HasExtraState,
    T: ObjectTypesStore + ObjectStore,
{
    fn mutate(&mut self, state: &mut S, input: &mut I) -> Result<MutationResult, libafl::Error> {
        self.flash = input.flash().as_ref().map(|f| f.provider.clone());

        let extra = state
            .extra_state()
            .global_outcome
            .as_ref()
            .map(|o| &o.extra);

        if let Some(ex) = extra {
            input.update_magic_number(ex);
            debug!(
                "sequence mutator last stage: {:?}, stage outcome success: {}",
                ex.stage_idx, ex.success
            );
        }

        if let StageReplayAction::Replay {
            stage_idx,
            snapshot,
        } = self.stage.decide(
            state.fuzz_state().current_mutator,
            extra,
            ptb_fingerprint(input.ptb()),
        ) {
            *input.ptb_mut() = snapshot;
            let magic = state.rand_mut().below_or_zero(2) == 0;
            let res = mutate_arg(self, state, input, magic, &Some(stage_idx));
            if res == MutationResult::Mutated {
                self.stage.record_success(input.ptb());
                state.fuzz_state_mut().current_mutator = Some(MutatorKind::Sequence);
            }
            *input.outcome_mut() = None;
            return Ok(res);
        }

        // before mutation, remove balance and key store processing commands
        self.remove_process_key_store(input.ptb_mut());
        self.remove_process_balance(input.ptb_mut());

        let res = self.mutate_sequence(state, input);

        let magic = state.rand_mut().below_or_zero(2) == 0;
        mutate_arg(self, state, input, magic, &None);

        // after mutation, re-add balance and key store processing commands
        self.process_balance(input.ptb_mut(), state);
        self.process_key_store(input.ptb_mut(), state);
        *input.outcome_mut() = None;

        if res == MutationResult::Mutated {
            self.stage.record_success(input.ptb());
            state.fuzz_state_mut().current_mutator = Some(MutatorKind::Sequence);
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
