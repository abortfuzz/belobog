use belobog_fork::ObjectTypesStore;
use itertools::Itertools;
use libafl::state::HasRand;
use libafl_bolts::rands::Rand;
use log::{debug, trace, warn};
use move_binary_format::file_format::{Ability, AbilitySet};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};
use sui_json_rpc_types::{SuiMoveAbility, SuiMoveAbilitySet, SuiMoveNormalizedType};
use sui_types::{
    TypeTag,
    base_types::ObjectID,
    object::Owner,
    storage::ObjectStore,
    transaction::{
        Argument, CallArg, Command, ObjectArg, ProgrammableMoveCall, ProgrammableTransaction,
    },
    type_input::TypeInput,
};

use crate::{
    r#const::{INIT_FUNCTION_SCORE, PRIVILEGE_FUNCTION_SCORE, SCORE_TICK},
    input::SuiFuzzInput,
    meta::{FunctionIdent, FuzzMetadata, HasFuzzMetadata},
    type_utils::TypeUtils,
};

#[derive(Hash, Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObjectData {
    pub existing_objects: BTreeMap<TypeTag, Vec<(Argument, Gate)>>,
    pub key_store_objects: Vec<Argument>,
    pub hot_potatoes: Vec<TypeTag>, // number of times we have sampled an object without the Drop ability
    pub used_object_ids: Vec<ObjectID>, // used to track object IDs that have been sampled
    pub balances: Vec<Argument>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Gate {
    Owned,
    Immutable,
    Shared,
}

pub enum ConstructResult {
    Ok(Vec<Argument>, Vec<TypeInput>, Vec<CallArg>),
    PartialFound(Vec<Option<Argument>>, BTreeMap<u16, TypeTag>, Vec<CallArg>),
    Unsolvable,
}

impl ObjectData {
    pub fn new() -> Self {
        Self {
            existing_objects: BTreeMap::new(),
            key_store_objects: Vec::new(),
            hot_potatoes: Vec::new(),
            used_object_ids: vec![],
            balances: Vec::new(),
        }
    }

    pub fn from_ptb(
        ptb: &ProgrammableTransaction,
        state: &impl HasFuzzMetadata,
        db: &impl ObjectStore,
    ) -> Self {
        if ptb.commands.is_empty() {
            let object_data = Self::new();
            if let Some(id) = state.fuzz_state().gas_id {
                return Self {
                    used_object_ids: vec![id],
                    ..object_data
                };
            }
            return Self::new(); // Return empty ObjectData for empty PTB
        }
        let mut existing_objects: BTreeMap<TypeTag, Vec<(Argument, Gate)>> = BTreeMap::new();
        let mut key_store_objects = Vec::new();
        let mut hot_potatoes = Vec::new();
        let mut used_object_ids = vec![state.fuzz_state().gas_id.unwrap()];
        let mut balances = Vec::new();
        for (i, input) in ptb.inputs.iter().enumerate() {
            if let CallArg::Object(obj) = input {
                let (obj_id, seq) = match obj {
                    ObjectArg::ImmOrOwnedObject((id, seq, _)) => (id, seq),
                    ObjectArg::SharedObject {
                        id,
                        initial_shared_version,
                        ..
                    } => (id, initial_shared_version),
                    _ => unimplemented!("Unsupported object argument type"),
                };
                used_object_ids.push(*obj_id);
                let object = db.get_object_by_key(obj_id, *seq).unwrap();
                let gate = match object.owner() {
                    Owner::AddressOwner(_) => Gate::Owned,
                    Owner::Immutable => Gate::Immutable,
                    Owner::Shared { .. } => Gate::Shared,
                    _ => {
                        panic!(
                            "Unsupported object owner type: {:?}",
                            db.get_object_by_key(obj_id, *seq).unwrap().owner()
                        );
                    }
                };
                existing_objects
                    .entry(object.type_().unwrap().clone().into())
                    .or_default()
                    .push((Argument::Input(i as u16), gate));
            }
        }
        for (i, cmd) in ptb.commands.iter().enumerate() {
            let Command::MoveCall(movecall) = cmd else {
                continue; // Only process MoveCall commands
            };
            let function = state
                .fuzz_state()
                .get_function(&movecall.package, &movecall.module, &movecall.function)
                .unwrap_or_else(|| {
                    panic!(
                        "Function {}::{}::{} not found in module",
                        movecall.package, movecall.module, movecall.function
                    )
                });
            let ty_args_map = &movecall
                .type_arguments
                .iter()
                .enumerate()
                .map(|(i, ty)| (i as u16, ty.clone().to_type_tag().unwrap()))
                .collect::<BTreeMap<_, _>>();
            for (arg, param) in movecall.arguments.iter().zip(function.parameters.iter()) {
                if !param.needs_sample() {
                    continue; // Skip parameters that do not need sampling
                }
                if matches!(param, SuiMoveNormalizedType::Reference(_))
                    || matches!(param, SuiMoveNormalizedType::MutableReference(_))
                {
                    continue; // Skip reference parameters
                }
                // if param.has_copy(state.fuzz_state()) {
                //     continue; // Skip parameters that have the Copy ability
                // }
                let instantiated_param = param.subst(ty_args_map).unwrap();
                existing_objects
                    .get_mut(&instantiated_param)
                    .unwrap_or_else(|| {
                        panic!(
                            "Expected existing objects for parameter type {:?}, input {}",
                            instantiated_param,
                            SuiFuzzInput {
                                ptb: ptb.clone(),
                                ..Default::default()
                            }
                        )
                    })
                    .retain(|(a, _)| arg != a);
                if existing_objects
                    .get(&instantiated_param)
                    .unwrap()
                    .is_empty()
                {
                    existing_objects.remove(&instantiated_param); // Remove empty entries
                }
                if matches!(arg, Argument::Input(_)) {
                    continue; // Skip input arguments for key store and hot potatoes
                }
                if param.is_balance() {
                    balances.retain(|a| a != arg); // Remove from balances if it is a balance object
                }
                if param.is_key_store(state.fuzz_state()) {
                    key_store_objects.retain(|a| a != arg); // Remove from key_store_objects if it is a key store object
                }
                if param.is_hot_potato(state.fuzz_state()) {
                    hot_potatoes.remove(
                        hot_potatoes
                            .iter()
                            .position(|x| x == &instantiated_param)
                            .unwrap_or_else(|| {
                                panic!(
                                    "Expected hot potato object for type {:?}, input {}",
                                    instantiated_param,
                                    SuiFuzzInput {
                                        ptb: ptb.clone(),
                                        ..Default::default()
                                    }
                                )
                            }),
                    );
                }
            }

            for (j, ret_ty) in function.return_.iter().enumerate() {
                if let SuiMoveNormalizedType::Vector(inner) = ret_ty {
                    let instantiated_ret_ty = ret_ty.subst(ty_args_map).unwrap();
                    match inner.as_ref() {
                        SuiMoveNormalizedType::Struct { inner } => {
                            let abilities = state
                                .fuzz_state()
                                .get_abilities(
                                    &ObjectID::from_str(&inner.address).unwrap(),
                                    &inner.module,
                                    &inner.name,
                                )
                                .expect(&format!(
                                    "Struct {}::{}::{} not found in module",
                                    inner.address, inner.module, inner.name
                                ));
                            if !abilities.has_drop() {
                                hot_potatoes.push(instantiated_ret_ty);
                            }
                        }
                        SuiMoveNormalizedType::TypeParameter(idx) => {
                            let ty_tag = ty_args_map.get(idx).unwrap();
                            let TypeTag::Struct(tag) = ty_tag else {
                                continue;
                            };
                            let abilities = state
                                .fuzz_state()
                                .get_abilities(
                                    &tag.address.into(),
                                    &tag.module.to_string(),
                                    &tag.name.to_string(),
                                )
                                .expect(&format!(
                                    "Struct {}::{}::{} not found in module",
                                    tag.address, tag.module, tag.name
                                ));
                            if !abilities.has_drop() {
                                hot_potatoes.push(instantiated_ret_ty);
                            }
                        }
                        _ => {}
                    }
                    continue;
                }
                if !matches!(ret_ty, SuiMoveNormalizedType::Struct { .. }) {
                    continue; // Only process struct return types
                }
                let instantiated_ret_ty = ret_ty.subst(ty_args_map).unwrap();
                let res_arg = if function.return_.len() == 1 {
                    Argument::Result(i as u16)
                } else {
                    Argument::NestedResult(i as u16, j as u16)
                };
                existing_objects
                    .entry(instantiated_ret_ty.clone())
                    .or_default()
                    .push((res_arg, Gate::Owned));
                if ret_ty.is_balance() {
                    balances.push(res_arg);
                }
                if ret_ty.is_key_store(state.fuzz_state()) {
                    key_store_objects.push(res_arg);
                }
                if ret_ty.is_hot_potato(state.fuzz_state()) {
                    hot_potatoes.push(instantiated_ret_ty);
                }
            }
        }
        Self {
            existing_objects,
            hot_potatoes,
            key_store_objects,
            used_object_ids,
            balances,
        }
    }

    pub fn from_ptb_and_remove_used(
        ptb: &ProgrammableTransaction,
        state: &impl HasFuzzMetadata,
        db: &impl ObjectStore,
        used_arguments: &Vec<Argument>,
    ) -> Self {
        let mut data = Self::from_ptb(ptb, state, db);
        for arg in used_arguments.iter() {
            for (ty_tag, candidates) in data.existing_objects.iter_mut() {
                if candidates.iter().all(|(a, _)| a != arg) {
                    continue;
                }
                candidates.retain(|(a, _)| a != arg);
                if let Some(idx) = data.hot_potatoes.iter().position(|x| x == ty_tag) {
                    data.hot_potatoes.remove(idx);
                }
            }
            data.existing_objects
                .retain(|_, candidates| !candidates.is_empty());
            data.balances.retain(|a| a != arg);
            data.key_store_objects.retain(|a| a != arg);
        }
        data
    }
}

pub fn sui_move_ability_set_to_ability_set(sui_move_ability_set: &SuiMoveAbilitySet) -> AbilitySet {
    let mut ability_set = AbilitySet::EMPTY;
    if sui_move_ability_set
        .abilities
        .contains(&SuiMoveAbility::Copy)
    {
        ability_set = ability_set.union(AbilitySet::singleton(Ability::Copy));
    }
    if sui_move_ability_set
        .abilities
        .contains(&SuiMoveAbility::Drop)
    {
        ability_set = ability_set.union(AbilitySet::singleton(Ability::Drop));
    }
    if sui_move_ability_set
        .abilities
        .contains(&SuiMoveAbility::Store)
    {
        ability_set = ability_set.union(AbilitySet::singleton(Ability::Store));
    }
    if sui_move_ability_set
        .abilities
        .contains(&SuiMoveAbility::Key)
    {
        ability_set = ability_set.union(AbilitySet::singleton(Ability::Key));
    }
    ability_set
}

fn intersect_generics(type_args: &[&BTreeMap<u16, TypeTag>]) -> Option<BTreeMap<u16, TypeTag>> {
    let mut result = BTreeMap::new();
    for type_arg in type_args.iter() {
        for (index, ty_tag) in type_arg.iter() {
            if let Some(existing_ty_tag) = result.get(index) {
                if existing_ty_tag != ty_tag {
                    return None; // Found a conflict in type arguments
                }
            } else {
                result.insert(*index, ty_tag.clone());
            }
        }
    }
    Some(result)
}

fn get_ty_args_candidates(rows: &[Vec<BTreeMap<u16, TypeTag>>]) -> Vec<BTreeMap<u16, TypeTag>> {
    rows.iter()
        .map(|r| r.iter())
        .multi_cartesian_product()
        .filter_map(|combo| intersect_generics(&combo))
        .collect()
}

pub fn gen_type_tag_by_abilities<S>(abilities: &AbilitySet, state: &mut S) -> TypeTag
where
    S: HasFuzzMetadata + HasRand,
{
    let ability_to_type_tag = state.fuzz_state().ability_to_type_tag.clone();
    ability_to_type_tag
        .get(abilities)
        .and_then(|type_tags| {
            if type_tags.is_empty() {
                None
            } else {
                Some(type_tags[state.rand_mut().below_or_zero(type_tags.len())].clone())
            }
        })
        .unwrap_or(TypeTag::U64) // Default to U64 if no type tag is found
}

fn get_type_tag_ability(type_tag: &TypeTag, state: &impl HasFuzzMetadata) -> AbilitySet {
    match type_tag {
        TypeTag::Bool => AbilitySet::PRIMITIVES,
        TypeTag::Address => AbilitySet::PRIMITIVES,
        TypeTag::U8 => AbilitySet::PRIMITIVES,
        TypeTag::U16 => AbilitySet::PRIMITIVES,
        TypeTag::U32 => AbilitySet::PRIMITIVES,
        TypeTag::U64 => AbilitySet::PRIMITIVES,
        TypeTag::U128 => AbilitySet::PRIMITIVES,
        TypeTag::U256 => AbilitySet::PRIMITIVES,
        TypeTag::Signer => AbilitySet::SIGNER,
        TypeTag::Vector(_) => AbilitySet::VECTOR,
        TypeTag::Struct(tag) => state
            .fuzz_state()
            .get_abilities(
                &tag.address.into(),
                &tag.module.to_string(),
                &tag.name.to_string(),
            )
            .unwrap_or_else(|| {
                panic!(
                    "Struct {}::{}::{} not found in module, meta: {:?}",
                    tag.address,
                    tag.module,
                    tag.name,
                    state.fuzz_state().module_address_to_package
                )
            }),
    }
}

// return a map of ty param index to type arguments
fn try_sample_object_from_db<T, S>(
    ty: &SuiMoveNormalizedType,
    db_type_tags: &[TypeTag],
    function_abilities: &[AbilitySet],
    gate: Gate,
    db: &T,
    state: &mut S,
) -> Vec<BTreeMap<u16, TypeTag>>
where
    T: ObjectTypesStore + ObjectStore,
    S: HasFuzzMetadata + HasRand,
{
    let mut param_ty_arg_candidates: Vec<BTreeMap<u16, TypeTag>> = Vec::new();
    for db_ty_tag in db_type_tags.iter() {
        if let Some(ty_args) = ty.extract_ty_args(db_ty_tag) {
            if ty_args.iter().any(|(idx, ty_tag)| {
                let ability_set = get_type_tag_ability(ty_tag, state);
                !function_abilities[*idx as usize].is_subset(ability_set)
            }) {
                continue; // Skip if any type argument does not match the function's abilities
            }
            let TypeTag::Struct(db_ty_tag) = db_ty_tag.clone() else {
                panic!("Expected a struct type tag");
            };
            match gate {
                Gate::Owned => {
                    if db
                        .get_mutable_objects_by_ty(
                            &(*db_ty_tag).into(),
                            state.fuzz_state().current_sender,
                        )
                        .is_empty()
                    {
                        continue; // Skip if no owned objects are found
                    }
                }
                Gate::Immutable => {
                    if db
                        .get_objects_by_ty(&(*db_ty_tag).into(), state.fuzz_state().current_sender)
                        .is_empty()
                    {
                        continue; // Skip if no immutable objects are found
                    }
                }
                Gate::Shared => {
                    if db
                        .get_mutable_objects_by_ty(
                            &(*db_ty_tag).into(),
                            state.fuzz_state().current_sender,
                        )
                        .is_empty()
                    {
                        continue; // Skip if no shared objects are found
                    }
                }
            }
            param_ty_arg_candidates.push(ty_args);
        } else {
            trace!(
                "Failed to extract type arguments for type {:?} from db type tag {:?}",
                ty, db_ty_tag
            );
        }
    }
    param_ty_arg_candidates
}

// return a map of ty param index to type arguments
fn try_sample_object_from_cache<S>(
    existing_objects: &BTreeMap<TypeTag, Vec<(Argument, Gate)>>,
    ty: &SuiMoveNormalizedType,
    function_abilities: &[AbilitySet],
    gate: Gate,
    state: &mut S,
) -> Vec<BTreeMap<u16, TypeTag>>
where
    S: HasFuzzMetadata + HasRand,
{
    let mut param_ty_arg_candidates: Vec<BTreeMap<u16, TypeTag>> = Vec::new();
    for (db_ty_tag, candidates) in existing_objects.iter() {
        if let Some(ty_args) = ty.extract_ty_args(db_ty_tag) {
            if ty_args.iter().any(|(idx, ty_tag)| {
                let ability_set = get_type_tag_ability(ty_tag, state);
                !function_abilities[*idx as usize].is_subset(ability_set)
            }) {
                continue; // Skip if any type argument does not match the function's abilities
            }
            let TypeTag::Struct(_) = db_ty_tag.clone() else {
                panic!("Expected a struct type tag");
            };
            match gate {
                Gate::Owned => {
                    if candidates.iter().all(|(_, gate)| gate != &Gate::Owned) {
                        continue; // Skip if no owned objects are found
                    }
                }
                Gate::Immutable => {}
                Gate::Shared => {
                    if candidates.iter().all(|(_, gate)| gate == &Gate::Immutable) {
                        continue; // Skip if no shared objects are found
                    }
                }
            }
            param_ty_arg_candidates.push(ty_args)
        }
    }
    param_ty_arg_candidates
}

// return a map of parameter index to object ID and type arguments
pub fn try_construct_args_from_db<T, S>(
    movecall: &ProgrammableMoveCall,
    db: &T,
    state: &mut S,
) -> Option<(BTreeMap<u16, ObjectID>, Vec<TypeInput>)>
where
    T: ObjectTypesStore + ObjectStore,
    S: HasFuzzMetadata + HasRand,
{
    let function = state
        .fuzz_state()
        .get_function(&movecall.package, &movecall.module, &movecall.function)
        .unwrap();
    let db_type_tags = db
        .tys()
        .iter()
        .map(|ty| ty.clone().into())
        .collect::<Vec<TypeTag>>();
    debug!(
        "Sampling objects for function {}::{}::{}",
        movecall.package, movecall.module, movecall.function
    );

    let function_abilities = function
        .type_parameters
        .iter()
        .map(sui_move_ability_set_to_ability_set)
        .collect::<Vec<_>>();
    let param_with_gate: Vec<(u16, SuiMoveNormalizedType, Gate)> = function
        .parameters
        .iter()
        .enumerate()
        .filter_map(|(i, param)| {
            if param.is_tx_context() {
                return None; // Skip TxContext parameters
            };
            match param {
                SuiMoveNormalizedType::Struct { .. } => {
                    Some((i as u16, param.clone(), Gate::Owned))
                }
                SuiMoveNormalizedType::Reference(b) => match b.as_ref() {
                    SuiMoveNormalizedType::Struct { .. } => {
                        Some((i as u16, b.as_ref().clone(), Gate::Immutable))
                    }
                    _ => None,
                },
                SuiMoveNormalizedType::MutableReference(b) => match b.as_ref() {
                    SuiMoveNormalizedType::Struct { .. } => {
                        Some((i as u16, b.as_ref().clone(), Gate::Shared))
                    }
                    _ => None,
                },
                _ => None,
            }
        })
        .collect();

    // generate type argument candidates for each parameter
    let mut ty_args_candidates: Vec<Vec<BTreeMap<u16, TypeTag>>> = Vec::new();
    for (i, param, gate) in param_with_gate.iter() {
        let param_ty_arg_candidates =
            try_sample_object_from_db(param, &db_type_tags, &function_abilities, *gate, db, state);
        if param_ty_arg_candidates.is_empty() {
            // If no candidates are found for this parameter, we cannot proceed
            debug!(
                "No candidates found for parameter {} with type {:?} and gate {:?}",
                i, param, gate
            );
            return None;
        };
        ty_args_candidates.push(param_ty_arg_candidates);
    }

    // generate type arguments based on the candidates
    let ty_args_candidates = get_ty_args_candidates(&ty_args_candidates);
    if ty_args_candidates.is_empty() {
        debug!("No valid type argument candidates found for the function");
        return None; // If no valid type argument candidates are found, we cannot proceed
    }
    let ty_args_map = &ty_args_candidates[state.rand_mut().below_or_zero(ty_args_candidates.len())];
    let ty_args = (0..function_abilities.len())
        .map(|i| {
            ty_args_map
                .get(&(i as u16))
                .cloned()
                .unwrap_or_else(|| gen_type_tag_by_abilities(&function_abilities[i], state))
        })
        .collect::<Vec<_>>();

    // Substitute the type arguments into the parameters and sample objects
    let mut object_candidates = BTreeMap::new();
    for (i, param, gate) in param_with_gate.iter() {
        let instantiated_ty = param
            .subst(ty_args_map)
            .expect("Failed to substitute type arguments");
        let TypeTag::Struct(tag) = instantiated_ty else {
            panic!("Expected a struct type tag for parameter {}", i);
        };
        debug!(
            "Sampling objects for parameter {} with type {:?} and gate {:?}",
            i, tag, gate
        );
        let object_ids = match gate {
            Gate::Owned => {
                db.get_mutable_objects_by_ty(&(*tag).into(), state.fuzz_state().current_sender)
            }
            Gate::Immutable => {
                db.get_objects_by_ty(&(*tag).into(), state.fuzz_state().current_sender)
            }
            Gate::Shared => {
                db.get_mutable_objects_by_ty(&(*tag).into(), state.fuzz_state().current_sender)
            }
        };
        object_candidates.insert(
            *i,
            object_ids.into_iter().map(|(id, _)| id).collect::<Vec<_>>(),
        );
    }
    if object_candidates.iter().any(|(_, ids)| ids.is_empty()) {
        debug!("No object candidates found for some parameters");
        return None;
    }
    let objects = object_candidates
        .into_iter()
        .map(|(index, ids)| {
            if ids.is_empty() {
                panic!(
                    "No objects found for parameter {} with type {:?} and gate {:?}",
                    index,
                    param_with_gate
                        .iter()
                        .find(|(i, _, _)| *i == index)
                        .map(|(_, ty, _)| ty),
                    param_with_gate
                        .iter()
                        .find(|(i, _, _)| *i == index)
                        .map(|(_, _, gate)| gate)
                );
            }
            let id = ids[state.rand_mut().below_or_zero(ids.len())];
            (index, id)
        })
        .collect::<BTreeMap<u16, ObjectID>>();

    Some((
        objects,
        ty_args.into_iter().map(|ty_arg| ty_arg.into()).collect(),
    ))
}

// return a map of parameter index to argument and type arguments
pub fn try_construct_args<T, S>(
    input_len: usize,
    params: &Vec<SuiMoveNormalizedType>,
    function_abilities: &[SuiMoveAbilitySet],
    fixed_ty_args: &BTreeMap<u16, TypeTag>,
    object_data: &ObjectData,
    db: &T,
    state: &mut S,
) -> ConstructResult
where
    T: ObjectTypesStore + ObjectStore,
    S: HasFuzzMetadata + HasRand,
{
    let mut existing_objects = object_data.existing_objects.clone();
    let mut used_object_ids = object_data.used_object_ids.clone();
    let db_type_tags = db
        .tys()
        .iter()
        .map(|ty| ty.clone().into())
        .collect::<Vec<TypeTag>>();

    if params.iter().any(|param| match param {
        SuiMoveNormalizedType::Struct { .. } => false,
        SuiMoveNormalizedType::Reference(b) => match b.as_ref() {
            SuiMoveNormalizedType::Struct { .. } => false,
            _ => true,
        },
        SuiMoveNormalizedType::MutableReference(b) => match b.as_ref() {
            SuiMoveNormalizedType::Struct { .. } => false,
            _ => true,
        },
        _ => true,
    }) {
        debug!("Skipping function with vector of struct parameters or type parameters");
        return ConstructResult::Unsolvable; // Skip functions with reference or mutable reference parameters
    }

    let function_abilities = function_abilities
        .iter()
        .map(sui_move_ability_set_to_ability_set)
        .collect::<Vec<_>>();

    let param_with_gate: Vec<(SuiMoveNormalizedType, Gate)> = params
        .iter()
        .map(|param| {
            if param.is_tx_context() {
                panic!("should not be tx context"); // Skip TxContext parameters
            };
            let param = param.partial_subst(fixed_ty_args);
            match param {
                SuiMoveNormalizedType::Struct { .. } => (param.clone(), Gate::Owned),
                SuiMoveNormalizedType::Reference(b) => match b.as_ref() {
                    SuiMoveNormalizedType::Struct { .. } => (b.as_ref().clone(), Gate::Immutable),
                    _ => unreachable!(),
                },
                SuiMoveNormalizedType::MutableReference(b) => match b.as_ref() {
                    SuiMoveNormalizedType::Struct { .. } => (b.as_ref().clone(), Gate::Shared),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
        })
        .collect();

    // generate type argument candidates for each parameter
    let mut ty_args_candidates: Vec<Vec<BTreeMap<u16, TypeTag>>> = vec![];
    let mut missing_idx = vec![];
    for (i, (param, gate)) in param_with_gate.iter().enumerate() {
        let param_ty_arg_candidates_from_cache = try_sample_object_from_cache(
            &existing_objects,
            param,
            &function_abilities,
            *gate,
            state,
        );
        let param_ty_arg_candidates_from_db = if param
            .ability(state.fuzz_state())
            .unwrap()
            .has_key()
        {
            try_sample_object_from_db(param, &db_type_tags, &function_abilities, *gate, db, state)
        } else {
            vec![]
        };
        let param_ty_arg_candidates = param_ty_arg_candidates_from_cache
            .into_iter()
            .chain(param_ty_arg_candidates_from_db.into_iter())
            .collect::<Vec<_>>();

        if param_ty_arg_candidates.is_empty() {
            debug!(
                "No candidates found for parameter {:?} and gate {:?}",
                param, gate
            );
            missing_idx.push(i as u16);
            continue;
        }

        ty_args_candidates.push(param_ty_arg_candidates);
    }

    // generate type arguments based on the candidates
    let ty_args_candidates = get_ty_args_candidates(&ty_args_candidates);
    if ty_args_candidates.is_empty() {
        debug!("No valid type argument candidates found for the function");
        return ConstructResult::Unsolvable; // If no valid type argument candidates are found, we cannot proceed
    }
    let ty_args_map = &ty_args_candidates[state.rand_mut().below_or_zero(ty_args_candidates.len())];

    // Substitute the type arguments into the parameters and sample objects
    let mut return_args = Vec::new();
    let mut inputs = Vec::new();
    for (i, (param, gate)) in param_with_gate.iter().enumerate() {
        if missing_idx.contains(&(i as u16)) {
            debug!(
                "Skipping parameter {} with type {:?} and gate {:?} due to missing candidates",
                i, param, gate
            );
            return_args.push(None);
            continue; // Skip parameters that have no candidates
        }
        let instantiated_ty = param
            .subst(ty_args_map)
            .expect("Failed to substitute type arguments");
        let TypeTag::Struct(tag) = instantiated_ty.clone() else {
            panic!("Expected a struct type tag for parameter");
        };
        if !matches!(param, SuiMoveNormalizedType::Struct { .. }) {
            panic!("Expected a struct type for parameter");
        };
        debug!("Sampling objects for parameter {} and gate {:?}", tag, gate);
        if let Some(args) = existing_objects.get_mut(&instantiated_ty) {
            let filtered_idxes: Vec<usize> = args
                .iter()
                .enumerate()
                .filter(|(_, (_, g))| match gate {
                    Gate::Owned => g != &Gate::Immutable,
                    Gate::Immutable => true,
                    Gate::Shared => g != &Gate::Immutable,
                })
                .map(|(i, _)| i)
                .collect();
            if !filtered_idxes.is_empty() {
                let (arg, _) = if
                // !param.ability(state.fuzz_state()).has_copy() ignore Copy ability
                gate == &Gate::Owned {
                    args.remove(
                        filtered_idxes[state.rand_mut().below_or_zero(filtered_idxes.len())],
                    )
                } else {
                    *args
                        .get(filtered_idxes[state.rand_mut().below_or_zero(filtered_idxes.len())])
                        .unwrap()
                };
                return_args.push(Some(arg));
                continue;
            } else {
                debug!(
                    "No existing objects found for parameter {:?} and gate {:?}",
                    param, gate
                );
                return_args.push(None);
                missing_idx.push(i as u16);
                continue; // If no objects are found, skip this parameter
            }
        }

        // Sample an object from the db
        let object_ids = match gate {
            Gate::Owned => {
                db.get_mutable_objects_by_ty(&(*tag).into(), state.fuzz_state().current_sender)
            }
            Gate::Immutable => {
                db.get_objects_by_ty(&(*tag).into(), state.fuzz_state().current_sender)
            }
            Gate::Shared => {
                db.get_mutable_objects_by_ty(&(*tag).into(), state.fuzz_state().current_sender)
            }
        };
        let object_ids = object_ids
            .into_iter()
            .filter(|(id, _)| !used_object_ids.contains(id))
            .collect::<Vec<_>>();
        if object_ids.is_empty() {
            debug!(
                "No objects found in DB for tag {:?} and gate {:?}",
                param, gate
            );
            return_args.push(None);
            missing_idx.push(i as u16);
            continue; // If no objects are found, skip this parameter
        }

        let object_id = object_ids[state.rand_mut().below_or_zero(object_ids.len())].0;
        used_object_ids.push(object_id);
        let Some(object) = db.get_object(&object_id) else {
            warn!("Object with ID {:?} not found in the database", object_id);
            return ConstructResult::Unsolvable;
        };
        match object.owner {
            Owner::AddressOwner(_) | Owner::Immutable => {
                inputs.push(CallArg::Object(ObjectArg::ImmOrOwnedObject((
                    object_id,
                    object.version(),
                    object.digest(),
                ))));
            }
            Owner::Shared {
                initial_shared_version,
            } => {
                inputs.push(CallArg::Object(ObjectArg::SharedObject {
                    id: object_id,
                    initial_shared_version,
                    mutable: true,
                }));
            }
            _ => {
                warn!("Unsupported object owner type: {:?}", object.owner);
                return ConstructResult::Unsolvable;
            }
        }
        let arg = Argument::Input((input_len + inputs.len() - 1) as u16);
        return_args.push(Some(arg));
    }

    if missing_idx.is_empty() {
        let ty_args = (0..function_abilities.len())
            .map(|i| {
                ty_args_map
                    .get(&(i as u16))
                    .cloned()
                    .unwrap_or_else(|| {
                        fixed_ty_args
                            .get(&(i as u16))
                            .unwrap_or(&gen_type_tag_by_abilities(&function_abilities[i], state))
                            .clone()
                    })
                    .into()
            })
            .collect::<Vec<_>>();
        ConstructResult::Ok(return_args.into_iter().flatten().collect(), ty_args, inputs)
    } else {
        ConstructResult::PartialFound(
            return_args,
            ty_args_map
                .iter()
                .chain(fixed_ty_args.iter())
                .map(|(k, v)| (*k, v.clone()))
                .collect(),
            inputs,
        )
    }
}

pub fn update_score(hot_potatoes: &Vec<TypeTag>, meta: &mut FuzzMetadata) {
    let mut increased_functions = BTreeSet::new();
    for type_tag in hot_potatoes.iter() {
        for (struct_ty, consumer_functions) in meta.consuming_graph.iter() {
            if struct_ty.extract_ty_args(type_tag).is_some() {
                for consumer_func in consumer_functions {
                    meta.function_scores
                        .entry(consumer_func.clone())
                        .and_modify(|score| *score += SCORE_TICK);
                    increased_functions.insert(consumer_func);
                }
            }
        }
    }
    for (func, score) in meta.function_scores.iter_mut() {
        if !increased_functions.contains(func) {
            if meta.privilege_functions.contains(&func) {
                *score = PRIVILEGE_FUNCTION_SCORE;
            } else {
                *score = INIT_FUNCTION_SCORE;
            }
        }
    }
}
