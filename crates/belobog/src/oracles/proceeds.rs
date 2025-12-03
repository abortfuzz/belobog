use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ops::Neg,
};

use belobog_types::error::BelobogError;
use libafl::executors::ExitKind;
use libafl_bolts::Named;
use log::{debug, info};
use move_core_types::language_storage::ModuleId;
use move_trace_format::format::TraceEvent;
use sui_json_rpc_types::BalanceChange;
use sui_types::{
    TypeTag,
    base_types::{ObjectID, SequenceNumber},
    coin::Coin,
    digests::ObjectDigest,
    effects::{TransactionEffects, TransactionEffectsAPI},
    execution_status::ExecutionStatus,
    gas_coin::GAS,
    object::Owner,
    storage::{BackingStore, ObjectStore},
    transaction::InputObjectKind,
};

use crate::{concolic::SymbolValue, meta::HasFuzzMetadata, state::HasExtraState};

use super::Oracle;

#[derive(Debug, Default, Clone, Copy)]
pub struct ProceedsOracle;

pub fn get_balance_changes_from_effect<P: ObjectStore>(
    object_provider: &P,
    effects: &TransactionEffects,
    input_objs: Vec<InputObjectKind>,
    mocked_coin: Option<ObjectID>,
) -> Option<Vec<BalanceChange>> {
    let (_, gas_owner) = effects.gas_object();

    // Only charge gas when tx fails, skip all object parsing
    if effects.status() != &ExecutionStatus::Success {
        return Some(vec![BalanceChange {
            owner: gas_owner,
            coin_type: GAS::type_tag(),
            amount: effects.gas_cost_summary().net_gas_usage().neg() as i128,
        }]);
    }

    let all_mutated = effects
        .all_changed_objects()
        .into_iter()
        .filter_map(|((id, version, digest), _, _)| {
            if matches!(mocked_coin, Some(coin) if id == coin) {
                return None;
            }
            Some((id, version, Some(digest)))
        })
        .collect::<Vec<_>>();

    let input_objs_to_digest = input_objs
        .iter()
        .filter_map(|k| match k {
            InputObjectKind::ImmOrOwnedMoveObject(o) => Some((o.0, o.2)),
            InputObjectKind::MovePackage(_) | InputObjectKind::SharedMoveObject { .. } => None,
        })
        .collect::<HashMap<ObjectID, ObjectDigest>>();
    let unwrapped_then_deleted = effects
        .unwrapped_then_deleted()
        .iter()
        .map(|e| e.0)
        .collect::<HashSet<_>>();
    get_balance_changes(
        object_provider,
        &effects
            .modified_at_versions()
            .into_iter()
            .filter_map(|(id, version)| {
                if matches!(mocked_coin, Some(coin) if id == coin) {
                    return None;
                }
                // We won't be able to get dynamic object from object provider today
                if unwrapped_then_deleted.contains(&id) {
                    return None;
                }
                Some((id, version, input_objs_to_digest.get(&id).cloned()))
            })
            .collect::<Vec<_>>(),
        &all_mutated,
    )
}

pub fn get_balance_changes<P: ObjectStore>(
    object_provider: &P,
    modified_at_version: &[(ObjectID, SequenceNumber, Option<ObjectDigest>)],
    all_mutated: &[(ObjectID, SequenceNumber, Option<ObjectDigest>)],
) -> Option<Vec<BalanceChange>> {
    // 1. subtract all input coins
    let balances = fetch_coins(object_provider, modified_at_version)?
        .into_iter()
        .fold(
            BTreeMap::<_, i128>::new(),
            |mut acc, (owner, type_, amount)| {
                *acc.entry((owner, type_)).or_default() -= amount as i128;
                acc
            },
        );
    // 2. add all mutated coins
    let balances = fetch_coins(object_provider, all_mutated)?.into_iter().fold(
        balances,
        |mut acc, (owner, type_, amount)| {
            *acc.entry((owner, type_)).or_default() += amount as i128;
            acc
        },
    );

    Some(
        balances
            .into_iter()
            .filter_map(|((owner, coin_type), amount)| {
                if amount == 0 {
                    return None;
                }
                Some(BalanceChange {
                    owner,
                    coin_type,
                    amount,
                })
            })
            .collect(),
    )
}

fn fetch_coins<P: ObjectStore>(
    object_provider: &P,
    objects: &[(ObjectID, SequenceNumber, Option<ObjectDigest>)],
) -> Option<Vec<(Owner, TypeTag, u64)>> {
    let mut all_mutated_coins = vec![];
    for (id, version, digest_opt) in objects {
        // TODO: use multi get object
        let o = object_provider.get_object_by_key(id, *version)?;
        if let Some(type_) = o.type_() {
            if type_.is_coin() {
                if let Some(digest) = digest_opt {
                    // TODO: can we return Err here instead?
                    assert_eq!(
                        *digest,
                        o.digest(),
                        "Object digest mismatch--got bad data from object_provider?"
                    )
                }
                let [coin_type]: [TypeTag; 1] =
                    type_.clone().into_type_params().try_into().unwrap();
                all_mutated_coins.push((
                    o.owner.clone(),
                    coin_type,
                    // we know this is a coin, safe to unwrap
                    Coin::extract_balance_if_coin(&o).unwrap().unwrap().1,
                ))
            }
        }
    }
    Some(all_mutated_coins)
}

impl Named for ProceedsOracle {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("ProceedsOracle")
    }
}

impl<S> Oracle<S> for ProceedsOracle
where
    S: HasFuzzMetadata + HasExtraState,
{
    fn done_execution<T>(
        &mut self,
        db: &T,
        state: &mut S,
        _stack: Option<&move_vm_stack::Stack>,
    ) -> Result<(ExitKind, Vec<String>), BelobogError>
    where
        T: BackingStore,
    {
        if state
            .extra_state()
            .global_outcome
            .as_ref()
            .is_some_and(|o| o.outcome.error.is_some())
        {
            return Ok((ExitKind::Ok, Vec::new()));
        }
        let Some(global_outcome) = state.extra_state().global_outcome.as_ref() else {
            debug!("No global outcome");
            return Ok((ExitKind::Ok, Vec::new()));
        };
        let balance_change = get_balance_changes_from_effect(
            db,
            &global_outcome.outcome.effects,
            global_outcome.outcome.input_objects.clone(),
            state.fuzz_state().gas_id,
        );
        debug!("gas id: {:?}", state.fuzz_state().gas_id);
        match balance_change {
            Some(bc) => {
                debug!("Balance change: {:?}", bc);
                if bc.iter().all(|c| c.amount >= 0) && bc.iter().any(|c| c.amount > 0) {
                    debug!("Found proceeds: {:?}", bc);
                    let mut message = String::from("Positive proceeds detected");
                    message.push_str(&format!(": {:?}", bc));
                    return Ok((ExitKind::Crash, vec![message]));
                }
            }
            None => {
                debug!("Failed to get balance change");
            }
        }
        Ok((ExitKind::Ok, Vec::new()))
    }
    fn event(
        &mut self,
        _event: &TraceEvent,
        _state: &mut S,
        _stack: Option<&move_vm_stack::Stack>,
        _symbol_stack: &Vec<SymbolValue>,
        _current_function: Option<&(ModuleId, String)>,
    ) -> Result<(ExitKind, Vec<String>), BelobogError> {
        Ok((ExitKind::Ok, Vec::new()))
    }
}
