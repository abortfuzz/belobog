use std::collections::BTreeSet;

use belobog_fork::ObjectTypesStore;
use belobog_types::error::BelobogError;
use libafl::executors::ExitKind;
use libafl_bolts::Named;
use log::info;
use move_binary_format::{
    binary_config::BinaryConfig,
    file_format::{Bytecode, Visibility},
};
use move_core_types::{account_address::AccountAddress, language_storage::ModuleId};
use move_trace_format::format::TraceEvent;
use sui_types::storage::BackingStore;

use crate::{
    concolic::SymbolValue,
    meta::{FunctionIdent, HasFuzzMetadata},
    state::HasExtraState,
};

use super::Oracle;

#[derive(Debug, Default, Clone, Copy)]
pub struct UnusedPrivateFunOracle {
    pub otw: bool,
}

impl Named for UnusedPrivateFunOracle {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("UnusedPrivateFunOracle")
    }
}

impl<S> Oracle<S> for UnusedPrivateFunOracle
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
        T: BackingStore + ObjectTypesStore,
    {
        if self.otw {
            return Ok((ExitKind::Ok, Vec::new()));
        }
        self.otw = true;
        let mut crashed = false;
        let mut reports = Vec::new();
        let target_packages = state
            .fuzz_state()
            .target_functions
            .iter()
            .map(|FunctionIdent(p, _, _)| *p)
            .filter(|p| p != &AccountAddress::ONE.into() && p != &AccountAddress::TWO.into())
            .collect::<BTreeSet<_>>();

        for pkg in target_packages {
            let pkg_obj = db.get_object(&pkg).unwrap();
            let modules = state
                .fuzz_state()
                .packages
                .get(&pkg)
                .unwrap()
                .abis
                .keys()
                .collect::<Vec<_>>();
            let mut unused_friend_functions = modules
                .iter()
                .flat_map(|m| {
                    let Ok(module_data) = pkg_obj
                        .data
                        .try_as_package()
                        .unwrap()
                        .deserialize_module_by_str(m, &BinaryConfig::new_unpublishable())
                    else {
                        return BTreeSet::new();
                    };
                    module_data
                        .function_defs()
                        .iter()
                        .filter(|f| matches!(f.visibility, Visibility::Friend) && !f.is_entry)
                        .map(|f| {
                            (
                                module_data.self_id(),
                                module_data
                                    .identifier_at(module_data.function_handle_at(f.function).name)
                                    .to_string(),
                            )
                        })
                        .collect::<BTreeSet<_>>()
                })
                .collect::<BTreeSet<_>>();
            for module in modules {
                let Ok(module_data) = pkg_obj
                    .data
                    .try_as_package()
                    .unwrap()
                    .deserialize_module_by_str(module, &BinaryConfig::new_unpublishable())
                else {
                    continue;
                };
                let mut unused_private_functions = module_data
                    .function_defs()
                    .iter()
                    .filter(|f| {
                        matches!(f.visibility, Visibility::Private)
                            && module_data
                                .identifier_at(module_data.function_handle_at(f.function).name)
                                .as_str()
                                != "init"
                            && !f.is_entry
                    })
                    .map(|f| {
                        (
                            module_data.self_id(),
                            module_data
                                .identifier_at(module_data.function_handle_at(f.function).name)
                                .to_string(),
                        )
                    })
                    .collect::<BTreeSet<_>>();
                for function in module_data.function_defs() {
                    let Some(code) = &function.code else {
                        continue;
                    };
                    for bytecode in &code.code {
                        if let Bytecode::Call(idx) = bytecode {
                            let function_handle = &module_data.function_handle_at(*idx);
                            let function_name = module_data.identifier_at(function_handle.name);
                            let module_id = module_data.module_id_for_handle(
                                module_data.module_handle_at(function_handle.module),
                            );
                            unused_private_functions
                                .remove(&(module_id.clone(), function_name.to_string()));
                            unused_friend_functions.remove(&(module_id, function_name.to_string()));
                        }
                        if let Bytecode::CallGeneric(idx) = bytecode {
                            let idx = &module_data.function_instantiation_at(*idx).handle;
                            let function_handle = &module_data.function_handle_at(*idx);
                            let function_name = module_data.identifier_at(function_handle.name);
                            let module_id = module_data.module_id_for_handle(
                                module_data.module_handle_at(function_handle.module),
                            );
                            unused_private_functions
                                .remove(&(module_id.clone(), function_name.to_string()));
                            unused_friend_functions.remove(&(module_id, function_name.to_string()));
                        }
                    }
                }
                if !unused_private_functions.is_empty() {
                    crashed = true;
                    reports.push(format!(
                        "[STATIC] Unused private functions in {}::{}: {:?}",
                        pkg, module, unused_private_functions
                    ));
                }
            }
            if !unused_friend_functions.is_empty() {
                crashed = true;
                reports.push(format!(
                    "[STATIC] Unused friend functions in {}: {:?}",
                    pkg, unused_friend_functions
                ));
            }
        }
        if crashed {
            Ok((ExitKind::Crash, reports))
        } else {
            Ok((ExitKind::Ok, Vec::new()))
        }
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
