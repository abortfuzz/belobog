use std::collections::BTreeSet;

use belobog_fork::ObjectTypesStore;
use belobog_types::error::BelobogError;
use libafl::executors::ExitKind;
use libafl_bolts::Named;
use log::{info, trace};
use move_binary_format::{
    binary_config::BinaryConfig,
    file_format::{Bytecode, SignatureToken},
    internals::ModuleIndex,
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
pub struct UnusedStructOracle {
    pub otw: bool,
}

impl Named for UnusedStructOracle {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("UnusedStructOracle")
    }
}

impl<S> Oracle<S> for UnusedStructOracle
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
            for module in modules {
                let Ok(module_data) = pkg_obj
                    .data
                    .try_as_package()
                    .unwrap()
                    .deserialize_module_by_str(module, &BinaryConfig::new_unpublishable())
                else {
                    continue;
                };
                let struct_pool = module_data.struct_defs();
                let enum_pool = module_data.enum_defs();
                let mut struct_is_visited = vec![false; struct_pool.len()];
                let mut enum_is_visited = vec![false; enum_pool.len()];
                for function in module_data.function_defs() {
                    // function parameters
                    let function_handle = module_data.function_handle_at(function.function);
                    let parameters = module_data.signature_at(function_handle.parameters);
                    parameters.0.iter().for_each(|sig| {
                        if let SignatureToken::Datatype(idx) = sig {
                            let struct_idx =
                                struct_pool.iter().position(|s| s.struct_handle == *idx);
                            if let Some(sid) = struct_idx {
                                struct_is_visited[sid] = true;
                            }
                        }
                    });

                    if let Some(codes) = &function.code {
                        for code in codes.code.iter() {
                            trace!("Bytecode: {:?}", code);
                            match code {
                                Bytecode::Pack(idx) => {
                                    struct_is_visited[idx.into_index()] = true;
                                }
                                Bytecode::PackGeneric(idx) => {
                                    struct_is_visited[module_data
                                        .struct_instantiation_at(*idx)
                                        .def
                                        .into_index()] = true;
                                }
                                Bytecode::PackVariant(idx) => {
                                    enum_is_visited[module_data
                                        .variant_handle_at(*idx)
                                        .enum_def
                                        .into_index()] = true;
                                }
                                Bytecode::PackVariantGeneric(idx) => {
                                    enum_is_visited[module_data
                                        .variant_instantiation_handle_at(*idx)
                                        .enum_def
                                        .into_index()] = true;
                                }
                                _ => {}
                            }
                        }
                    } else {
                        continue;
                    }
                }
                let unused_struct = struct_is_visited
                    .iter()
                    .enumerate()
                    .filter_map(|(id, visited)| if !visited { Some(id) } else { None })
                    .collect::<Vec<_>>();
                let unused_enum = enum_is_visited
                    .iter()
                    .enumerate()
                    .filter_map(|(id, visited)| if !visited { Some(id) } else { None })
                    .collect::<Vec<_>>();
                if !unused_struct.is_empty() {
                    crashed = true;
                    reports.push(format!(
                        "[STATIC] Unused structs in {}::{}: {:?}",
                        pkg, module, unused_struct
                    ));
                }
                if !unused_enum.is_empty() {
                    crashed = true;
                    reports.push(format!(
                        "[STATIC] Unused enums in {}::{}: {:?}",
                        pkg, module, unused_enum
                    ));
                }
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
