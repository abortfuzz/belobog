use std::collections::BTreeSet;

use belobog_fork::ObjectTypesStore;
use belobog_types::error::BelobogError;
use libafl::executors::ExitKind;
use libafl_bolts::Named;
use log::{debug, info, trace};
use move_binary_format::{
    binary_config::BinaryConfig, file_format::Bytecode, internals::ModuleIndex,
};
use move_core_types::runtime_value::MoveValue;
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
pub struct UnusedConstOracle {
    pub otw: bool,
}

impl Named for UnusedConstOracle {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("UnusedConstOracle")
    }
}

impl<S> Oracle<S> for UnusedConstOracle
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
                let const_pool = &module_data.constant_pool;
                debug!(
                    "Analyzing constant pool of {}::{}: {:?}",
                    pkg, module, const_pool
                );
                let len = const_pool.len();
                let mut is_visited = vec![false; len];
                for function in module_data.function_defs() {
                    if let Some(codes) = &function.code {
                        for code in codes.code.iter() {
                            trace!("Visiting bytecode: {:?}", code);
                            match code {
                                Bytecode::LdConst(idx) => {
                                    is_visited[idx.into_index()] = true;
                                }
                                _ => {}
                            }
                        }
                    } else {
                        continue;
                    }
                }
                let mut unused_value: Vec<MoveValue> = vec![];
                for (id, visited) in is_visited.into_iter().enumerate() {
                    if !visited {
                        let constant = &const_pool[id];
                        let value = constant.deserialize_constant().unwrap();
                        unused_value.push(value);
                    }
                }
                if !unused_value.is_empty() {
                    crashed = true;
                    reports.push(format!(
                        "[STATIC] Unused constants in {}::{}: {:?}",
                        pkg, module, unused_value
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
