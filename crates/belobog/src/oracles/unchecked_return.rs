use belobog_fork::ObjectTypesStore;
use belobog_types::error::BelobogError;
use libafl::executors::ExitKind;
use libafl_bolts::Named;
use log::info;
use move_core_types::language_storage::ModuleId;
use move_model::model::FunId;
use move_stackless_bytecode::stackless_bytecode::{Bytecode, Operation};
use move_trace_format::format::TraceEvent;
use sui_types::storage::BackingStore;

use crate::{concolic::SymbolValue, generate_bytecode::FunctionInfo, meta::HasFuzzMetadata};

use super::{Oracle, common::load_target_modules};

#[derive(Debug, Default, Clone, Copy)]
pub struct UncheckedReturnOracle {
    pub otw: bool,
}

impl Named for UncheckedReturnOracle {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("UncheckedReturnOracle")
    }
}

impl<S> Oracle<S> for UncheckedReturnOracle
where
    S: HasFuzzMetadata,
{
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

        let modules = load_target_modules(db, state)?;
        for module in modules {
            for function in module.functions() {
                if module.is_native(function) {
                    continue;
                }
                for fid in detect_unchecked_return(function) {
                    crashed = true;
                    reports.push(format!(
                        "[STATIC] Unchecked return value in {}::{} -> {}",
                        module.qualified_module_name(),
                        function.name,
                        module.get_function_name(&fid)
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
}

fn detect_unchecked_return(function: &FunctionInfo) -> Vec<FunId> {
    let mut fids = vec![];
    for (offset, instr) in function.code.iter().enumerate() {
        if let Bytecode::Call(_, dsts, Operation::Function(_, fid, _), _, _) = instr {
            if dsts.is_empty() {
                continue;
            }
            let mut consumed = 0usize;
            let mut idx = offset + 1;
            while idx < function.code.len() && consumed < dsts.len() {
                match &function.code[idx] {
                    Bytecode::Call(_, _, Operation::Destroy, srcs, _) => {
                        if srcs.len() == 1 && dsts.contains(&srcs[0]) {
                            consumed += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                idx += 1;
            }
            if consumed > 0 {
                fids.push(*fid);
            }
        }
    }
    fids
}
