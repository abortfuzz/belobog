use std::collections::BTreeMap;

use belobog_fork::ObjectTypesStore;
use belobog_types::error::BelobogError;
use libafl::executors::ExitKind;
use libafl_bolts::Named;
use log::info;
use move_binary_format::file_format::Bytecode;
use move_core_types::language_storage::ModuleId;
use move_stackless_bytecode::stackless_bytecode::{Bytecode as SLBytecode, Constant};
use move_trace_format::format::TraceEvent;
use sui_types::storage::BackingStore;

use crate::{
    concolic::SymbolValue, generate_bytecode::FunctionInfo, meta::HasFuzzMetadata,
    utils::hash_to_u64,
};

use super::{
    Oracle,
    common::{format_vulnerability_info, get_def_bytecode, load_target_modules},
};

#[derive(Debug, Default, Clone)]
pub struct InfiniteLoopOracle {
    pub otw: bool,
    pub branch_counts: BTreeMap<u64, BTreeMap<u16, (u64, usize)>>,
}

impl Named for InfiniteLoopOracle {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("InfiniteLoopOracle")
    }
}

impl<S> Oracle<S> for InfiniteLoopOracle
where
    S: HasFuzzMetadata,
{
    fn event(
        &mut self,
        event: &TraceEvent,
        _state: &mut S,
        _stack: Option<&move_vm_stack::Stack>,
        symbol_stack: &Vec<SymbolValue>,
        current_function: Option<&(ModuleId, String)>,
    ) -> Result<(ExitKind, Vec<String>), BelobogError> {
        match event {
            TraceEvent::OpenFrame { frame, gas_left: _ } => {
                let key = format!("{}::{}", frame.module, frame.function_name);
                let key = hash_to_u64(&key);
                self.branch_counts.remove(&key);
            }
            TraceEvent::BeforeInstruction {
                type_parameters: _,
                pc,
                gas_left: _,
                instruction,
                extra: _,
            } => {
                match instruction {
                    Bytecode::BrFalse(_) | Bytecode::BrTrue(_) => {
                        let Some((module_id, function_name)) = current_function else {
                            return Ok((ExitKind::Ok, Vec::new()));
                        };
                        if symbol_stack.is_empty() {
                            return Ok((ExitKind::Ok, Vec::new()));
                        }
                        let cond_symbol = &symbol_stack[symbol_stack.len() - 1];
                        match cond_symbol {
                            SymbolValue::Unknown => return Ok((ExitKind::Ok, Vec::new())),
                            SymbolValue::Value(v) => {
                                let key = format!("{}::{}", module_id, function_name);
                                let key = hash_to_u64(&key);
                                let v = hash_to_u64(&v.to_string());
                                let count = self
                                    .branch_counts
                                    .entry(key)
                                    .or_default()
                                    .entry(*pc)
                                    .or_default();
                                if count.0 != v {
                                    // Reset count if condition value changes
                                    count.0 = v;
                                    count.1 = 1;
                                } else {
                                    if count.1 >= 1000 {
                                        count.1 = 0;
                                        let message = format_vulnerability_info(
                                            "Potential infinite loop detected",
                                            current_function,
                                            Some(*pc),
                                        );
                                        return Ok((ExitKind::Crash, vec![message]));
                                    }
                                    count.1 += 1;
                                }
                            }
                        }
                    }
                    _ => {}
                };
            }
            _ => {}
        }
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
                if detect_infinite_loop(function) {
                    crashed = true;
                    reports.push(format!(
                        "[STATIC] Potential infinite loop in {}::{}",
                        module.qualified_module_name(),
                        function.name
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

fn detect_infinite_loop(function: &FunctionInfo) -> bool {
    use move_binary_format::file_format::CodeOffset;
    let label_offsets = SLBytecode::label_offsets(&function.code);
    for (offset, instr) in function.code.iter().enumerate() {
        if let SLBytecode::Branch(_, then_label, else_label, cond) = instr {
            let Some(def_instr) = get_def_bytecode(function, *cond, offset) else {
                continue;
            };
            let constant = match def_instr {
                SLBytecode::Load(_, _, Constant::Bool(v)) => Some(*v),
                _ => None,
            };
            let Some(value) = constant else {
                continue;
            };
            let current_offset = offset as CodeOffset;
            let then_offset = match label_offsets.get(then_label) {
                Some(v) => *v,
                None => continue,
            };
            let else_offset = match label_offsets.get(else_label) {
                Some(v) => *v,
                None => continue,
            };

            if value && then_offset <= current_offset {
                return true;
            }
            if !value && else_offset <= current_offset {
                return true;
            }
        }
    }
    false
}
