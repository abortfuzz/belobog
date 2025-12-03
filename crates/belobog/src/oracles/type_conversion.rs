use belobog_fork::ObjectTypesStore;
use belobog_types::error::BelobogError;
use libafl::executors::ExitKind;
use libafl_bolts::Named;
use log::{debug, info};
use move_binary_format::file_format::Bytecode;
use move_core_types::language_storage::ModuleId;
use move_model::ty::{PrimitiveType, Type};
use move_stackless_bytecode::stackless_bytecode::{Bytecode as SLBytecode, Operation};
use move_trace_format::format::TraceEvent;
use move_vm_types::values::ValueImpl;
use sui_types::storage::BackingStore;

use crate::{
    concolic::SymbolValue, generate_bytecode::FunctionInfo, meta::HasFuzzMetadata,
    oracles::common::load_target_modules, state::HasExtraState,
};

use super::{Oracle, common::format_vulnerability_info};

#[derive(Debug, Default, Clone, Copy)]
pub struct TypeConversionOracle {
    pub otw: bool,
}

impl Named for TypeConversionOracle {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("TypeConversionOracle")
    }
}

impl<S> Oracle<S> for TypeConversionOracle
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

        let modules = load_target_modules(db, state)?;
        for module in modules {
            for function in module.functions() {
                debug!(
                    "Analyzing function: {}::{}::{}, idx: {}",
                    module.compiled.address(),
                    module.compiled.name(),
                    function.name,
                    function.idx
                );
                if module.is_native(function) {
                    continue;
                }
                if detect_unnecessary_type_conversion(function) {
                    crashed = true;
                    reports.push(format!(
                        "[STATIC] Unnecessary type conversion in {}::{}",
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
    fn event(
        &mut self,
        event: &TraceEvent,
        _state: &mut S,
        stack: Option<&move_vm_stack::Stack>,
        _symbol_stack: &Vec<SymbolValue>,
        current_function: Option<&(ModuleId, String)>,
    ) -> Result<(ExitKind, Vec<String>), BelobogError> {
        match event {
            TraceEvent::BeforeInstruction {
                type_parameters: _,
                pc,
                gas_left: _,
                instruction,
                extra: _,
            } => {
                let stack = stack.expect("no stack?!");
                if stack.value.is_empty() {
                    return Ok((ExitKind::Ok, Vec::new()));
                }
                let top_value = &stack.value[stack.value.len() - 1].0;
                let type_conversion = match (instruction, top_value) {
                    (Bytecode::CastU8, ValueImpl::U8(_)) => true,
                    (Bytecode::CastU16, ValueImpl::U16(_)) => true,
                    (Bytecode::CastU32, ValueImpl::U32(_)) => true,
                    (Bytecode::CastU64, ValueImpl::U64(_)) => true,
                    (Bytecode::CastU128, ValueImpl::U128(_)) => true,
                    (Bytecode::CastU256, ValueImpl::U256(_)) => true,
                    _ => false,
                };
                if type_conversion {
                    let message = format_vulnerability_info(
                        "Unnecessary type conversion detected",
                        current_function,
                        Some(*pc),
                    );
                    Ok((ExitKind::Crash, vec![message]))
                } else {
                    Ok((ExitKind::Ok, Vec::new()))
                }
            }
            _ => Ok((ExitKind::Ok, Vec::new())),
        }
    }
}

fn detect_unnecessary_type_conversion(function: &FunctionInfo) -> bool {
    for (offset, instr) in function.code.iter().enumerate() {
        match instr {
            SLBytecode::Call(_, _, Operation::CastU8, srcs, _) => {
                if function.local_types[srcs[0] as usize] == Type::Primitive(PrimitiveType::U8) {
                    info!(
                        "Unnecessary CastU8 at offset {} in function {}",
                        offset, function.name
                    );
                    return true;
                }
            }
            SLBytecode::Call(_, _, Operation::CastU16, srcs, _) => {
                if function.local_types[srcs[0] as usize] == Type::Primitive(PrimitiveType::U16) {
                    info!(
                        "Unnecessary CastU16 at offset {} in function {}",
                        offset, function.name
                    );
                    return true;
                }
            }
            SLBytecode::Call(_, _, Operation::CastU32, srcs, _) => {
                if function.local_types[srcs[0] as usize] == Type::Primitive(PrimitiveType::U32) {
                    info!(
                        "Unnecessary CastU32 at offset {} in function {}",
                        offset, function.name
                    );
                    return true;
                }
            }
            SLBytecode::Call(_, _, Operation::CastU64, srcs, _) => {
                if function.local_types[srcs[0] as usize] == Type::Primitive(PrimitiveType::U64) {
                    info!(
                        "Unnecessary CastU64 at offset {} in function {}",
                        offset, function.name
                    );
                    return true;
                }
            }
            SLBytecode::Call(_, _, Operation::CastU128, srcs, _) => {
                if function.local_types[srcs[0] as usize] == Type::Primitive(PrimitiveType::U128) {
                    info!(
                        "Unnecessary CastU128 at offset {} in function {}",
                        offset, function.name
                    );
                    return true;
                }
            }
            SLBytecode::Call(_, _, Operation::CastU256, srcs, _) => {
                if function.local_types[srcs[0] as usize] == Type::Primitive(PrimitiveType::U256) {
                    info!(
                        "Unnecessary CastU256 at offset {} in function {}",
                        offset, function.name
                    );
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}
