use belobog_types::error::BelobogError;
use libafl::executors::ExitKind;
use libafl_bolts::Named;
use log::info;
use move_binary_format::file_format::Bytecode;
use move_core_types::language_storage::ModuleId;
use move_trace_format::format::TraceEvent;
use move_vm_types::values::{Value, ValueImpl};

use crate::{concolic::SymbolValue, meta::HasFuzzMetadata};

use super::{Oracle, common::format_vulnerability_info};

#[derive(Debug, Default, Clone, Copy)]
pub struct OverflowOracle;

impl Named for OverflowOracle {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("OverflowOracle")
    }
}

impl<S> Oracle<S> for OverflowOracle
where
    S: HasFuzzMetadata,
{
    fn done_execution<T>(
        &mut self,
        _db: &T,
        _state: &mut S,
        _stack: Option<&move_vm_stack::Stack>,
    ) -> Result<(ExitKind, Vec<String>), BelobogError> {
        Ok((ExitKind::Ok, Vec::new()))
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
                let overflow = match instruction {
                    Bytecode::Shl => {
                        let stack_len = stack.value.len();
                        let rhs = &stack.value[stack_len - 1];
                        let lhs = &stack.value[stack_len - 2];
                        match (lhs, rhs) {
                            (Value(ValueImpl::U8(l)), Value(ValueImpl::U8(r))) => {
                                (*l).overflowing_shl((*r).into()).1
                            }
                            (Value(ValueImpl::U16(l)), Value(ValueImpl::U8(r))) => {
                                (*l).overflowing_shl((*r).into()).1
                            }
                            (Value(ValueImpl::U32(l)), Value(ValueImpl::U8(r))) => {
                                (*l).overflowing_shl((*r).into()).1
                            }
                            (Value(ValueImpl::U64(l)), Value(ValueImpl::U8(r))) => {
                                (*l).overflowing_shl((*r).into()).1
                            }
                            (Value(ValueImpl::U128(l)), Value(ValueImpl::U8(r))) => {
                                (*l).overflowing_shl((*r).into()).1
                            }
                            (Value(ValueImpl::U256(l)), Value(ValueImpl::U8(r))) => {
                                let l = **l;
                                // debug!("shl: {:?} << {:?}, result: {:?}", l, *r, (l << (*r)));
                                l << (*r) < l
                            }
                            _ => panic!("shl with non-integer values: {:?} {:?}", lhs, rhs),
                        }
                    }
                    _ => false,
                };
                if overflow {
                    let message =
                        format_vulnerability_info("Overflow detected", current_function, Some(*pc));
                    Ok((ExitKind::Crash, vec![message]))
                } else {
                    Ok((ExitKind::Ok, Vec::new()))
                }
            }
            _ => Ok((ExitKind::Ok, Vec::new())),
        }
    }
}
