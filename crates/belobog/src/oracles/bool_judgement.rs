use belobog_fork::ObjectTypesStore;
use belobog_types::error::BelobogError;
use libafl::executors::ExitKind;
use libafl_bolts::Named;
use log::info;
use move_binary_format::file_format::Bytecode;
use move_core_types::language_storage::ModuleId;
use move_stackless_bytecode::stackless_bytecode::{Bytecode as SLBytecode, Constant, Operation};
use move_trace_format::format::TraceEvent;
use move_vm_types::values::ValueImpl;
use sui_types::storage::BackingStore;
use z3::{
    DeclKind,
    ast::{Ast, Dynamic, Int},
};

use crate::{concolic::SymbolValue, generate_bytecode::FunctionInfo, meta::HasFuzzMetadata};

use super::{
    Oracle,
    common::{format_vulnerability_info, get_def_bytecode, load_target_modules},
};
use move_model::ty::{PrimitiveType, Type};

#[derive(Debug, Default, Clone, Copy)]
pub struct BoolJudgementOracle {
    pub otw: bool,
}

impl Named for BoolJudgementOracle {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("BoolJudgementOracle")
    }
}

impl<S> Oracle<S> for BoolJudgementOracle
where
    S: HasFuzzMetadata,
{
    fn event(
        &mut self,
        event: &TraceEvent,
        _state: &mut S,
        stack: Option<&move_vm_stack::Stack>,
        symbol_stack: &Vec<SymbolValue>,
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
                let loss = match instruction {
                    Bytecode::Eq
                    | Bytecode::Neq
                    | Bytecode::Lt
                    | Bytecode::Le
                    | Bytecode::Gt
                    | Bytecode::Ge => {
                        let stack_len = symbol_stack.len();
                        if stack_len < 2 {
                            return Ok((ExitKind::Ok, Vec::new()));
                        }
                        let rhs = &symbol_stack[stack_len - 1];
                        let lhs = &symbol_stack[stack_len - 2];
                        if matches!(instruction, Bytecode::Eq | Bytecode::Neq)
                            && let Some(v) = stack.and_then(|s| s.value.last())
                        {
                            // == true or != true are unnecessary if one side is a bool constant
                            if matches!(v.0, ValueImpl::Bool(_)) {
                                if matches!(lhs, SymbolValue::Value(l) if int_has_variable(l) == Some(false))
                                    || matches!(rhs, SymbolValue::Value(r) if int_has_variable(r) == Some(false))
                                {
                                    let message = format_vulnerability_info(
                                        "Unnecessary bool judgement (bool constant)",
                                        current_function,
                                        Some(*pc),
                                    );
                                    return Ok((ExitKind::Crash, vec![message]));
                                }
                            }
                        }
                        match (lhs, rhs) {
                            (SymbolValue::Value(l), SymbolValue::Value(r)) => {
                                int_has_variable(l) == Some(false)
                                    && int_has_variable(r) == Some(false)
                            }
                            _ => false,
                        }
                    }
                    _ => false,
                };
                if loss {
                    let message = format_vulnerability_info(
                        "Unnecessary bool judgement (two constants)",
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
                if detect_bool_judgement(function) {
                    crashed = true;
                    reports.push(format!(
                        "[STATIC] Unnecessary bool judgement in {}::{}",
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

fn int_has_variable(expr: &Int) -> Option<bool> {
    let mut stack = vec![Dynamic::from(expr.clone())];
    let mut count = 0;
    while let Some(node) = stack.pop() {
        count += 1;
        if count > 10000 {
            return None;
        }
        if node.is_const() {
            if let Ok(decl) = node.safe_decl() {
                if decl.kind() == DeclKind::UNINTERPRETED {
                    return Some(true);
                }
            }
        }
        stack.extend(node.children());
    }
    Some(false)
}

fn detect_bool_judgement(function: &FunctionInfo) -> bool {
    let local_types = &function.local_types;
    for (offset, instr) in function.code.iter().enumerate() {
        match instr {
            SLBytecode::Call(_, _, Operation::Eq, srcs, _)
            | SLBytecode::Call(_, _, Operation::Neq, srcs, _) => {
                if srcs.len() != 2 {
                    continue;
                }
                let left = match get_def_bytecode(function, srcs[0], offset) {
                    Some(code) => code,
                    None => continue,
                };
                let right = match get_def_bytecode(function, srcs[1], offset) {
                    Some(code) => code,
                    None => continue,
                };

                if (is_ld_bool(left) && ret_is_bool(right, local_types))
                    || (is_ld_bool(right) && ret_is_bool(left, local_types))
                {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

fn is_ld_bool(bytecode: &SLBytecode) -> bool {
    matches!(
        bytecode,
        SLBytecode::Load(_, _, Constant::Bool(true))
            | SLBytecode::Load(_, _, Constant::Bool(false))
    )
}

fn ret_is_bool(bytecode: &SLBytecode, local_types: &[Type]) -> bool {
    match bytecode {
        SLBytecode::Call(_, dsts, _, _, _)
            if dsts.get(0).and_then(|idx| local_types.get(*idx))
                == Some(&Type::Primitive(PrimitiveType::Bool)) =>
        {
            true
        }
        SLBytecode::Assign(_, dst, _, _)
            if local_types.get(*dst) == Some(&Type::Primitive(PrimitiveType::Bool)) =>
        {
            true
        }
        SLBytecode::Load(_, dst, _)
            if local_types.get(*dst) == Some(&Type::Primitive(PrimitiveType::Bool)) =>
        {
            true
        }
        _ => false,
    }
}
