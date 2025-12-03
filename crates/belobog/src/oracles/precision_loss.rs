use belobog_fork::ObjectTypesStore;
use belobog_types::error::BelobogError;
use libafl::executors::ExitKind;
use libafl_bolts::Named;
use log::{debug, info};
use move_binary_format::file_format::Bytecode;
use move_core_types::language_storage::ModuleId;
use move_model::symbol::SymbolPool;
use move_stackless_bytecode::stackless_bytecode::{Bytecode as SLBytecode, Operation};
use move_trace_format::format::TraceEvent;
use sui_types::storage::BackingStore;
use z3::ast::{Ast, Dynamic};

use crate::{
    concolic::SymbolValue, generate_bytecode::FunctionInfo, meta::HasFuzzMetadata,
    state::HasExtraState,
};

use super::{
    Oracle,
    common::{format_vulnerability_info, get_def_bytecode, load_target_modules},
};

#[derive(Debug, Default, Clone, Copy)]
pub struct PrecisionLossOracle {
    pub otw: bool,
}

impl Named for PrecisionLossOracle {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("PrecisionLossOracle")
    }
}

impl<S> Oracle<S> for PrecisionLossOracle
where
    S: HasFuzzMetadata + HasExtraState,
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
            TraceEvent::BeforeInstruction {
                type_parameters: _,
                pc,
                gas_left: _,
                instruction,
                extra: _,
            } => {
                let loss = match instruction {
                    Bytecode::Mul => {
                        let stack_len = symbol_stack.len();
                        if stack_len < 2 {
                            return Ok((ExitKind::Ok, Vec::new()));
                        }
                        let rhs = &symbol_stack[stack_len - 1];
                        let lhs = &symbol_stack[stack_len - 2];
                        match (lhs, rhs) {
                            (SymbolValue::Value(l), _) => {
                                debug!("Checking LHS for division...");
                                let result = contains_division(l);
                                debug!("LHS contains division: {}", result);
                                result
                            }
                            (_, SymbolValue::Value(r)) => {
                                debug!("Checking RHS for division...");
                                let result = contains_division(r);
                                debug!("RHS contains division: {}", result);
                                result
                            }
                            _ => false,
                        }
                    }
                    _ => false,
                };
                if loss {
                    let message = format_vulnerability_info(
                        "Precision loss detected",
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
                if detect_precision_loss(function, module.global_env.symbol_pool()) {
                    crashed = true;
                    reports.push(format!(
                        "[STATIC] Precision loss detected in {}::{}",
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

fn contains_division(expr: &z3::ast::Int) -> bool {
    let mut stack = vec![Dynamic::from(expr.clone())];
    let mut count = 0;
    while let Some(node) = stack.pop() {
        count += 1;
        if count > 10000 {
            break;
        }
        if let Ok(decl) = node.safe_decl() {
            match decl.kind() {
                z3::DeclKind::DIV | z3::DeclKind::IDIV => return true,
                _ => {}
            }
        }
        stack.extend(node.children());
    }
    false
}

fn detect_precision_loss(function: &FunctionInfo, symbol_pool: &SymbolPool) -> bool {
    for (offset, instr) in function.code.iter().enumerate() {
        if let SLBytecode::Call(_, _, Operation::Mul, srcs, _) = instr {
            if srcs.len() != 2 {
                continue;
            }
            let op1 = match get_def_bytecode(function, srcs[0], offset) {
                Some(code) => code,
                None => continue,
            };
            let op2 = match get_def_bytecode(function, srcs[1], offset) {
                Some(code) => code,
                None => continue,
            };
            if is_div(op1) || is_div(op2) || is_sqrt(op1, symbol_pool) || is_sqrt(op2, symbol_pool)
            {
                return true;
            }
        }
    }
    false
}

fn is_div(bytecode: &SLBytecode) -> bool {
    matches!(bytecode, SLBytecode::Call(_, _, Operation::Div, _, _))
}

fn is_sqrt(bytecode: &SLBytecode, symbol_pool: &SymbolPool) -> bool {
    matches!(bytecode, SLBytecode::Call(_, _, Operation::Function(_, fid, _), _, _) if symbol_pool.string(fid.symbol()).as_str() == "sqrt")
}
