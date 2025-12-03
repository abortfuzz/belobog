use std::{collections::BTreeMap, str::FromStr, sync::mpsc, thread, time::Duration};

use log::{debug, info, trace, warn};
use move_core_types::u256::U256;
use move_vm_types::values::Value;
use sui_json_rpc_types::SuiMoveNormalizedType;
use z3::{
    Config, Solver,
    ast::{Ast, Bool, Int},
    with_z3_config,
};

use crate::{
    executor::Log,
    meta::{FunctionIdent, FuzzMetadata},
};

const SOLVER_TIMEOUT_MS: u64 = 500;

enum SolveOutcome {
    Sat(BTreeMap<usize, String>),
    Unsat,
    Unknown,
    Timeout,
}

fn run_solver_worker(solver_script: String, arg_names: BTreeMap<usize, String>) -> SolveOutcome {
    let mut cfg = Config::new();
    cfg.set_timeout_msec(SOLVER_TIMEOUT_MS);
    with_z3_config(&cfg, move || {
        let solver = Solver::new();
        solver.from_string(solver_script.as_str());

        let translated_args = arg_names
            .iter()
            .map(|(idx, name)| (*idx, Int::new_const(name.as_str())))
            .collect::<BTreeMap<_, _>>();

        match solver.check() {
            z3::SatResult::Sat => {
                if let Some(model) = solver.get_model() {
                    debug!("Satisfiable with model:");
                    let mut assignments = BTreeMap::new();
                    for (i, int) in translated_args.iter() {
                        if let Some(val) = model.eval(int, true) {
                            debug!("  arg[{}] = {}", i, val);
                            assignments.insert(*i, val.to_string());
                        } else {
                            info!("  arg[{}] = <could not evaluate>", i);
                        }
                    }
                    SolveOutcome::Sat(assignments)
                } else {
                    info!("Satisfiable but no model found");
                    SolveOutcome::Unknown
                }
            }
            z3::SatResult::Unsat => {
                info!("Unsatisfiable");
                SolveOutcome::Unsat
            }
            z3::SatResult::Unknown => {
                info!("Solver returned unknown");
                SolveOutcome::Unknown
            }
        }
    })
}

pub fn solve(
    target_function: FunctionIdent,
    args: &BTreeMap<usize, Int>,
    constraints: &Vec<Bool>,
    meta: &FuzzMetadata,
) -> Option<BTreeMap<usize, Value>> {
    if constraints.is_empty() {
        trace!(
            "No constraints to solve. target function: {:?}",
            target_function
        );
        return None;
    }
    let solver = Solver::new();
    for c in constraints {
        solver.assert(c);
    }
    let function = meta
        .get_function(&target_function.0, &target_function.1, &target_function.2)
        .unwrap();
    let function_params = function.parameters.clone();
    let mut arg_names = BTreeMap::new();
    for (i, int) in args.iter() {
        solver.assert(int.ge(Int::from_u64(0)));
        match function_params.get(*i).unwrap_or_else(|| {
            panic!(
                "Parameter index {} out of bounds for function {:?}",
                i, target_function
            )
        }) {
            SuiMoveNormalizedType::Bool => {
                solver.assert(int.le(Int::from_u64(1)));
            }
            SuiMoveNormalizedType::U8 => {
                solver.assert(int.le(Int::from_u64(u8::MAX as u64)));
            }
            SuiMoveNormalizedType::U16 => {
                solver.assert(int.le(Int::from_u64(u16::MAX as u64)));
            }
            SuiMoveNormalizedType::U32 => {
                solver.assert(int.le(Int::from_u64(u32::MAX as u64)));
            }
            SuiMoveNormalizedType::U64 => {
                solver.assert(int.le(Int::from_u64(u64::MAX)));
            }
            SuiMoveNormalizedType::U128 => {
                let max = "340282366920938463463374607431768211455"; // 2^128 - 1
                solver.assert(int.le(Int::from_str(max).unwrap()));
            }
            SuiMoveNormalizedType::U256 => {
                let max = "115792089237316195423570985008687907853269984665640564039457584007913129639935"; // 2^256 - 1
                solver.assert(int.le(Int::from_str(max).unwrap()));
            }
            _ => {
                panic!(
                    "Unsupported argument type for symbolic execution, function: {:?}, param idx: {}, args: {:?}",
                    target_function, i, args
                );
            }
        }
        arg_names.insert(*i, int.decl().name());
    }
    let solver_script = solver.to_string();
    debug!("Solver script:\n{}", solver_script);

    let (tx, rx) = mpsc::channel();
    let solver_script_worker = solver_script;
    let arg_names_worker = arg_names;
    let worker_handle = thread::spawn(move || {
        let outcome = run_solver_worker(solver_script_worker, arg_names_worker);
        let _ = tx.send(outcome);
    });

    let solver_outcome = match rx.recv_timeout(Duration::from_millis(SOLVER_TIMEOUT_MS + 50)) {
        Ok(outcome) => outcome,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            debug!("Solver worker timed out after {} ms", SOLVER_TIMEOUT_MS);
            SolveOutcome::Timeout
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            warn!("Solver worker disconnected before sending a result");
            SolveOutcome::Unknown
        }
    };

    if !matches!(solver_outcome, SolveOutcome::Timeout) {
        if let Err(err) = worker_handle.join() {
            warn!("Solver worker panicked: {:?}", err);
            return None;
        }
    }

    match solver_outcome {
        SolveOutcome::Sat(assignments) => {
            let mut result = BTreeMap::new();
            for (idx, raw_value) in assignments {
                let ty = function_params.get(idx).unwrap_or_else(|| {
                    panic!(
                        "Parameter index {} out of bounds for function {:?}",
                        idx, target_function
                    )
                });
                match value_from_assignment(ty, &raw_value) {
                    Some(value) => {
                        debug!("arg[{}] = {}", idx, raw_value);
                        result.insert(idx, value);
                    }
                    None => {
                        warn!(
                            "Failed to parse model value '{}' for arg {} ({:?})",
                            raw_value, idx, ty
                        );
                    }
                }
            }
            Some(result)
        }
        SolveOutcome::Unsat => None,
        SolveOutcome::Unknown => None,
        SolveOutcome::Timeout => {
            debug!("Solver timed out");
            None
        }
    }
}

fn value_from_assignment(ty: &SuiMoveNormalizedType, raw_value: &str) -> Option<Value> {
    match ty {
        SuiMoveNormalizedType::Bool => {
            let v = raw_value.trim();
            match v {
                "true" | "1" => Some(Value::bool(true)),
                "false" | "0" => Some(Value::bool(false)),
                _ => {
                    let normalized = normalize_model_value(v);
                    match normalized.as_str() {
                        "true" | "1" => Some(Value::bool(true)),
                        "false" | "0" => Some(Value::bool(false)),
                        _ => None,
                    }
                }
            }
        }
        SuiMoveNormalizedType::U8 => {
            let num = parse_numeric_value(raw_value)?.parse::<u128>().ok()?;
            if num > u8::MAX as u128 {
                None
            } else {
                Some(Value::u8(num as u8))
            }
        }
        SuiMoveNormalizedType::U16 => {
            let num = parse_numeric_value(raw_value)?.parse::<u128>().ok()?;
            if num > u16::MAX as u128 {
                None
            } else {
                Some(Value::u16(num as u16))
            }
        }
        SuiMoveNormalizedType::U32 => {
            let num = parse_numeric_value(raw_value)?.parse::<u128>().ok()?;
            if num > u32::MAX as u128 {
                None
            } else {
                Some(Value::u32(num as u32))
            }
        }
        SuiMoveNormalizedType::U64 => {
            let num = parse_numeric_value(raw_value)?.parse::<u64>().ok()?;
            Some(Value::u64(num))
        }
        SuiMoveNormalizedType::U128 => {
            let num = parse_numeric_value(raw_value)?.parse::<u128>().ok()?;
            Some(Value::u128(num))
        }
        SuiMoveNormalizedType::U256 => {
            let num = parse_numeric_value(raw_value)?;
            let value = U256::from_str(&num).ok()?;
            Some(Value::u256(value))
        }
        _ => {
            panic!("Unsupported argument type for symbolic execution {:?}", ty);
        }
    }
}

fn normalize_model_value(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with('(') && trimmed.ends_with(')') && trimmed.len() >= 2 {
        trimmed[1..trimmed.len() - 1].replace(' ', "")
    } else {
        trimmed.to_string()
    }
}

fn parse_numeric_value(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.starts_with("(_ bv") && trimmed.ends_with(')') {
        let inner = trimmed.trim_matches(|c| c == '(' || c == ')');
        let mut parts = inner.split_whitespace();
        let Some(first) = parts.next() else {
            return None;
        };
        let Some(second) = parts.next() else {
            return None;
        };
        if first == "_" && second.starts_with("bv") {
            return Some(second[2..].to_string());
        }
    } else if trimmed.starts_with("#x") {
        let value = U256::from_str_radix(&trimmed[2..], 16).ok()?;
        return Some(value.to_string());
    } else if trimmed.starts_with("#b") {
        let value = U256::from_str_radix(&trimmed[2..], 2).ok()?;
        return Some(value.to_string());
    }
    Some(normalize_model_value(trimmed))
}
