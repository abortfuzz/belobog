use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    io::Write,
    path::Path,
};

use libafl_bolts::impl_serdeany;
use log::debug;
use move_binary_format::{CompiledModule, file_format::Bytecode};
use move_core_types::{account_address::AccountAddress, language_storage::ModuleId};
use serde::{Deserialize, Serialize};
use sui_json_rpc_types::SuiMoveNormalizedModule;
use sui_types::{
    base_types::ObjectID,
    error::{ExecutionError, ExecutionErrorKind},
    execution_status::ExecutionStatus,
    object::Object,
    transaction::{Command, ProgrammableTransaction},
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FunctionCoverage {
    pub bytecode: Option<Vec<Bytecode>>,
    pub coverage: BTreeSet<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ModuleCoverage {
    pub functions_coverage: BTreeMap<String, FunctionCoverage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct APICoverage {
    pub functions: BTreeSet<String>,
    pub all_functions: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EvaluationMetrics {
    pub instructions_sum: u64,
    pub modules_coverage: BTreeMap<String, ModuleCoverage>,
    pub api_coverage: APICoverage,
    pub seeds_counts: u64,
    pub invalid_seeds: u64,
    pub abort_seeds: u64,
    pub cycle: u64,
    pub timestamp: u64,
}

impl EvaluationMetrics {
    pub fn on_pc(&mut self, module_id: &ModuleId, function_name: &str, pc: u16) {
        self.instructions_sum += 1;
        let module = self
            .modules_coverage
            .entry(module_id.to_canonical_string(true))
            .or_default();
        let func = module
            .functions_coverage
            .entry(function_name.to_string())
            .or_default();

        func.coverage.insert(pc);
    }

    pub fn dump(&self, path: &Path) {
        let fp = std::fs::File::create(path).expect("open metrics file");
        serde_json::to_writer(fp, self).expect("dump metrics");
    }

    pub fn append_jsonl(&self, path: &Path) {
        let mut fp = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open metrics file");
        serde_json::to_writer(&mut fp, self).expect("dump metrics");
        writeln!(fp).expect("write newline");
    }

    pub fn load_target_package(&mut self, object: Object) {
        if let Some(package) = object.data.try_as_package() {
            for it in package.serialized_module_map().values() {
                let compiled_module = CompiledModule::deserialize_with_defaults(it).unwrap();
                self.load_target_module(&compiled_module);
            }
        } else {
            log::warn!("{} is not package?!", object.id());
        }
    }

    pub fn load_target_module(&mut self, module: &CompiledModule) {
        for def in module.function_defs() {
            let function = module.function_handle_at(def.function);
            let fname = module.identifier_at(function.name);
            let module_coverage = self
                .modules_coverage
                .entry(module.self_id().to_canonical_string(true))
                .or_default();
            self.api_coverage
                .all_functions
                .insert(format!("{}::{}", module.self_id(), fname));
            if let Some(code) = &def.code {
                module_coverage
                    .functions_coverage
                    .entry(fname.to_string())
                    .or_default()
                    .bytecode = Some(code.code.clone());
                log::info!(
                    "Target function {}:{} loaded for evalution metrics",
                    module.self_id(),
                    fname
                );
            } else {
                log::warn!(
                    "Function {}:{} not containing code?!",
                    module.self_id(),
                    fname
                );
            }
        }
    }

    pub fn on_executed(
        &mut self,
        status: &Result<(), ExecutionError>,
        ptb: &ProgrammableTransaction,
    ) {
        self.seeds_counts += 1;
        match status {
            Ok(_) => {
                for cmd in ptb.commands.iter() {
                    if let Command::MoveCall(movecall) = cmd {
                        self.api_coverage.functions.insert(format!(
                            "{}::{}::{}",
                            movecall.package, movecall.module, movecall.function
                        ));
                    }
                }
                return;
            }
            Err(e) => {
                match e.kind() {
                    ExecutionErrorKind::UnusedValueWithoutDrop {
                        result_idx,
                        secondary_idx,
                    } => {
                        debug!(
                            "unused value without drop at result index {}, secondary index {}. ptb: {}",
                            result_idx, secondary_idx, ptb
                        );
                        self.invalid_seeds += 1;
                    }
                    ExecutionErrorKind::TypeArgumentError { argument_idx, kind } => {
                        debug!(
                            "type argument error occurred at argument index {}. ptb: {}",
                            argument_idx, ptb
                        );
                        self.invalid_seeds += 1;
                    }
                    ExecutionErrorKind::CommandArgumentError { arg_idx, kind } => {
                        debug!(
                            "command argument error occurred at argument index {}. ptb: {}",
                            arg_idx, ptb
                        );
                        self.invalid_seeds += 1;
                    }
                    ExecutionErrorKind::MoveAbort(_, _)
                    | ExecutionErrorKind::MovePrimitiveRuntimeError(_) => {
                        // it's okay for runtime errors...
                        self.abort_seeds += 1;
                    }
                    ExecutionErrorKind::InsufficientGas => {}
                    _ => {
                        log::warn!("Unexpected error kind: {:?}, ptb: {}", e, ptb);
                    }
                }
            }
        }
    }
}
