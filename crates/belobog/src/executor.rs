use belobog_fork::snapshot::find_reference_gas_price;
use belobog_fork::{
    ObjectStoreCommit, ObjectTypesStore, cache::CachedStore,
    snapshot::find_first_checkpoint_in_epoch,
};
use belobog_types::error::BelobogError;
use color_eyre::eyre::{OptionExt, eyre};
use itertools::Itertools;
use libafl::{
    HasMetadata,
    executors::{Executor, ExitKind, HasObservers},
    observers::{MapObserver, ObserversTuple, StdMapObserver},
    state::{HasExecutions, HasRand},
};
use libafl_bolts::tuples::{Handle, RefIndexable};
use libafl_bolts::{impl_serdeany, tuples::MatchNameRef};
use log::{debug, info, trace, warn};
use move_binary_format::CompiledModule;
use move_binary_format::file_format::Bytecode;
use move_core_types::account_address::AccountAddress;
use move_core_types::language_storage::ModuleId;
use move_trace_format::{
    format::{Effect, MoveTraceBuilder, TraceEvent},
    interface::Tracer,
};
use move_vm_stack::Stack;
use move_vm_types::values::IntegerValue;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::Display,
    io::Write,
    marker::PhantomData,
    ops::AddAssign,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use sui_adapter_v2::adapter::substitute_package_id;
use sui_sdk::{SuiClient, types::gas::SuiGasStatus};
use sui_types::gas::SuiGasStatusAPI;
use sui_types::gas_model::tables::GasStatus;
use sui_types::gas_model::units_types::CostTable;
use sui_types::inner_temporary_store::ObjectMap;
use sui_types::move_package::MovePackage;
use sui_types::object::MoveObject;
use sui_types::storage::BackingStore;
use sui_types::{
    base_types::{ObjectID, SequenceNumber, SuiAddress},
    committee::ProtocolVersion,
    effects::{ObjectRemoveKind, TransactionEffects, TransactionEffectsAPI, TransactionEvents},
    error::{ExecutionError, ExecutionErrorKind},
    execution_status::{CommandIndex, ExecutionFailureStatus, ExecutionStatus},
    inner_temporary_store::{InnerTemporaryStore, WrittenObjects},
    metrics::LimitsMetrics,
    object::{Object, Owner},
    storage::{BackingPackageStore, ObjectStore as _, WriteKind},
    supported_protocol_versions::{Chain, ProtocolConfig},
    transaction::{
        Argument, CallArg, CheckedInputObjects, Command, InputObjectKind, ObjectReadResult,
        ObjectReadResultKind, ProgrammableTransaction, TransactionData, TransactionDataAPI,
        TransactionKind,
    },
};
use z3::ast::Bool;

use crate::input::SuiFuzzInput;
use crate::utils::change_type_address;
use crate::{
    build::{Magic, PackageMetadata},
    concolic::ConcolicState,
    flash::FlashWrapper,
    input::SuiInput,
    meta::{FuzzMetadata, HasFuzzMetadata, MutatorKind},
    r#move::may_be_oracle,
    oracles::OracleTuple,
    state::HasExtraState,
    utils::{pprint_event, pprint_object},
};

pub const CODE_OBSERVER_NAME: &str = "code_observer";

pub fn latest_protocol_config() -> ProtocolConfig {
    ProtocolConfig::get_for_version(ProtocolVersion::max(), Chain::Mainnet)
}

pub fn latest_executor()
-> Result<Arc<dyn sui_execution::executor::Executor + Send + Sync>, BelobogError> {
    Ok(sui_execution::executor(&latest_protocol_config(), false)?)
}

#[derive(Debug, Clone, Default)]
pub struct SuiLogTracer {
    pub adddress_filters: HashSet<ObjectID>,
    pub module_filters: HashSet<String>,
    pub functions_filters: HashSet<String>,
}

impl SuiLogTracer {
    pub fn may_log<I: SuiInput>(
        &self,
        input: &I,
        outcome: &ExecutionOutcome,
        o: &ExecutionExtraOutcome,
    ) -> Option<String> {
        for cmd in input.ptb().commands.iter() {
            if let Command::MoveCall(mc) = cmd
                && (self.adddress_filters.contains(&mc.package)
                    || self.module_filters.contains(&mc.module)
                    || self.functions_filters.contains(&mc.function))
            {
                // let cmps = o.cmps.iter().find(|(_, m)| m.contains_key(&mc.module)).and_then(|(_, m)| m.get(&mc.module)).and_then(|m| m.get(&mc.function));
                let cmps = "";
                return Some(format!(
                    "Traced Input:\n{}\nOutcome:\n{}\ncmps:\n{:?}",
                    input, outcome, cmps
                ));
            }
        }
        None
    }

    pub fn init() -> Result<Option<Self>, BelobogError> {
        match std::env::var("SUI_TRACE") {
            Ok(v) => Ok(Some(Self::from_env_str(&v)?)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(e) => Err(eyre!("var error for SUI_TRACE {}", e).into()),
        }
    }

    pub fn add_filter(&mut self, s: &str) -> Result<(), BelobogError> {
        let vs = s.split(":").collect_vec();
        let (address, module, function) = if vs.len() == 3 {
            (
                Some(ObjectID::from_str(vs[0])?),
                Some(vs[1].to_string()),
                Some(vs[2].to_string()),
            )
        } else if vs.len() == 2 {
            (
                Some(ObjectID::from_str(vs[0])?),
                Some(vs[1].to_string()),
                None,
            )
        } else if vs.len() == 1 {
            (None, None, Some(vs[0].to_string()))
        } else {
            return Err(eyre!("expected [module_id]:[module_name]:[function_name] or [module_id]:[module_name] or [function_name]").into());
        };

        if let Some(address) = address {
            self.adddress_filters.insert(address);
        }
        if let Some(module) = module {
            self.module_filters.insert(module);
        }
        if let Some(function) = function {
            self.functions_filters.insert(function);
        }

        Ok(())
    }
    pub fn from_env_str(sui_trace: &str) -> Result<Self, BelobogError> {
        let mut ret = Self::default();
        for ts in sui_trace.split(",") {
            ret.add_filter(ts)?;
        }
        Ok(ret)
    }
}

#[derive(Default, Debug, Clone)]
pub struct CoverageTracer {
    pub packages: Vec<u64>,
    pub had_br: bool,
    pub prev: u64,
}

impl CoverageTracer {
    fn hash_package(package: &[u8]) -> u64 {
        libafl_bolts::hash_std(package)
    }

    fn hit_cov(&mut self, pc: u16, ob: &mut [u8]) {
        if !ob.is_empty() {
            let pkg = self.packages.last().expect("stack empty?!");
            trace!("Hit a coverage at {} within package hash {}", pc, pkg);
            let pc = pc as u64;
            let pc = (pc >> 4) ^ (pc << 8) ^ *pkg;
            let len = ob.len() as u64;
            let hit = ((self.prev ^ pc) % len) as usize;
            self.prev = pc;

            ob[hit] = ob[hit].saturating_add(1);
        }
    }

    pub fn call_package(&mut self, package: String) {
        let pkg = Self::hash_package(package.as_bytes());
        trace!("Calling package {} to {}", package, pkg);
        self.packages.push(pkg);
        self.had_br = true;
    }

    pub fn call_end_package(&mut self) {
        let pkg = self.packages.pop();
        trace!("Leaving {:?}", &pkg);
        self.had_br = true;
    }

    pub fn will_branch(&mut self) {
        self.had_br = true;
    }

    pub fn may_do_coverage(&mut self, pc: u16, ob: &mut [u8]) {
        if self.had_br {
            self.had_br = false;
            self.hit_cov(pc, ob);
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy)]
pub enum CmpOp {
    LT,
    LE,
    GT,
    GE,
    NEQ,
    EQ,
}

impl Display for CmpOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LE => f.write_str("<="),
            Self::LT => f.write_str("<"),
            Self::GE => f.write_str(">="),
            Self::GT => f.write_str(">"),
            Self::NEQ => f.write_str("!="),
            Self::EQ => f.write_str("=="),
        }
    }
}

impl TryFrom<&Bytecode> for CmpOp {
    type Error = BelobogError;
    fn try_from(value: &Bytecode) -> Result<Self, Self::Error> {
        match value {
            Bytecode::Le => Ok(Self::LE),
            Bytecode::Lt => Ok(Self::LT),
            Bytecode::Ge => Ok(Self::GE),
            Bytecode::Gt => Ok(Self::GT),
            Bytecode::Neq => Ok(Self::NEQ),
            Bytecode::Eq => Ok(Self::EQ),
            _ => Err(eyre!("{:?} can not convert into Cmpop", value).into()),
        }
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct CmpLog {
    pub lhs: Magic,
    pub rhs: Magic,
    pub op: CmpOp,
    pub constraint: Option<Bool>,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct ShlLog {
    pub lhs: Magic,
    pub rhs: Magic,
    pub constraint: Option<Bool>,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct CastLog {
    pub lhs: Magic,
    pub constraint: Option<Bool>,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum Log {
    CmpLog(CmpLog),
    ShlLog(ShlLog),
    CastLog(CastLog),
}

impl Display for CmpLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}{}{}", &self.lhs, &self.op, &self.rhs))
    }
}

#[derive(Debug)]
pub struct TraceOutcome {
    pub pending_error: Option<BelobogError>,
    pub cmps: BTreeMap<String, BTreeMap<String, BTreeMap<String, Vec<Log>>>>,
    pub exit_kind_verdict: ExitKind,
    pub vulnerabilities: BTreeSet<String>,
}

impl Default for TraceOutcome {
    fn default() -> Self {
        Self {
            pending_error: None,
            cmps: BTreeMap::new(),
            exit_kind_verdict: ExitKind::Ok,
            vulnerabilities: BTreeSet::new(),
        }
    }
}

struct FullTracer<'a, 'b, 's, S, RT> {
    pub current_functions: Vec<(ModuleId, String)>,
    pub code_observer: &'a mut StdMapObserver<'b, u8, false>,
    pub coverage: CoverageTracer,
    pub state: &'s mut S,
    pub oracles: &'a mut RT,
    pub outcome: &'s mut TraceOutcome,
    pub solver: &'s mut ConcolicState,
}

impl<'a, 'b, 's, S, RT> FullTracer<'a, 'b, 's, S, RT>
where
    RT: OracleTuple<S>,
    S: HasFuzzMetadata,
{
    fn bin_ops(stack: Option<&Stack>) -> Result<(Magic, Magic), BelobogError> {
        if let Some(stack) = stack {
            let stack_len = stack.value.len();
            let rhs = stack
                .value
                .get(stack_len - 1)
                .ok_or_eyre(eyre!("stack less than 2?!"))?
                .copy_value()?
                .value_as::<IntegerValue>()?
                .into();
            let lhs = stack
                .value
                .get(stack_len - 2)
                .ok_or_eyre(eyre!("stack less than 2?!"))?
                .copy_value()?
                .value_as::<IntegerValue>()?
                .into();
            Ok((lhs, rhs))
        } else {
            Err(eyre!("we need two values on top of stack but get none...").into())
        }
    }

    fn notify_event(
        &mut self,
        event: &TraceEvent,
        _writer: move_trace_format::interface::Writer<'_>,
        stack: Option<&Stack>,
    ) -> Result<ExitKind, BelobogError> {
        let (kind, oracle_vulns) = self.oracles.event_all(
            event,
            self.state,
            stack,
            &self.solver.stack,
            self.current_functions.last(),
        )?;
        for info in oracle_vulns {
            self.outcome.vulnerabilities.insert(info);
        }
        let constraint = self.solver.notify_event(event, stack);
        match event {
            TraceEvent::OpenFrame { frame, gas_left: _ } => {
                let package = format!(
                    "{}:{}:{}",
                    frame.module.address().to_canonical_string(true),
                    frame.module.name(),
                    &frame.function_name
                );
                self.current_functions
                    .push((frame.module.clone(), frame.function_name.clone()));
                self.coverage.call_package(package);
            }
            TraceEvent::CloseFrame {
                frame_id: _,
                return_: _,
                gas_left: _,
            } => {
                self.coverage.call_end_package();
                self.current_functions.pop();
            }
            TraceEvent::BeforeInstruction {
                type_parameters: _,
                pc,
                gas_left: _,
                instruction,
                extra: _,
            } => {
                if let Some(metrics) = self.state.eval_metrics_mut() {
                    if let Some(current) = self.current_functions.last() {
                        metrics.on_pc(&current.0, &current.1, *pc);
                    } else {
                        warn!("no current function when before instruction at {}", pc);
                    }
                }
                self.coverage.may_do_coverage(*pc, self.code_observer);
                match instruction {
                    Bytecode::BrFalse(_)
                    | Bytecode::BrTrue(_)
                    | Bytecode::Branch(_)
                    | Bytecode::VariantSwitch(_) => {
                        self.coverage.will_branch();
                    }
                    Bytecode::Lt
                    | Bytecode::Le
                    | Bytecode::Ge
                    | Bytecode::Gt
                    | Bytecode::Neq
                    | Bytecode::Eq => match Self::bin_ops(stack) {
                        Ok((lhs, rhs)) => {
                            if let Some(current_function) = self.current_functions.first() {
                                trace!(
                                    "Tracking cmplog at {}:{}:{}",
                                    current_function.0.address(),
                                    current_function.0.name(),
                                    current_function.1
                                );
                                let op = CmpOp::try_from(instruction)?;
                                self.outcome
                                    .cmps
                                    .entry(current_function.0.address().to_canonical_string(true))
                                    .or_default()
                                    .entry(current_function.0.name().to_string())
                                    .or_default()
                                    .entry(current_function.1.to_string())
                                    .or_default()
                                    .push(Log::CmpLog(CmpLog {
                                        lhs,
                                        rhs,
                                        op,
                                        constraint,
                                    }));
                            } else {
                                warn!("Fail to track cmplog because of no current function")
                            }
                        }
                        Err(e) => {
                            if !matches!(instruction, Bytecode::Eq)
                                && !matches!(instruction, Bytecode::Neq)
                            {
                                warn!("Can not track cmplog due to {}", e);
                            }
                        }
                    },
                    Bytecode::Shl => match Self::bin_ops(stack) {
                        Ok((lhs, rhs)) => {
                            if let Some(current_function) = self.current_functions.first() {
                                trace!(
                                    "Tracking shllog at {}:{}:{}",
                                    current_function.0.address(),
                                    current_function.0.name(),
                                    current_function.1
                                );
                                self.outcome
                                    .cmps
                                    .entry(current_function.0.address().to_canonical_string(true))
                                    .or_default()
                                    .entry(current_function.0.name().to_string())
                                    .or_default()
                                    .entry(current_function.1.to_string())
                                    .or_default()
                                    .push(Log::ShlLog(ShlLog {
                                        lhs,
                                        rhs,
                                        constraint,
                                    }));
                            } else {
                                warn!("Fail to track cmplog because of no current function")
                            }
                        }
                        Err(e) => {
                            if !matches!(instruction, Bytecode::Eq)
                                && !matches!(instruction, Bytecode::Neq)
                            {
                                warn!("Can not track cmplog due to {}", e);
                            }
                        }
                    },
                    Bytecode::CastU8
                    | Bytecode::CastU16
                    | Bytecode::CastU32
                    | Bytecode::CastU64
                    | Bytecode::CastU128 => {
                        if let Some(Some(lhs)) = stack.map(|s| s.value.last()) {
                            let lhs: Magic = lhs.copy_value()?.value_as::<IntegerValue>()?.into();
                            if let Some(current_function) = self.current_functions.first() {
                                trace!(
                                    "Tracking castlog at {}:{}:{}",
                                    current_function.0.address(),
                                    current_function.0.name(),
                                    current_function.1
                                );
                                self.outcome
                                    .cmps
                                    .entry(current_function.0.address().to_canonical_string(true))
                                    .or_default()
                                    .entry(current_function.0.name().to_string())
                                    .or_default()
                                    .entry(current_function.1.to_string())
                                    .or_default()
                                    .push(Log::CastLog(CastLog { lhs, constraint }));
                            } else {
                                warn!("Fail to track castlog because of no current function")
                            }
                        } else {
                            warn!("Can not track castlog due to stack empty");
                        }
                    }

                    _ => {}
                }
            }
            TraceEvent::Effect(e) => {
                if let Effect::ExecutionError(e) = e.as_ref()
                    && e.contains("!! TRACING ERROR !!")
                {
                    warn!("Receive an error from tracing: {}", e);
                }
            }
            _ => {}
        }

        if self.outcome.exit_kind_verdict == ExitKind::Ok {
            self.outcome.exit_kind_verdict = kind;
        }
        Ok(kind)
    }
}

impl<'a, 'b, 's, S, RT> Tracer for FullTracer<'a, 'b, 's, S, RT>
where
    RT: OracleTuple<S>,
    S: HasFuzzMetadata,
{
    fn notify(
        &mut self,
        event: &TraceEvent,
        writer: move_trace_format::interface::Writer<'_>,
        stack: Option<&Stack>,
    ) {
        trace!(
            "An event occured: {:?}, stack = {:?}",
            event,
            stack.map(|s| &s.value)
        );
        if self.outcome.pending_error.is_some() {
            return;
        }
        if let Err(e) = self.notify_event(event, writer, stack) {
            self.outcome.pending_error = Some(e);
        }
    }
}

#[derive(Clone)]
pub struct ExecutionEnvironment {
    pub protocol_config: ProtocolConfig,
    pub epoch: u64, // inclusive, the epoch of the `checkpoint`
    pub epoch_ms: u64,
    pub checkpoint: u64, // exclusive, fork at `checkpoint` meaning objects at the end of `checkpoint`` - 1
    pub reference_gas_price: u64, // TODO!
    pub metrics: Arc<LimitsMetrics>,
    pub registry: prometheus::Registry,
}

impl ExecutionEnvironment {
    pub fn local_testing() -> Self {
        // TODO: Deploy system packages to avoid fetching them on-chain
        Self::new(latest_protocol_config(), 846, 0, 0, 175841286)
    }

    pub async fn from_rpc(rpc: &SuiClient, checkpoint: u64) -> Result<Self, BelobogError> {
        let checkpoint_data = rpc.read_api().get_checkpoint(checkpoint.into()).await?;
        let epoch = checkpoint_data.epoch;
        let first_checkpoint_in_epoch = find_first_checkpoint_in_epoch(
            rpc,
            checkpoint_data.epoch,
            checkpoint_data.sequence_number,
            Duration::from_secs(30),
        )
        .await?;

        let first_checkpoint_data = rpc
            .read_api()
            .get_checkpoint(first_checkpoint_in_epoch.into())
            .await?;
        let epoch_ms = first_checkpoint_data.timestamp_ms;
        let current_checkpoint = rpc
            .read_api()
            .get_latest_checkpoint_sequence_number()
            .await?;
        let current_epoch = rpc
            .read_api()
            .get_checkpoint(current_checkpoint.into())
            .await?
            .epoch;
        let reverse = current_epoch.abs_diff(epoch) < (current_epoch / 2);
        let rgp = find_reference_gas_price(rpc, epoch, reverse)
            .await?
            .ok_or_eyre(eyre!("no rgp for {}", epoch))?;
        let protocol_version = rpc
            .read_api()
            .get_checkpoint((first_checkpoint_in_epoch - 1).into())
            .await?
            .end_of_epoch_data
            .ok_or_eyre(eyre!(
                "no epoch data from {}",
                first_checkpoint_in_epoch - 1
            ))?
            .next_epoch_protocol_version;
        let protocol_config =
            ProtocolConfig::get_for_version_if_supported(protocol_version, Chain::Mainnet)
                .ok_or_eyre(eyre!("no protocol config"))?;
        Ok(Self::new(protocol_config, epoch, epoch_ms, rgp, checkpoint))
    }
    pub fn new(
        protocol_config: ProtocolConfig,
        epoch: u64,
        epoch_ms: u64,
        rgp: u64,
        checkpoint: u64,
    ) -> Self {
        let registry = prometheus::Registry::new();
        let metrics = Arc::new(LimitsMetrics::new(&registry));
        Self {
            protocol_config,
            epoch,
            epoch_ms,
            checkpoint,
            reference_gas_price: rgp,
            metrics,
            registry,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionErrorMirror {
    pub kind: ExecutionErrorKind,
    pub source: Option<String>,
    pub command: Option<CommandIndex>,
}

impl From<ExecutionError> for ExecutionErrorMirror {
    fn from(value: ExecutionError) -> Self {
        Self {
            kind: value.kind().clone(),
            source: value.source().as_ref().map(|t| t.to_string()),
            command: value.command(),
        }
    }
}

impl Display for ExecutionErrorMirror {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "ExecutionError(kind={}, source={:?}, command={:?})",
            &self.kind, &self.source, &self.command
        ))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOutcome {
    pub events_verdict: ExitKind,
    pub events: TransactionEvents,
    pub written: WrittenObjects,
    pub effects: TransactionEffects,
    #[serde(default)]
    pub input_objects: Vec<InputObjectKind>,
    #[serde(default)]
    pub allowed_success: bool,
    pub error: Option<ExecutionErrorMirror>,
    #[serde(default)]
    pub vulnerabilities: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionExtraOutcome {
    pub cmps: BTreeMap<String, BTreeMap<String, BTreeMap<String, Vec<Log>>>>,
    pub solver: ConcolicState,
    pub stage_idx: Option<usize>,
    pub success: bool,
}

#[derive(Debug, Clone, Default)]
pub struct MutatorOutcome {
    pub cmps: BTreeMap<String, BTreeMap<String, BTreeMap<String, Vec<Log>>>>,
}

#[derive(Debug, Clone)]
pub struct GlobalOutcome {
    pub outcome: ExecutionOutcome,
    pub extra: ExecutionExtraOutcome,
}

impl GlobalOutcome {
    pub fn new(outcome: ExecutionOutcome, extra: ExecutionExtraOutcome) -> Self {
        Self { outcome, extra }
    }
}
impl_serdeany!(ExecutionOutcome);

impl Display for ExecutionOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.format_impl(None))
    }
}

impl ExecutionOutcome {
    pub fn format_impl(&self, meta: Option<&FuzzMetadata>) -> String {
        let exec = format!(
            "Execution Result: {} (Allowed Success: {})",
            match &self.error {
                None => "Success".to_string(),
                Some(e) => format!("Failure with error {}", e),
            },
            self.allowed_success
        );
        let verdict = format!("Verdict: {:?}", &self.events_verdict);
        let written = format!(
            "Store written Objects:\n{}",
            self.written
                .iter()
                .map(|t| format!("\t{}: {}", &t.0, pprint_object(t.1)))
                .join("\n")
        );
        let events = format!(
            "Store Events:\n{}",
            self.events
                .data
                .iter()
                .map(|t| format!("\t{}", pprint_event(t, meta)))
                .join("\n")
        );
        let changed = format!(
            "Changed Objects:\n{}",
            self.effects
                .all_changed_objects()
                .iter()
                .map(|t| format!("\t{}: {:?} (owner {})", &t.0.0, &t.2, &t.1))
                .join("\n")
        );
        let deleted = format!(
            "Deleted Objects:\n{}",
            self.effects
                .all_removed_objects()
                .iter()
                .map(|t| format!(
                    "\t{}: {}",
                    &t.0.0,
                    match t.1 {
                        ObjectRemoveKind::Delete => "delete",
                        ObjectRemoveKind::Wrap => "wrap",
                    }
                ))
                .join("\n")
        );
        let vulnerabilities = if self.vulnerabilities.is_empty() {
            "Vulnerabilities:\n\t<none>".to_string()
        } else {
            format!(
                "Vulnerabilities:\n{}",
                self.vulnerabilities
                    .iter()
                    .map(|entry| format!("\t{}", entry))
                    .join("\n")
            )
        };
        // let cmps = self
        //     .cmps
        //     .iter()
        //     .map(|f| {
        //         let id = f.0;
        //         f.1.iter()
        //             .map(|v| {
        //                 let module = v.0;
        //                 let cmps =
        //                     v.1.iter()
        //                         .map(|v| {
        //                             format!(
        //                                 "\t\t{}: [{}]",
        //                                 v.0,
        //                                 v.1.iter().map(|c| c.to_string()).join(", ")
        //                             )
        //                         })
        //                         .join("\n");
        //                 format!("\t{}:{}:\n{}", id, module, cmps)
        //             })
        //             .join("\n")
        //     })
        //     .join("\n");
        // let cmps = format!("Comparisons:\n{}", cmps);
        let gas = format!("Gas:\n{}", &self.effects.gas_cost_summary());
        [
            exec,
            verdict,
            written,
            events,
            changed,
            deleted,
            vulnerabilities,
            gas,
        ]
        .into_iter()
        .join("\n")
        .to_string()
    }

    pub fn format(&self, meta: &FuzzMetadata) -> String {
        self.format_impl(Some(meta))
    }
}

pub struct SuiFuzzExecutor<T, OT, RT, I, S> {
    pub executor: Arc<dyn sui_execution::Executor + Send + Sync>,
    pub ob: OT,
    pub db: T,
    pub env: ExecutionEnvironment,
    pub attacker: SuiAddress,
    pub admin: SuiAddress,
    pub oracles: RT,
    pub minted_gas: Object,
    pub log_tracer: Option<SuiLogTracer>,
    pub ph: PhantomData<(I, S)>,
    pub disable_oracles: bool,
}

// Safety: Actually only one thread owns us and the pointer is never leaked
// unsafe impl<T, OT, RT, I, S> Send for SuiFuzzExecutor<T, OT, RT, I, S> {}

impl<T, OT, RT, I, S> HasObservers for SuiFuzzExecutor<T, OT, RT, I, S> {
    type Observers = OT;
    fn observers(&self) -> RefIndexable<&Self::Observers, Self::Observers> {
        RefIndexable::from(&self.ob)
    }
    fn observers_mut(&mut self) -> RefIndexable<&mut Self::Observers, Self::Observers> {
        RefIndexable::from(&mut self.ob)
    }
}

impl<T, OT, RT, I, S> SuiFuzzExecutor<T, OT, RT, I, S>
where
    T: BackingStore + ObjectTypesStore + ObjectStoreCommit,
    OT: ObserversTuple<I, S>,
    RT: OracleTuple<S>,
    S: HasRand + HasFuzzMetadata + HasExecutions + HasMetadata + HasExtraState,
    S::Rand: fastcrypto::traits::AllowedRng,
{
    pub fn new(
        env: ExecutionEnvironment,
        ob: OT,
        db: T,
        attacker: SuiAddress,
        admin: SuiAddress,
        oracles: RT,
        state: &mut S,
    ) -> Result<Self, BelobogError> {
        let gas_id = ObjectID::random_from_rng(state.rand_mut());
        let gas_object = db.mint_gas_object(gas_id, attacker.into())?;
        debug!("Minted gas {} for attacker {}", gas_id, &attacker);
        Ok(Self {
            // Only v3 has tracer implementation
            // TODO: Maybe tracer for older evm??
            executor: sui_execution::executor(
                &ProtocolConfig::get_for_version(ProtocolVersion::max(), Chain::Mainnet),
                false,
            )?,
            minted_gas: gas_object,
            ob,
            db,
            env,
            attacker,
            admin,
            oracles,
            log_tracer: SuiLogTracer::init()?,
            ph: PhantomData,
            disable_oracles: false,
        })
    }

    pub fn disable_oracles(&mut self) {
        self.disable_oracles = true;
    }

    pub fn enable_oracles(&mut self) {
        self.disable_oracles = false;
    }

    pub fn execute_fuzz_input(
        &mut self,
        ptb: ProgrammableTransaction,
        flash: Option<FlashWrapper>,
        state: &mut S,
        sender: SuiAddress,
    ) -> Result<(InnerTemporaryStore, ExecutionOutcome, ExecutionExtraOutcome), BelobogError> {
        trace!("Executing fuzz input: {}", ptb);
        let code_ob: &mut StdMapObserver<'_, u8, false> = self
            .ob
            .get_mut(&Handle::new(Cow::Borrowed(CODE_OBSERVER_NAME)))
            .expect("no code ob installed");
        code_ob[0] = 1;
        let mut outcome = TraceOutcome::default();
        let mut concolic_state = ConcolicState::new();
        let tracer = FullTracer {
            current_functions: vec![],
            coverage: CoverageTracer::default(),
            code_observer: code_ob,
            state,
            oracles: &mut self.oracles,
            outcome: &mut outcome,
            solver: &mut concolic_state,
        };

        let input_objects = match ptb.input_objects() {
            Ok(v) => v,
            Err(e) => {
                warn!("Input objects have error: {}", e);
                return Err(BelobogError::Other(e.into()));
            }
        };

        let mut objects = vec![];
        for objref in input_objects {
            match objref {
                InputObjectKind::MovePackage(package) => {
                    let package = self
                        .db
                        .get_package_object(&package)?
                        .ok_or_eyre(eyre!("package {} not found", package))?;
                    objects.push(ObjectReadResult::new(
                        objref,
                        ObjectReadResultKind::Object(package.into()),
                    ));
                }
                InputObjectKind::ImmOrOwnedMoveObject((obj_id, version, _digest)) => {
                    let object = self
                        .db
                        .get_object_by_key(&obj_id, version)
                        .ok_or_eyre(eyre!("object {} {} not found", obj_id, version))?;
                    objects.push(ObjectReadResult::new(
                        objref,
                        ObjectReadResultKind::Object(object),
                    ));
                }
                InputObjectKind::SharedMoveObject {
                    id,
                    initial_shared_version,
                    mutable: _,
                } => {
                    match self.db.get_object(&id) {
                        Some(object) => {
                            if initial_shared_version == object.owner.start_version().unwrap() {
                                objects.push(ObjectReadResult::new(
                                    objref,
                                    ObjectReadResultKind::Object(object),
                                ));
                            } else {
                                return Err(eyre!(
                                    "mismatched input: {:?} vs {}",
                                    &object,
                                    initial_shared_version
                                )
                                .into());
                            }
                        }
                        None => {
                            // let tx = db.env.begin_ro_txn().await?;
                            // let version =
                            // db.get_highest_object_data_lte_checkpoint(&tx, id, checkpoint.sequence_number - 1)
                            //     .await?;
                            // trace!("Latest version: {:?}", version);
                            return Err(eyre!(
                                "Shared object {}:{} not found",
                                id,
                                initial_shared_version
                            )
                            .into());
                        }
                    }
                }
            }
        }

        let gas_id = self.minted_gas.compute_object_reference();
        let gas_budget = if ptb
            .commands
            .iter()
            .any(|c| matches!(c, Command::Publish(_, _)))
        {
            1_000_000_000_000_000_0
        } else {
            1_000_000_000_000_000
        };
        let mut gas = SuiGasStatus::new(gas_budget, 1, 0, &self.env.protocol_config)?;
        let gas_status = GasStatus::new(
            CostTable {
                instruction_tiers: BTreeMap::from_iter(vec![(0, 50_000_000_000_00)]),
                stack_height_tiers: BTreeMap::new(),
                stack_size_tiers: BTreeMap::new(),
            },
            gas_budget,
            1,
            0,
        );
        *gas.move_gas_status_mut() = gas_status;

        objects.push(ObjectReadResult::new(
            InputObjectKind::ImmOrOwnedMoveObject(gas_id),
            ObjectReadResultKind::Object(self.minted_gas.clone()),
        ));

        let mut tracer = Some(MoveTraceBuilder::new_with_tracer(Box::new(tracer)));

        let tx_kind = TransactionKind::ProgrammableTransaction(ptb.clone());
        let tx_data = TransactionData::new(tx_kind, sender, gas_id, gas_budget, 0);

        trace!("Tx digest is {}", tx_data.digest());
        let (store, _gas_status, effects, _timing, result) =
            self.executor.execute_transaction_to_effects(
                &self.db,
                &self.env.protocol_config,
                self.env.metrics.clone(),
                false,
                Ok(()),
                &self.env.epoch,
                self.env.epoch_ms,
                CheckedInputObjects::new_for_replay(objects.into()),
                tx_data.gas_data().clone(),
                gas,
                tx_data.kind().clone(),
                tx_data.sender(),
                tx_data.digest(),
                &mut tracer,
            );
        drop(tracer);
        state.executions_mut().add_assign(1);

        if let Some(eval_metric) = state.eval_metrics_mut() {
            eval_metric.on_executed(&result, &ptb);
        }

        if let Some(err) = outcome.pending_error {
            trace!("Tracer has a pending error: {:?}", err);
            return Err(err);
        }

        let mut allowed_success = false;
        let mut stage_idx = None;
        trace!("Execution status: {:?}", &effects.status());
        if let ExecutionStatus::Failure { command, .. } = effects.status() {
            stage_idx = *command;
        }
        if let Some(flash) = flash.as_ref() {
            if let ExecutionStatus::Failure { error, command } = effects.status() {
                match error {
                    ExecutionFailureStatus::MoveAbort(loc, _) => {
                        if !flash.allowed_abort(loc, command, &ptb) {
                            code_ob.reset_map()?;
                        } else {
                            trace!("We allowed an abort at {:?}", loc);
                            allowed_success = true;
                        }
                    }
                    _ => {
                        code_ob.reset_map()?;
                    }
                }
            } else {
                allowed_success = true;
            }
        } else if result.is_err() || effects.status().is_err() {
            code_ob.reset_map()?;
        }

        // check if there are any crash events
        // TODO: Check during tracing?!
        for ev in store.events.data.iter() {
            if let Some(ev) = may_be_oracle(ev) {
                debug!("Found a crash event: {}", &ev);
                outcome.exit_kind_verdict = ExitKind::Crash;
            }
        }

        let out = ExecutionOutcome {
            events_verdict: outcome.exit_kind_verdict,
            events: store.events.clone(),
            written: store.written.clone(),
            input_objects: ptb.input_objects().unwrap_or_default(),
            effects,
            allowed_success,
            error: result.err().map(|t| t.into()),
            vulnerabilities: outcome.vulnerabilities.clone(),
        };

        let extra = ExecutionExtraOutcome {
            cmps: outcome.cmps,
            solver: concolic_state,
            stage_idx,
            success: allowed_success,
        };

        Ok((store, out, extra))
    }

    pub fn deploy_contract(
        &mut self,
        packages_with_deps: Vec<(Vec<CompiledModule>, Vec<ObjectID>, PackageMetadata)>,
        state: &mut S,
    ) -> Result<(ObjectID, SequenceNumber), BelobogError> {
        let mut results = vec![];
        for (modules, mut deps, mut abis) in packages_with_deps {
            let fixed_package_id = (*modules[0].self_id().address()).into();
            debug!(
                "Deploying package with original id {} and deps {:?}",
                fixed_package_id, deps
            );
            let mut zero_addr_modules = modules.clone();
            // rebase to zero address
            for it in zero_addr_modules.iter_mut() {
                for address_mut in it.address_identifiers.iter_mut() {
                    if let Some(new_addr) = state
                        .fuzz_state()
                        .module_address_to_package
                        .get(&(*address_mut).into())
                    {
                        *address_mut = (*new_addr).into();
                    }
                }
                let self_handle = it.self_handle().clone();
                let self_address_idx = self_handle.address;
                if let Some(address_mut) =
                    it.address_identifiers.get_mut(self_address_idx.0 as usize)
                    && *address_mut != AccountAddress::ZERO
                {
                    *address_mut = AccountAddress::ZERO;
                }
            }
            for dep in deps.iter_mut() {
                if let Some(new_dep) = state.fuzz_state().module_address_to_package.get(dep) {
                    *dep = *new_dep;
                }
            }
            let mut modules_bytes = vec![];
            for module in &zero_addr_modules {
                let mut buf = vec![];
                module.serialize_with_version(module.version, &mut buf)?;
                modules_bytes.push(buf);
            }

            let ptb = ProgrammableTransaction {
                inputs: vec![CallArg::Pure(bcs::to_bytes(&self.admin)?)],
                commands: vec![
                    Command::Publish(modules_bytes, deps.clone()), // This produces an upgrade cap
                    Command::TransferObjects(vec![Argument::Result(0)], Argument::Input(0)),
                ],
            };

            let (mut store, out, _) = self.execute_fuzz_input(ptb, None, state, self.admin)?;
            // look for new objects
            let mut new_object = None;
            debug!("all changed: {:?}", out.effects.all_changed_objects());
            for t in out.effects.all_changed_objects() {
                if matches!(&t.2, WriteKind::Create) && matches!(&t.1, Owner::Immutable) {
                    let object = store.written.get(&t.0.0).unwrap();
                    if object.is_package() {
                        new_object = Some(t.0);
                    }
                }
            }
            if let Some(err) = out.error {
                match err.kind {
                    ExecutionFailureStatus::PublishUpgradeMissingDependency => {
                        return Err(eyre!(
                            "Fail to deploy package due to missing dependency, 
                        maybe the 'published-at' address is for testnet. 
                        Remember to delete 'Move.lock' after you change it."
                        )
                        .into());
                    }
                    ExecutionFailureStatus::VMVerificationOrDeserializationError => {
                        return Err(eyre!(
                            "Fail to deploy package due to invalid bytecode, 
                            maybe you used a too new Move version.
                            eg. sui commit c3de14d, 41db91c"
                        )
                        .into());
                    }
                    _ => return Err(eyre!("Fail to deploy due to {}", err).into()),
                }
            } else if let Some(new_object) = new_object {
                debug!(
                    "Contract deployed at {}, original id: {}",
                    new_object.0, fixed_package_id
                );
                debug!("Outcome:\n{}", out.format(state.fuzz_state()));
                if out.error.is_some() {
                    return Err(eyre!("Fail to deploy due to {}", out.error.unwrap()).into());
                }

                let object = store.written.get(&new_object.0).unwrap();
                let package = object.data.try_as_package().unwrap();
                let new_abis: BTreeMap<_, _> = package
                    .serialized_module_map()
                    .values()
                    .map(|m| {
                        let compiled_module = CompiledModule::deserialize_with_defaults(m).unwrap();
                        let normed =
                            PackageMetadata::normalize_compiled_module(&compiled_module).unwrap();
                        (normed.name.to_string(), normed)
                    })
                    .collect();

                self.db.commit_store(store, &out.effects);

                let meta = state
                    .fuzz_state_mut()
                    .packages
                    .entry(new_object.0)
                    .or_insert(abis);
                for (mname, module) in new_abis.into_iter() {
                    meta.abis.insert(mname, module);
                }
                state
                    .fuzz_state_mut()
                    .module_address_to_package
                    .insert(fixed_package_id, new_object.0);

                results.push(Ok((new_object.0, new_object.1)));
            } else {
                return Err(eyre!("fail to deploy").into());
            }
        }
        results.pop().unwrap()
    }

    pub fn prerun(
        &mut self,
        ptb: ProgrammableTransaction,
        state: &mut S,
    ) -> Result<ExecutionOutcome, BelobogError> {
        let (store, out, _) = self.execute_fuzz_input(ptb.clone(), None, state, self.admin)?;
        debug!(
            "Input:\n{}",
            SuiFuzzInput {
                ptb: ptb.clone(),
                outcome: Some(out.clone()),
                ..Default::default()
            }
        );
        if out.error.is_some() {
            return Err(eyre!("Fail to prerun due to {}", out.error.unwrap()).into());
        }
        self.db.commit_store(store, &out.effects);
        Ok(out)
    }
}

impl<EM, Z, T, OT, RT, I, S> Executor<EM, I, S, Z> for SuiFuzzExecutor<T, OT, RT, I, S>
where
    T: BackingStore + ObjectTypesStore + ObjectStoreCommit,
    OT: ObserversTuple<I, S>,
    RT: OracleTuple<S>,
    I: SuiInput,
    S: HasRand + HasFuzzMetadata + HasExecutions + HasMetadata + HasExtraState,
    S::Rand: fastcrypto::traits::AllowedRng,
{
    fn run_target(
        &mut self,
        _fuzzer: &mut Z,
        state: &mut S,
        _mgr: &mut EM,
        input: &I,
    ) -> Result<ExitKind, libafl::Error> {
        // Clear any pending outcome
        state.extra_state_mut().global_outcome = None;
        let (store, mut outcome, extra) = self.execute_fuzz_input(
            input.ptb().clone(),
            input.flash().clone(),
            state,
            self.attacker,
        )?;
        let db = CachedStore::new(&self.db);
        db.commit_store(store, &outcome.effects);
        state.extra_state_mut().global_outcome =
            Some(GlobalOutcome::new(outcome.clone(), extra.clone()));
        let (kind, oracle_vulns) = self.oracles.done_execution(&db, state, None)?;
        if !oracle_vulns.is_empty() {
            outcome.vulnerabilities.extend(oracle_vulns.iter().cloned());
        }
        if let Some(global) = state.extra_state_mut().global_outcome.as_mut() {
            global.outcome = outcome.clone();
        }
        let kind = if self.disable_oracles {
            ExitKind::Ok
        } else if outcome.events_verdict == ExitKind::Ok {
            kind
        } else {
            outcome.events_verdict
        };

        if let Some(tracer) = &self.log_tracer
            && let Some(v) = tracer.may_log(input, &outcome, &extra)
        {
            info!("{}", v);
        }

        Ok(kind)
    }
}
