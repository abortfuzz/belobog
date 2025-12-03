use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Display,
    hash::Hash,
    io::Read,
    str::FromStr,
};

use belobog_types::error::BelobogError;
use itertools::Itertools;
use libafl::inputs::Input;
use libafl_bolts::{fs::write_file_atomic, generic_hash_std};
use move_core_types::u256::U256;
use serde::{Deserialize, Serialize};
use sui_types::{
    base_types::ObjectID,
    transaction::{Argument, CallArg, Command, ObjectArg, ProgrammableTransaction},
};

use crate::{
    build::Magic,
    executor::{CmpOp, ExecutionExtraOutcome, ExecutionOutcome, Log},
    flash::FlashWrapper,
    meta::{FunctionIdent, FuzzMetadata, HasFuzzMetadata},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuiFuzzInput {
    pub ptb: ProgrammableTransaction,

    // Input Metadata
    pub outcome: Option<ExecutionOutcome>,
    pub magic_number_pool: BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeSet<Vec<u8>>>>>,
    pub flash: Option<FlashWrapper>,
    pub display: Option<String>,
}

impl Display for SuiFuzzInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.format_impl(None))
    }
}

impl Hash for SuiFuzzInput {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ptb.hash(state);
        self.flash.hash(state);
        // ignore metadata
    }
}

impl Default for SuiFuzzInput {
    fn default() -> Self {
        Self {
            ptb: ProgrammableTransaction {
                inputs: vec![],
                commands: vec![],
            },
            outcome: None,
            magic_number_pool: BTreeMap::new(),
            flash: None,
            display: None,
        }
    }
}

impl SuiFuzzInput {
    pub fn format(&self, meta: &FuzzMetadata) -> String {
        self.format_impl(Some(meta))
    }

    pub fn format_impl(&self, meta: Option<&FuzzMetadata>) -> String {
        let inputs = self
            .ptb
            .inputs
            .iter()
            .map(|t| match t {
                CallArg::Pure(x) => {
                    let v = if x.len() == 1 {
                        u8::from_le_bytes(x.as_slice().try_into().unwrap()).to_string()
                    } else if x.len() == 2 {
                        u16::from_le_bytes(x.as_slice().try_into().unwrap()).to_string()
                    } else if x.len() == 4 {
                        u32::from_le_bytes(x.as_slice().try_into().unwrap()).to_string()
                    } else if x.len() == 8 {
                        u64::from_le_bytes(x.as_slice().try_into().unwrap()).to_string()
                    } else if x.len() == 16 {
                        u128::from_le_bytes(x.as_slice().try_into().unwrap()).to_string()
                    } else if x.len() == 32 {
                        U256::from_le_bytes(x.as_slice().try_into().unwrap()).to_string()
                    } else {
                        "...".to_string()
                    };
                    format!("Pure({}, guess={})", const_hex::encode(x), v)
                }
                CallArg::Object(obj) => match obj {
                    ObjectArg::ImmOrOwnedObject(imm) => format!("ImmOrOwnedObject({})", imm.0),
                    ObjectArg::Receiving(recv) => format!("Receiving({})", recv.0),
                    ObjectArg::SharedObject {
                        id,
                        initial_shared_version: _,
                        mutable,
                    } => {
                        if *mutable {
                            format!("Shared(&mut {})", id)
                        } else {
                            format!("Shared(&{})", id)
                        }
                    }
                },
                CallArg::FundsWithdrawal(balance) => format!("BalanceWithdraw({:?})", balance),
            })
            .collect_vec();

        let fmt_arg = |arg: &Argument| match arg {
            Argument::Input(idx) => inputs
                .get(*idx as usize)
                .map(|v| format!("Input({}, {})", idx, v))
                .unwrap_or_else(|| format!("Input({})", *idx)),
            Argument::GasCoin => "GasCoin".to_string(),
            Argument::NestedResult(m, n) => {
                format!("NestedResult({}, {})", m, n)
            }
            Argument::Result(m) => {
                format!("Result({})", m)
            }
        };

        let fmt_arguments = |args: &[Argument]| args.iter().map(&fmt_arg).collect_vec();

        let calls = self
            .ptb
            .commands
            .iter()
            .map(|call| match call {
                Command::MoveCall(mc) => {
                    let args = fmt_arguments(&mc.arguments);

                    format!(
                        "MoveCall({}:{}:{}{}({}))",
                        &mc.package,
                        &mc.module,
                        &mc.function,
                        if mc.type_arguments.is_empty() {
                            ""
                        } else {
                            &format!(
                                "<{}>",
                                mc.type_arguments
                                    .iter()
                                    .map(|t| t.to_canonical_string(true))
                                    .join(",")
                            )
                        },
                        args.into_iter().join(",")
                    )
                }
                Command::SplitCoins(src, dst) => {
                    let src = fmt_arg(src);
                    let dsts = fmt_arguments(dst);

                    format!("SplitCoins({}, [{}])", src, dsts.into_iter().join(","))
                }
                Command::TransferObjects(srcs, dst) => {
                    let dst = fmt_arg(dst);
                    let srcs = fmt_arguments(srcs);

                    format!("TransferObjects([{}], {})", srcs.into_iter().join(","), dst)
                }
                Command::MergeCoins(dst, srcs) => {
                    let dst = fmt_arg(dst);
                    let srcs = fmt_arguments(srcs);

                    format!("MergeCoins({}, [{}])", dst, srcs.into_iter().join(","))
                }
                Command::Publish(_, deps) => {
                    format!(
                        "Publish(..., [{}])",
                        deps.iter().map(|t| t.to_string()).join(",")
                    )
                }
                Command::MakeMoveVec(ty, srcs) => {
                    let srcs = fmt_arguments(srcs);
                    format!(
                        "MakeMoveVec({}, [{}])",
                        ty.as_ref()
                            .map(|t| t.to_canonical_string(true))
                            .unwrap_or_else(|| "None".to_string()),
                        srcs.into_iter().join(",")
                    )
                }
                Command::Upgrade(_, deps, package, ticket) => {
                    let ticket = fmt_arg(ticket);

                    format!(
                        "Upgrade(..., [{}], {}, {})",
                        deps.iter().map(|v| v.to_string()).join(","),
                        package,
                        ticket
                    )
                }
            })
            .collect_vec();

        let flash = if let Some(flash) = &self.flash {
            format!("{}", flash)
        } else {
            "No flash".to_string()
        };

        let outcome = if let Some(oc) = &self.outcome {
            oc.format_impl(meta)
        } else {
            "No outcome".to_string()
        };
        format!(
            "Inputs:\n\t{}\nCommands:\n\t{}\nFlash:\n\t{}\nOutcome: |\n{}\n",
            inputs.into_iter().join("\n\t"),
            calls.into_iter().join("\n\t"),
            flash,
            outcome
        )
    }

    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn flash(flash: FlashWrapper) -> Result<Self, BelobogError> {
        let ptb = flash.flash_seed(&flash.flash_coin, flash.initial_flash_amount)?;

        Ok(Self {
            ptb,
            flash: Some(flash),
            ..Default::default()
        })
    }
}

pub trait SuiInput: Display {
    fn ptb(&self) -> &ProgrammableTransaction;

    fn ptb_mut(&mut self) -> &mut ProgrammableTransaction;

    fn outcome(&self) -> &Option<ExecutionOutcome>;
    fn outcome_mut(&mut self) -> &mut Option<ExecutionOutcome>;

    fn update_magic_number(&mut self, outcome: &ExecutionExtraOutcome);
    fn magic_number_pool(&self, meta: &FuzzMetadata) -> BTreeMap<FunctionIdent, BTreeSet<Vec<u8>>>;
    fn flash(&self) -> &Option<FlashWrapper>;

    fn display_mut(&mut self) -> &mut Option<String>;
    fn format_with_meta(&self, meta: Option<&FuzzMetadata>) -> String;
}

impl SuiInput for SuiFuzzInput {
    fn ptb(&self) -> &ProgrammableTransaction {
        &self.ptb
    }

    fn ptb_mut(&mut self) -> &mut ProgrammableTransaction {
        &mut self.ptb
    }

    fn outcome(&self) -> &Option<ExecutionOutcome> {
        &self.outcome
    }

    fn outcome_mut(&mut self) -> &mut Option<ExecutionOutcome> {
        &mut self.outcome
    }

    fn update_magic_number(&mut self, outcome: &ExecutionExtraOutcome) {
        self.magic_number_pool.clear(); // clear old pool, and rebuild it
        let mutate_cmps = outcome.cmps.clone();
        for (pkg, module_map) in &mutate_cmps {
            for (module, func_map) in module_map {
                for (func, cmps) in func_map {
                    let function_pool = self
                        .magic_number_pool
                        .entry(pkg.to_string())
                        .or_default()
                        .entry(module.to_string())
                        .or_default()
                        .entry(func.to_string())
                        .or_default();
                    for cmp in cmps.iter() {
                        let Log::CmpLog(cmp) = cmp else {
                            continue;
                        };
                        if matches!(cmp.op, CmpOp::EQ | CmpOp::GE | CmpOp::LE) {
                            for magic in [&cmp.lhs, &cmp.rhs] {
                                match magic {
                                    Magic::U8(v) => {
                                        function_pool.insert(vec![*v]);
                                    }
                                    Magic::U16(v) => {
                                        function_pool.insert(v.to_le_bytes().to_vec());
                                    }
                                    Magic::U32(v) => {
                                        function_pool.insert(v.to_le_bytes().to_vec());
                                    }
                                    Magic::U64(v) => {
                                        function_pool.insert(v.to_le_bytes().to_vec());
                                    }
                                    Magic::U128(v) => {
                                        function_pool.insert(v.to_le_bytes().to_vec());
                                    }
                                    Magic::U256(v) => {
                                        function_pool.insert(v.to_le_bytes().to_vec());
                                    }
                                    Magic::Bytes(v) => {
                                        if !v.is_empty() {
                                            function_pool.insert(v.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn magic_number_pool(&self, meta: &FuzzMetadata) -> BTreeMap<FunctionIdent, BTreeSet<Vec<u8>>> {
        let mut res = BTreeMap::new();
        for (pkg, module_map) in &self.magic_number_pool {
            let pkg = ObjectID::from_str(pkg).unwrap();
            let pkg = meta
                .module_address_to_package
                .get(&pkg)
                .cloned()
                .unwrap_or(pkg);
            for (module, func_map) in module_map {
                for (func, cmps) in func_map {
                    let function_ident = FunctionIdent(pkg, module.clone(), func.clone());
                    res.insert(function_ident, cmps.clone());
                }
            }
        }
        res
    }

    fn flash(&self) -> &Option<FlashWrapper> {
        &self.flash
    }

    fn display_mut(&mut self) -> &mut Option<String> {
        &mut self.display
    }
    fn format_with_meta(&self, meta: Option<&FuzzMetadata>) -> String {
        self.format_impl(meta)
    }
}

impl Input for SuiFuzzInput {
    fn to_file<P>(&self, path: P) -> Result<(), libafl::Error>
    where
        P: AsRef<std::path::Path>,
    {
        write_file_atomic(
            path,
            &serde_json::to_vec(self).map_err(|e| libafl::Error::serialize(e.to_string()))?,
        )
    }

    fn from_file<P>(path: P) -> Result<Self, libafl::Error>
    where
        P: AsRef<std::path::Path>,
    {
        let mut file = std::fs::File::open(path)?;
        let mut bytes = vec![];
        file.read_to_end(&mut bytes)?;
        serde_json::from_slice(&bytes).map_err(|e| libafl::Error::serialize(e.to_string()))
    }

    fn generate_name(&self, id: Option<libafl::corpus::CorpusId>) -> String {
        if let Some(id) = id {
            format!("{}.json", id)
        } else {
            format!("{:016x}.json", generic_hash_std(self))
        }
    }
}

pub fn merge_ptbs(
    dst: &mut ProgrammableTransaction,
    src: ProgrammableTransaction,
    cmd_index: usize,
) {
    let (inputs, mut cmds) = (src.inputs, src.commands);

    let handle_argument_input =
        |arg: &mut Argument, input_map: &HashMap<u16, u16>, src_result_head: u16| match arg {
            Argument::Input(idx) => {
                if let Some(new) = input_map.get(idx) {
                    *idx = *new;
                }
            }
            Argument::Result(ridx) | Argument::NestedResult(ridx, _) => {
                *ridx += src_result_head;
            }
            _ => {}
        };

    let mut input_map = HashMap::new();
    for (old_input_idx, input) in inputs.into_iter().enumerate() {
        let new_input_idx = if let Some(idx) = dst.inputs.iter().position(|t| match t {
            CallArg::Object(ObjectArg::ImmOrOwnedObject(_)) => t == &input,
            CallArg::Object(ObjectArg::SharedObject {
                id: _,
                initial_shared_version: _,
                mutable: _,
            }) => t == &input,
            _ => false,
        }) {
            idx as u16
        } else {
            let idx = dst.inputs.len();
            dst.inputs.push(input);
            idx as u16
        };
        let old_input_idx = old_input_idx as u16;
        input_map.insert(old_input_idx, new_input_idx);
    }

    let cmd_index = cmd_index as u16;
    for cmd in cmds.iter_mut() {
        match cmd {
            Command::MakeMoveVec(_, args) => {
                for arg in args.iter_mut() {
                    handle_argument_input(arg, &input_map, cmd_index);
                }
            }
            Command::MergeCoins(lhs, rhs)
            | Command::SplitCoins(lhs, rhs)
            | Command::TransferObjects(rhs, lhs) => {
                handle_argument_input(lhs, &input_map, cmd_index);
                for arg in rhs.iter_mut() {
                    handle_argument_input(arg, &input_map, cmd_index);
                }
            }
            Command::MoveCall(mc) => {
                for arg in mc.arguments.iter_mut() {
                    handle_argument_input(arg, &input_map, cmd_index);
                }
            }
            Command::Publish(_, _) => {}
            Command::Upgrade(_, _, _, arg) => {
                handle_argument_input(arg, &input_map, cmd_index);
            }
        }
    }

    let mp = HashMap::new();
    let origin_skip = cmds.len() as u16;
    let cmd_index = cmd_index as usize;
    for cmd in dst.commands.iter_mut().skip(cmd_index) {
        match cmd {
            Command::MakeMoveVec(_, args) => {
                for arg in args.iter_mut() {
                    handle_argument_input(arg, &mp, origin_skip);
                }
            }
            Command::MergeCoins(lhs, rhs)
            | Command::SplitCoins(lhs, rhs)
            | Command::TransferObjects(rhs, lhs) => {
                handle_argument_input(lhs, &mp, origin_skip);
                for arg in rhs.iter_mut() {
                    handle_argument_input(arg, &mp, origin_skip);
                }
            }
            Command::MoveCall(mc) => {
                for arg in mc.arguments.iter_mut() {
                    handle_argument_input(arg, &mp, origin_skip);
                }
            }
            Command::Publish(_, _) => {}
            Command::Upgrade(_, _, _, arg) => {
                handle_argument_input(arg, &mp, origin_skip);
            }
        }
    }

    dst.commands.splice(cmd_index..cmd_index, cmds);
}
