use std::collections::BTreeSet;
use std::{fmt::Debug, num::NonZero, path::PathBuf, str::FromStr, time::Duration};

use crate::flash::FlashProvider;
use crate::meta::FunctionIdent;
use crate::metrics::EvaluationMetrics;
use crate::mutators::sequence::{append_function, process_key_store, remove_process_key_store};
use crate::object_sampler::{ObjectData, sui_move_ability_set_to_ability_set};
use crate::oracles::bool_judgement::BoolJudgementOracle;
use crate::oracles::infinite_loop::InfiniteLoopOracle;
use crate::oracles::precision_loss::PrecisionLossOracle;
use crate::oracles::proceeds::ProceedsOracle;
use crate::oracles::type_conversion::TypeConversionOracle;
use crate::oracles::unchecked_return::UncheckedReturnOracle;
use crate::oracles::unused_const::UnusedConstOracle;
use crate::oracles::unused_private_fun::UnusedPrivateFunOracle;
use crate::oracles::unused_struct::UnusedStructOracle;
use crate::sched::SuiFuzzInputScore;
use crate::state::{HasExtraState, SuperState};
use crate::{
    build::PackageMetadata,
    executor::{CODE_OBSERVER_NAME, ExecutionEnvironment, SuiFuzzExecutor},
    flash::FlashWrapper,
    input::SuiFuzzInput,
    meta::{FuzzMetadata, HasFuzzMetadata},
    mutators::{arg::ArgMutator, magic_number::MagicNumberMutator, sequence::SequenceMutator},
    oracles::{CouldDisabledOralce, OracleTuple, overflow::OverflowOracle},
    utils::{AppendOutcomeFeedback, SelectiveCorpus, SuperRand},
};
use belobog_fork::db::ObjectForkDatabase;
use belobog_fork::empty::EmptyStore;
use belobog_fork::{
    ObjectStoreCommit, ObjectTypesStore, cache::CachedStore, graphql::GraphQlDatabase,
};
use belobog_fork::{TrivialBackStore, file::MDBXCachedStore, types::TypeCachedStore};
use belobog_types::error::BelobogError;
use clap::Args;
use clap::Subcommand;
use color_eyre::eyre::eyre;
use itertools::Itertools;
use libafl::{
    Evaluator, Fuzzer, HasMetadata, StdFuzzer,
    corpus::{InMemoryCorpus, InMemoryOnDiskCorpus},
    events::{ProgressReporter, SimpleEventManager},
    feedback_and_fast,
    feedbacks::{CrashFeedback, ExitKindFeedback, ExitKindLogic, MaxMapPow2Feedback},
    monitors::SimpleMonitor,
    observers::{ObserversTuple, StdMapObserver},
    schedulers::WeightedScheduler,
    stages::{CalibrationStage, StdMutationalStage},
    state::{HasExecutions, HasRand, StdState},
};
use libafl_bolts::{rands::StdRand, tuples::tuple_list};
use log::{info, warn};
use mdbx_derive::MDBXDatabase;
use move_core_types::language_storage::StructTag;
use rand_libafl::RngCore;
use serde::{Deserialize, Serialize};
use sui_sdk::{SuiClient, SuiClientBuilder};
use sui_types::transaction::ProgrammableTransaction;
use sui_types::{
    base_types::{ObjectID, SuiAddress},
    storage::BackingStore,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectWithversion {
    pub id: ObjectID,
    pub version: Option<u64>,
}

impl FromStr for ObjectWithversion {
    type Err = BelobogError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ts = s.split(":").collect_vec();
        if ts.len() == 1 {
            let id = ObjectID::from_str(ts[0])
                .map_err(|e| eyre!("can not parse id {} with {}", ts[0], e))?;
            return Ok(Self { id, version: None });
        } else if ts.len() == 2 {
            let version = u64::from_str(ts[1])
                .map_err(|e| eyre!("can not parse version {} with {}", ts[1], e))?;
            let id = ObjectID::from_str(ts[0])
                .map_err(|e| eyre!("can not parse id {} with {}", ts[0], e))?;
            Ok(Self {
                id,
                version: Some(version),
            })
        } else {
            Err(eyre!("can not parse ObjectWithversion from {}", s).into())
        }
    }
}

fn yes() -> bool {
    true
}

#[derive(Subcommand, Serialize, Deserialize, Clone, Debug)]
pub enum FuzzTargets {
    Onchain {
        #[arg(
            short,
            long = "address",
            env = "ONCHAIN_ADDRESSES",
            value_delimiter = ','
        )]
        addresses: Vec<ObjectID>,
        #[serde(default)]
        #[arg(short, long = "object", env = "ONCHAIN_OBJECTS", value_delimiter = ',')]
        objects: Vec<ObjectWithversion>,
        #[serde(default = "yes")] // because previous this defaults to true
        #[arg(short, long)]
        ptb_objects: bool,
        #[arg(short, long)]
        checkpoint: Option<u64>,
        #[arg(short, long)]
        struct_tags: Vec<StructTag>,
    },
    Source {
        #[arg(short, long = "folder")]
        folders: Vec<PathBuf>,
        #[arg(short, long, value_delimiter = ',')]
        prerun_functions: Vec<FunctionIdent>,
    },
}

impl FuzzTargets {
    pub async fn env(&mut self, rpc: &SuiClient) -> Result<ExecutionEnvironment, BelobogError> {
        match self {
            Self::Onchain {
                addresses: _,
                objects: _,
                ptb_objects: _,
                checkpoint,
                struct_tags: _,
            } => {
                let fork = if let Some(fork) = checkpoint {
                    *fork
                } else {
                    rpc.read_api()
                        .get_latest_checkpoint_sequence_number()
                        .await?
                };
                *checkpoint = Some(fork);
                Ok(ExecutionEnvironment::from_rpc(rpc, fork).await?)
            }
            Self::Source {
                folders,
                prerun_functions: _,
            } => {
                *folders = folders
                    .iter_mut()
                    .map(|t| t.canonicalize())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(ExecutionEnvironment::local_testing())
            }
        }
    }

    pub fn onchain(&self) -> bool {
        matches!(self, Self::Onchain { .. })
    }

    pub async fn setup<T, OT, RT, S, I>(
        &self,
        state: &mut S,
        rpc: &SuiClient,
        executor: &mut SuiFuzzExecutor<T, OT, RT, I, S>,
    ) -> Result<(), BelobogError>
    where
        T: ObjectTypesStore + BackingStore + ObjectStoreCommit,
        OT: ObserversTuple<I, S>,
        RT: OracleTuple<S>,
        S: HasRand + HasFuzzMetadata + HasExecutions + HasMetadata + HasExtraState,
        S::Rand: fastcrypto::traits::AllowedRng,
    {
        match self {
            FuzzTargets::Source {
                folders,
                prerun_functions,
            } => {
                let attacker_mint_id = ObjectID::random_from_rng(state.rand_mut());
                let admin_mint_id = ObjectID::random_from_rng(state.rand_mut());
                executor
                    .db
                    .mint_gas_object(attacker_mint_id, executor.attacker.into())?;
                executor
                    .db
                    .mint_gas_object(admin_mint_id, executor.admin.into())?;
                info!("Minted gas for attacker: {}", attacker_mint_id);
                info!("Minted gas for admin: {}", admin_mint_id);

                state.fuzz_state_mut().register_system_packages(rpc).await?;
                state
                    .fuzz_state_mut()
                    .register_sui_objects(&executor.db)
                    .await?;
                for folder in folders {
                    let packages = PackageMetadata::from_folder_unpublished_or_root(folder)?;
                    let (address, seq) = executor.deploy_contract(packages, state)?;
                    info!("address: {}", address);
                    state.fuzz_state_mut().add_package_to_target(&address);
                    state
                        .fuzz_state_mut()
                        .may_add_coverage_target(&address, &executor.db);
                    state
                        .fuzz_state_mut()
                        .target_functions
                        .retain(|f| &f.1 != "log" && &f.1 != "oracle");
                    let object = executor.db.get_object_by_key(&address, seq).unwrap();
                    state
                        .fuzz_state_mut()
                        .analyze_dependency(rpc, &object, &executor.db)
                        .await?;
                }
                state
                    .fuzz_state_mut()
                    .analyze_object_types(rpc, &executor.db)
                    .await?;
                info!(
                    "module address to package: {:?}",
                    state.fuzz_state().module_address_to_package
                );
                info!(
                    "target functions: {:?}",
                    state.fuzz_state().target_functions
                );

                state.fuzz_state_mut().set_to_admin();
                state.fuzz_state_mut().gas_id = Some(executor.minted_gas.id());
                for prerun_function in prerun_functions {
                    let mut ptb = ProgrammableTransaction {
                        inputs: vec![],
                        commands: vec![],
                    };
                    let prerun_function = FunctionIdent(
                        *state
                            .fuzz_state_mut()
                            .module_address_to_package
                            .get(&prerun_function.0)
                            .unwrap_or(&prerun_function.0),
                        prerun_function.1.to_string(),
                        prerun_function.2.to_string(),
                    );
                    append_function(
                        &executor.db,
                        state,
                        &mut ptb,
                        &prerun_function,
                        Default::default(),
                        Default::default(),
                        &Default::default(),
                        true,
                        0,
                    )
                    .ok_or(eyre!("error appending prerun function {}", prerun_function))?;
                    executor.prerun(ptb, state)?;
                }
                state.fuzz_state_mut().set_to_attacker();
            }
            FuzzTargets::Onchain {
                addresses,
                objects,
                ptb_objects,
                checkpoint: _,
                struct_tags,
            } => {
                let mut objects = objects.clone();
                for address in addresses {
                    info!("Adding {} to our environment...", address);
                    let (object, abis) = PackageMetadata::from_rpc(rpc, *address).await?;
                    info!("Looking for objects from history ptbs...");
                    if *ptb_objects {
                        objects.extend(abis.fetch_move_objects(rpc).await?.into_iter().map(|v| {
                            ObjectWithversion {
                                id: v.0,
                                version: Some(v.1.into()),
                            }
                        }));
                    }
                    state
                        .fuzz_state_mut()
                        .add_single_package(rpc, *address, &executor.db)
                        .await?;
                    state
                        .fuzz_state_mut()
                        .analyze_dependency(rpc, &object, &executor.db)
                        .await?;
                    state.fuzz_state_mut().add_package_to_target(&object.id());
                    state
                        .fuzz_state_mut()
                        .may_add_coverage_target_object(object);
                }
                for object in objects {
                    if let Some(version) = object.version {
                        executor.db.get_object_by_key(&object.id, version.into());
                    } else {
                        executor.db.get_object(&object.id);
                    }
                }

                // analyze struct tags
                for tag in struct_tags {
                    let (_, abis) = PackageMetadata::from_rpc(rpc, tag.address.into()).await?;
                    state
                        .fuzz_state_mut()
                        .packages
                        .insert(tag.address.into(), abis);
                    let abilities = &state
                        .fuzz_state()
                        .get_abilities(&tag.address.into(), tag.module.as_str(), tag.name.as_str())
                        .unwrap();
                    state
                        .fuzz_state_mut()
                        .ability_to_type_tag
                        .entry(*abilities)
                        .or_default()
                        .push(tag.clone().into());
                }

                state
                    .fuzz_state_mut()
                    .analyze_object_types(rpc, &executor.db)
                    .await?;
            }
        }
        Ok(())
    }
}

#[derive(Args, Serialize, Deserialize, Clone, Debug)]
pub struct FuzzArgs {
    #[arg(long, default_value_t = Self::random_attacker())]
    pub attacker: SuiAddress,
    #[arg(short, long)]
    pub output: Option<PathBuf>,
    #[arg(short, long)]
    pub cache_db: Option<String>,
    #[arg(long)]
    pub cache_ro: bool,
    #[arg(long)]
    pub fork_db: Option<String>,
    #[arg(short, long)]
    pub time_limit: Option<u64>,
    #[arg(short, long, default_value_t = Self::random_seed())]
    pub seed: u64,
    #[arg(long, default_value = "https://fullnode.mainnet.sui.io")]
    pub rpc: String,
    #[arg(short, long)]
    pub no_builtin_oracles: bool,
    // https://suivision.xyz/txblock/ETCaBBiffASZ3oXBBcoM6VYd3NcTb5T1Sqo4xLECKZws?tab=Overview
    // cetus,0xc6faf3703b0e8ba9ed06b7851134bbbe7565eb35ff823fd78432baa4cbeaa12e,0xdaa46292632c3c4d8f31f23ea0f9b36a28ff3677e9684980e4438403a67a3d8f,0x04d7dc303d0c7c381ba468da0b42ac57499aec62dd221e52496f7e2350009ee2,0x2::sui::SUI,555671134433
    #[arg(short, long)]
    pub flash: Option<String>,
    #[arg(long, value_delimiter = ',')]
    pub privilege_functions: Vec<FunctionIdent>,

    // debug options
    #[arg(long, default_value_t = 16384)]
    pub debug_code_coverage: usize,
    #[arg(long)]
    pub debug_eval_metrics: bool,
    #[arg(long, default_value_t = 5)]
    pub debug_eval_metrics_cycles: usize,

    #[clap(subcommand)]
    pub source: FuzzTargets,
}

pub struct OkFeedback;

impl ExitKindLogic for OkFeedback {
    const NAME: std::borrow::Cow<'static, str> = std::borrow::Cow::Borrowed("OkFeedback");
    fn check_exit_kind(kind: &libafl::executors::ExitKind) -> Result<bool, libafl::Error> {
        Ok(matches!(kind, libafl::executors::ExitKind::Ok))
    }
}

impl FuzzArgs {
    fn random_seed() -> u64 {
        rand_libafl::rng().next_u64()
    }

    pub fn random_attacker() -> SuiAddress {
        // Fair dice roll!
        SuiAddress::from_str("0xa773c4c5ef0b74150638fcfe8b0cd1bb3bbf6f1af963715168ad909bbaf2eddb")
            .unwrap()
    }

    pub fn admin(&self) -> SuiAddress {
        SuiAddress::from_str("0xb64151ee0dd0f7bab72df320c5f8e0c4b784958e7411a6c37d352fe9e176092f")
            .unwrap()
    }

    pub fn env(&mut self, rpc: &SuiClient) -> Result<ExecutionEnvironment, BelobogError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.source.env(rpc))
        })
    }

    pub fn rpc(&self) -> Result<SuiClient, BelobogError> {
        Ok(tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { SuiClientBuilder::default().build(&self.rpc).await })
        })?)
    }

    pub fn db(
        &self,
        env: &ExecutionEnvironment,
    ) -> Result<
        impl ObjectTypesStore + BackingStore + ObjectStoreCommit + Clone + 'static,
        BelobogError,
    > {
        // TODO: Support belobog-fork

        let inner = if let Some(fork_db) = &self.fork_db {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let db = ObjectForkDatabase::new(fork_db, true).await?;
                    let fork_ckpt = db
                        .db
                        .metadata()
                        .await?
                        .ok_or_else(|| eyre!("no metadata for {}", fork_db))?;
                    if fork_ckpt.checkpoint != env.checkpoint {
                        return Err(eyre!(
                            "fork db is at {} but we want to fork {}",
                            fork_ckpt.checkpoint,
                            env.checkpoint
                        )
                        .into());
                    }
                    Ok::<_, BelobogError>(TrivialBackStore::T1(db))
                })
            })?
        } else {
            TrivialBackStore::T2(GraphQlDatabase::new_mystens(env.checkpoint))
        };

        let db = if let Some(cache_db) = &self.cache_db {
            let mdbx = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    MDBXCachedStore::new(cache_db, inner, env.checkpoint, self.cache_ro).await
                })
            })?;

            TrivialBackStore::T1(TypeCachedStore::new(CachedStore::new(mdbx)))
        } else {
            let db = TypeCachedStore::new(CachedStore::new(inner));
            TrivialBackStore::T2(db)
        };

        Ok(db)
    }

    pub fn code_observer(&self) -> StdMapObserver<'_, u8, false> {
        StdMapObserver::owned(CODE_OBSERVER_NAME, vec![0u8; self.debug_code_coverage])
    }

    pub fn rand(&self) -> SuperRand {
        SuperRand(StdRand::with_seed(self.seed))
    }

    pub fn oracles<S: HasFuzzMetadata + HasExtraState>(&self) -> impl OracleTuple<S> {
        let disabled = self.no_builtin_oracles;
        tuple_list!(
            CouldDisabledOralce::new(OverflowOracle {}, disabled),
            CouldDisabledOralce::new(ProceedsOracle {}, disabled),
            CouldDisabledOralce::new(TypeConversionOracle::default(), disabled),
            CouldDisabledOralce::new(UnusedConstOracle::default(), disabled),
            CouldDisabledOralce::new(UnusedPrivateFunOracle::default(), disabled),
            CouldDisabledOralce::new(UnusedStructOracle::default(), disabled),
            CouldDisabledOralce::new(PrecisionLossOracle::default(), disabled),
            CouldDisabledOralce::new(BoolJudgementOracle::default(), disabled),
            CouldDisabledOralce::new(UncheckedReturnOracle::default(), disabled),
            CouldDisabledOralce::new(InfiniteLoopOracle::default(), disabled)
        )
    }

    pub fn fuzz(mut self) -> Result<(), BelobogError> {
        let attacker = self.attacker;
        let admin = self.admin();
        let rpc = self.rpc()?;
        let env = self.env(&rpc)?;
        let db = self.db(&env)?;
        let code_observer = self.code_observer();
        let coverage_feedback = MaxMapPow2Feedback::with_name("code-fb", &code_observer);

        let calib = CalibrationStage::new(&coverage_feedback);
        let mut corpus_feedback = feedback_and_fast!(
            ExitKindFeedback::<OkFeedback>::new(),
            AppendOutcomeFeedback {},
            coverage_feedback
        );
        let mut crash_feedback = feedback_and_fast!(
            CrashFeedback::new(),
            AppendOutcomeFeedback {},
            MaxMapPow2Feedback::with_name("crash-fb", &code_observer)
        );

        if let Some(output) = &self.output {
            let config = output.join("config.json");

            if config.exists() {
                let meta = config.metadata()?;
                let now = std::time::SystemTime::now();
                let elpsed = now
                    .duration_since(meta.modified()?)
                    .map_err(|e| eyre!("fail to get elapsed for config file due to {}", e))?;
                if elpsed.as_secs_f64() > (20 * 60) as f64 {
                    return Err(eyre!("The output folder exists for more than 20 minutes, please remove it manually").into());
                } else {
                    let folders = [output.join("queue"), output.join("crashes")];

                    for folder in folders.iter() {
                        if folder.exists() {
                            std::fs::remove_dir_all(folder)?;
                        }
                    }
                    info!("Cleared the folder {}", output.display());
                }
            } else {
                std::fs::create_dir_all(output)?;
            }

            let fp = std::fs::File::create(&config)?;
            serde_json::to_writer_pretty(fp, &self)?;
        }

        let corpus = if let Some(output) = &self.output {
            let corpus = output.join("queue");
            std::fs::create_dir_all(&corpus)?;
            SelectiveCorpus::corpus1(InMemoryOnDiskCorpus::<SuiFuzzInput>::new(corpus)?)
        } else {
            SelectiveCorpus::corpus2(InMemoryCorpus::<SuiFuzzInput>::new())
        };

        let crashes = if let Some(output) = &self.output {
            let crash = output.join("crashes");
            std::fs::create_dir_all(&crash)?;
            SelectiveCorpus::corpus1(InMemoryOnDiskCorpus::new(crash)?)
        } else {
            SelectiveCorpus::corpus2(InMemoryCorpus::new())
        };

        let state = StdState::new(
            self.rand(),
            corpus,
            crashes,
            &mut corpus_feedback,
            &mut crash_feedback,
        )?;

        let mut state = SuperState::new(state);

        let meta = FuzzMetadata {
            attacker,
            admin,
            current_sender: attacker,
            eval_metrics: if self.debug_eval_metrics {
                Some(EvaluationMetrics::default())
            } else {
                None
            },
            ..Default::default()
        };
        state.add_metadata::<FuzzMetadata>(meta);

        let sched: WeightedScheduler<_, SuiFuzzInputScore, _> =
            WeightedScheduler::new(&mut state, &code_observer);
        let mut executor = SuiFuzzExecutor::new(
            env,
            tuple_list!(code_observer),
            db.clone(),
            attacker,
            admin,
            self.oracles(),
            &mut state,
        )?;

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.source.setup(&mut state, &rpc, &mut executor).await?;

                Ok::<_, BelobogError>(())
            })
        })?;
        info!(
            "Known packages: {:?}",
            state.fuzz_state_mut().packages.keys()
        );
        info!(
            "Module address to package: {:?}",
            state.fuzz_state_mut().module_address_to_package
        );
        info!("db object types: {:?}", db.ty_cache());
        state
            .fuzz_state_mut()
            .initialize(executor.minted_gas.id(), self.privilege_functions.clone());
        info!(
            "Target functions: {:?}",
            state.fuzz_state_mut().target_functions
        );
        info!(
            "producing graph: {:?}",
            state.fuzz_state_mut().producing_graph
        );
        info!(
            "consuming graph: {:?}",
            state.fuzz_state_mut().consuming_graph
        );
        // info!(
        //     "ability to type tag: {:?}",
        //     &state.fuzz_state_mut().ability_to_type_tag
        // );

        let mut stages = tuple_list!(
            calib,
            StdMutationalStage::with_max_iterations(
                SequenceMutator::new(db.clone()),
                NonZero::new(256).unwrap()
            ),
            StdMutationalStage::with_max_iterations(ArgMutator::new(), NonZero::new(1).unwrap()),
            StdMutationalStage::with_max_iterations(
                MagicNumberMutator::new(),
                NonZero::new(256).unwrap()
            ),
        );

        let mut fuzzer = StdFuzzer::new(sched, corpus_feedback, crash_feedback);
        let mut mgr = SimpleEventManager::new(SimpleMonitor::new(|s| info!("{}", s)));

        info!("Adding initial input...");
        let initial_input = SuiFuzzInput::new();
        fuzzer.add_input(&mut state, &mut executor, &mut mgr, initial_input.clone())?;

        fuzzer.evaluate_input(&mut state, &mut executor, &mut mgr, &initial_input)?;

        if let Some(flash) = &self.flash {
            let flash_wrapper = FlashWrapper::from_str_with_store(flash, &db)?;
            let mut flash_seed = SuiFuzzInput::flash(flash_wrapper.clone())?;

            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    state
                        .fuzz_state_mut()
                        .analyze_object_types(&rpc, &executor.db)
                        .await?;
                    match flash_wrapper.provider {
                        FlashProvider::Cetus { package, .. } => {
                            state
                                .fuzz_state_mut()
                                .add_single_package(&rpc, package, &executor.db)
                                .await?;
                        }
                        FlashProvider::Nemo { package, .. } => {
                            state
                                .fuzz_state_mut()
                                .add_single_package(&rpc, package, &executor.db)
                                .await?;
                        }
                    }

                    Ok::<_, BelobogError>(())
                })
            })?;
            executor.disable_oracles();
            process_key_store(&mut flash_seed.ptb, &state, &executor.db);

            info!("We will add an input:\n{}", &flash_seed);
            let (result, _corpus_id) =
                fuzzer.evaluate_input(&mut state, &mut executor, &mut mgr, &flash_seed)?;

            if !result.is_corpus() || result.is_solution() {
                return Err(eyre!("flash input is unexpected with {:?}", result).into());
            }
            remove_process_key_store(&mut flash_seed.ptb);
            executor.enable_oracles();
        }

        if let Some(output) = &self.output {
            let metadata = output.join("meta.json");
            info!("Saving metadata...");
            let fp = std::fs::File::create(&metadata)?;
            serde_json::to_writer_pretty(fp, state.fuzz_state())?;
        }

        #[cfg(feature = "pprof")]
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(1000)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .unwrap();

        let start = std::time::SystemTime::now();
        let mut cycle = 1usize;
        loop {
            if let Some(limit) = self.time_limit {
                let current = std::time::SystemTime::now();

                let elapsed = current.duration_since(start).expect("non mono clock?!");
                if elapsed > Duration::from_secs(limit) {
                    break;
                }
            }

            if let Err(e) = fuzzer.fuzz_one(&mut stages, &mut executor, &mut state, &mut mgr) {
                warn!("Getting fuzz error: {:?}", e);
                break;
            }

            // Clear per-round execution outcome to avoid leaking stage indices into the next round.
            state.extra_state_mut().global_outcome = None;

            info!("Cycle {} done", cycle);
            if cycle % self.debug_eval_metrics_cycles == 0 {
                if let Some(metric) = state.eval_metrics_mut() {
                    metric.timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_millis() as u64;
                    if let Some(output) = &self.output {
                        info!("dumping intermediate evaluation metrics to jsonl...");
                        metric.append_jsonl(&output.join("eval.jsonl"));
                    }
                    metric.cycle += 1;
                }
            }
            cycle += 1;
            mgr.report_progress(&mut state)?;
        }

        #[cfg(feature = "pprof")]
        {
            let report = guard.report().build().expect("generate report");
            let file = std::fs::File::create("flamegraph.svg").unwrap();
            report.flamegraph(file).unwrap();
        }

        if let Some(metric) = state.eval_metrics() {
            if let Some(output) = &self.output {
                info!("dumping final evaluation metrics...");
                metric.dump(&output.join("eval.json"));
            }
        }

        Ok(())
    }
}
