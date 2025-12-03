use std::{path::PathBuf, str::FromStr};

use belobog_types::error::BelobogError;
use clap::Args;
use color_eyre::eyre::eyre;
use libafl::{
    Evaluator, HasMetadata, StdFuzzer,
    corpus::{Corpus, InMemoryCorpus},
    events::SimpleEventManager,
    feedback_and_fast,
    feedbacks::{ConstFeedback, CrashFeedback, ExitKindFeedback},
    monitors::SimpleMonitor,
    schedulers::StdScheduler,
    state::{HasCorpus, HasSolutions, StdState},
};
use libafl_bolts::tuples::tuple_list;
use log::info;
use move_core_types::{
    annotated_value as A,
    runtime_value::{self as R, MoveStructLayout},
};
use serde::{Deserialize, Serialize};
use sui_json_rpc_types::{SuiMoveNormalizedStructType, SuiMoveNormalizedType};
use sui_sdk::json::MoveTypeLayout;
use sui_types::{base_types::ObjectID, object::Data, storage::ObjectStore};

use crate::{
    executor::SuiFuzzExecutor,
    input::{SuiFuzzInput, SuiInput},
    meta::{FuzzMetadata, HasFuzzMetadata},
    state::SuperState,
    utils::AppendOutcomeFeedback,
};

use super::fuzz::{FuzzArgs, OkFeedback};

#[derive(Args, Serialize, Deserialize, Debug, Clone)]
pub struct FuzzReplayArgs {
    #[arg(short, long)]
    pub config: PathBuf,
    #[arg(short, long)]
    pub seed: PathBuf,
}

pub fn annotated_to_runtime(annotated_ty: &A::MoveTypeLayout) -> R::MoveTypeLayout {
    match annotated_ty {
        A::MoveTypeLayout::Address => R::MoveTypeLayout::Address,
        A::MoveTypeLayout::U8 => R::MoveTypeLayout::U8,
        A::MoveTypeLayout::U16 => R::MoveTypeLayout::U16,
        A::MoveTypeLayout::U32 => R::MoveTypeLayout::U32,
        A::MoveTypeLayout::U64 => R::MoveTypeLayout::U64,
        A::MoveTypeLayout::U128 => R::MoveTypeLayout::U128,
        A::MoveTypeLayout::U256 => R::MoveTypeLayout::U256,
        A::MoveTypeLayout::Bool => R::MoveTypeLayout::Bool,
        A::MoveTypeLayout::Vector(inner) => {
            R::MoveTypeLayout::Vector(Box::new(annotated_to_runtime(inner)))
        }
        A::MoveTypeLayout::Struct(s) => R::MoveTypeLayout::Struct(Box::new(MoveStructLayout::new(
            s.fields
                .iter()
                .map(|arg0| annotated_to_runtime(&arg0.layout))
                .collect(),
        ))),
        A::MoveTypeLayout::Enum(e) => {
            todo!()
        }
        A::MoveTypeLayout::Signer => R::MoveTypeLayout::Signer,
    }
}

impl FuzzReplayArgs {
    pub fn run(self) -> Result<(), BelobogError> {
        let fp = std::fs::File::open(&self.config)?;
        let mut fuzz_args: FuzzArgs = serde_json::from_reader(fp)?;

        let fp = std::fs::File::open(&self.seed)?;
        let seed: SuiFuzzInput = serde_json::from_reader(fp)?;

        let prev_outcome = seed.outcome().clone().unwrap();

        let rpc = fuzz_args.rpc()?;
        let env = fuzz_args.env(&rpc)?;
        let db = fuzz_args.db(&env)?;

        let ob = fuzz_args.code_observer();
        let corpus = InMemoryCorpus::new();
        let crash = InMemoryCorpus::new();
        let mut corpus_feedback = feedback_and_fast!(
            ExitKindFeedback::<OkFeedback>::new(),
            AppendOutcomeFeedback {},
            ConstFeedback::new(true)
        );
        let mut crash_feedback = feedback_and_fast!(CrashFeedback::new(), AppendOutcomeFeedback {});

        let state = StdState::new(
            fuzz_args.rand(),
            corpus,
            crash,
            &mut corpus_feedback,
            &mut crash_feedback,
        )?;
        let mut state = SuperState::new(state);
        let fuzz_state = FuzzMetadata::default();
        state.add_metadata(fuzz_state);
        let mut executor = SuiFuzzExecutor::new(
            env,
            tuple_list!(ob),
            db,
            fuzz_args.attacker,
            fuzz_args.admin(),
            fuzz_args.oracles(),
            &mut state,
        )?;

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                fuzz_args
                    .source
                    .setup(&mut state, &rpc, &mut executor)
                    .await?;

                Ok::<_, BelobogError>(())
            })
        })?;
        state.fuzz_state_mut().initialize(
            executor.minted_gas.id(),
            fuzz_args.privilege_functions.clone(),
        );

        let mut mgr = SimpleEventManager::new(SimpleMonitor::new(|s| info!("{}", s)));
        let sched = StdScheduler::new(); // TODO: !
        let mut fuzzer = StdFuzzer::new(sched, corpus_feedback, crash_feedback);
        info!("Seed to execute is:\n{}", &seed);
        let (id, exec_result) = fuzzer.add_input(&mut state, &mut executor, &mut mgr, seed)?;

        info!("Result is {:?}", &exec_result);
        let case = if let Ok(case) = state.corpus().get(id) {
            info!("The seed was found in corpus");
            case
        } else {
            info!("The seed was found in solutions");
            state.solutions().get(id).expect("also not in corpus?!")
        };

        let input_borrow = case.borrow();
        let input = input_borrow.input().as_ref().unwrap();
        let outcome = input.outcome().as_ref().unwrap();
        if outcome != &prev_outcome {
            info!("We have different outcome");
            info!(
                "Previous outcome:\n{}\nCurrent outcome:\n{}",
                prev_outcome.format(state.fuzz_state()),
                outcome.format(state.fuzz_state())
            );
        } else {
            info!(
                "The replayed outcome is:\n{}",
                outcome.format(state.fuzz_state())
            );
        }

        Ok(())
    }
}
