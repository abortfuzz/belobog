use libafl::{
    HasMetadata, HasNamedMetadata,
    corpus::{HasCurrentCorpusId, HasTestcase},
    state::{
        HasCorpus, HasCurrentStageId, HasExecutions, HasImported, HasLastFoundTime,
        HasLastReportTime, HasRand, HasSolutions, Stoppable,
    },
};

use crate::executor::{GlobalOutcome, MutatorOutcome};

#[derive(Debug, Clone, Default)]
pub struct ExtraNonSerdeFuzzState {
    pub global_outcome: Option<GlobalOutcome>,
    pub mutator_outcome: MutatorOutcome,
}

pub trait HasExtraState {
    fn extra_state(&self) -> &ExtraNonSerdeFuzzState;
    fn extra_state_mut(&mut self) -> &mut ExtraNonSerdeFuzzState;
}

pub struct SuperState<S> {
    pub state: S,
    pub extra: ExtraNonSerdeFuzzState,
}

impl<S> SuperState<S> {
    pub fn new(state: S) -> Self {
        Self {
            state,
            extra: ExtraNonSerdeFuzzState::default(),
        }
    }
}

impl<S> HasExtraState for SuperState<S> {
    fn extra_state(&self) -> &ExtraNonSerdeFuzzState {
        &self.extra
    }

    fn extra_state_mut(&mut self) -> &mut ExtraNonSerdeFuzzState {
        &mut self.extra
    }
}

impl<S: HasMetadata> HasMetadata for SuperState<S> {
    fn metadata_map(&self) -> &libafl_bolts::serdeany::SerdeAnyMap {
        self.state.metadata_map()
    }

    fn metadata_map_mut(&mut self) -> &mut libafl_bolts::serdeany::SerdeAnyMap {
        self.state.metadata_map_mut()
    }
}

impl<S: HasNamedMetadata> HasNamedMetadata for SuperState<S> {
    fn named_metadata_map(&self) -> &libafl_bolts::serdeany::NamedSerdeAnyMap {
        self.state.named_metadata_map()
    }
    fn named_metadata_map_mut(&mut self) -> &mut libafl_bolts::serdeany::NamedSerdeAnyMap {
        self.state.named_metadata_map_mut()
    }
}

impl<S: HasRand> HasRand for SuperState<S> {
    type Rand = S::Rand;
    fn rand_mut(&mut self) -> &mut Self::Rand {
        self.state.rand_mut()
    }

    fn rand(&self) -> &Self::Rand {
        self.state.rand()
    }
}

impl<S: HasExecutions> HasExecutions for SuperState<S> {
    fn executions(&self) -> &u64 {
        self.state.executions()
    }

    fn executions_mut(&mut self) -> &mut u64 {
        self.state.executions_mut()
    }
}

impl<S: HasCorpus<I>, I> HasCorpus<I> for SuperState<S> {
    type Corpus = S::Corpus;

    fn corpus(&self) -> &Self::Corpus {
        self.state.corpus()
    }
    fn corpus_mut(&mut self) -> &mut Self::Corpus {
        self.state.corpus_mut()
    }
}

impl<S: HasSolutions<I>, I> HasSolutions<I> for SuperState<S> {
    type Solutions = S::Solutions;

    fn solutions(&self) -> &Self::Solutions {
        self.state.solutions()
    }

    fn solutions_mut(&mut self) -> &mut Self::Solutions {
        self.state.solutions_mut()
    }
}

impl<S: HasLastFoundTime> HasLastFoundTime for SuperState<S> {
    fn last_found_time(&self) -> &std::time::Duration {
        self.state.last_found_time()
    }
    fn last_found_time_mut(&mut self) -> &mut std::time::Duration {
        self.state.last_found_time_mut()
    }
}

impl<S: Stoppable> Stoppable for SuperState<S> {
    fn stop_requested(&self) -> bool {
        self.state.stop_requested()
    }
    fn discard_stop_request(&mut self) {
        self.state.discard_stop_request();
    }

    fn request_stop(&mut self) {
        self.state.request_stop();
    }
}

impl<S: HasTestcase<I>, I> HasTestcase<I> for SuperState<S> {
    fn testcase(
        &self,
        id: libafl::corpus::CorpusId,
    ) -> Result<std::cell::Ref<libafl::corpus::Testcase<I>>, libafl::Error> {
        self.state.testcase(id)
    }

    fn testcase_mut(
        &self,
        id: libafl::corpus::CorpusId,
    ) -> Result<std::cell::RefMut<libafl::corpus::Testcase<I>>, libafl::Error> {
        self.state.testcase_mut(id)
    }
}

impl<S: HasCurrentCorpusId> HasCurrentCorpusId for SuperState<S> {
    fn clear_corpus_id(&mut self) -> Result<(), libafl::Error> {
        self.state.clear_corpus_id()
    }

    fn current_corpus_id(&self) -> Result<Option<libafl::corpus::CorpusId>, libafl::Error> {
        self.state.current_corpus_id()
    }

    fn set_corpus_id(&mut self, id: libafl::corpus::CorpusId) -> Result<(), libafl::Error> {
        self.state.set_corpus_id(id)
    }
}

impl<S: HasLastReportTime> HasLastReportTime for SuperState<S> {
    fn last_report_time(&self) -> &Option<std::time::Duration> {
        self.state.last_report_time()
    }
    fn last_report_time_mut(&mut self) -> &mut Option<std::time::Duration> {
        self.state.last_report_time_mut()
    }
}

impl<S: HasImported> HasImported for SuperState<S> {
    fn imported(&self) -> &usize {
        self.state.imported()
    }
    fn imported_mut(&mut self) -> &mut usize {
        self.state.imported_mut()
    }
}

impl<S: HasCurrentStageId> HasCurrentStageId for SuperState<S> {
    fn clear_stage_id(&mut self) -> Result<(), libafl::Error> {
        self.state.clear_stage_id()
    }
    fn on_restart(&mut self) -> Result<(), libafl::Error> {
        self.state.on_restart()
    }
    fn current_stage_id(&self) -> Result<Option<libafl::stages::StageId>, libafl::Error> {
        self.state.current_stage_id()
    }
    fn set_current_stage_id(&mut self, id: libafl::stages::StageId) -> Result<(), libafl::Error> {
        self.state.set_current_stage_id(id)
    }
}
