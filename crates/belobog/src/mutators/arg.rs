use std::marker::PhantomData;

use libafl::{
    HasMetadata,
    mutators::{MutationResult, Mutator},
    state::HasRand,
};
use libafl_bolts::Named;
use sui_types::transaction::ProgrammableTransaction;

use crate::{
    flash::FlashProvider,
    input::SuiInput,
    meta::HasFuzzMetadata,
    mutators::{HasFlash, mutate_arg},
    state::HasExtraState,
};

pub struct ArgMutator<I, S> {
    pub ph: PhantomData<(I, S)>,
    pub flash: Option<FlashProvider>,
    pub prev: Option<ProgrammableTransaction>,
    pub times: u64,
}

impl<I, S> Default for ArgMutator<I, S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, S> ArgMutator<I, S> {
    pub fn new() -> Self {
        Self {
            ph: PhantomData,
            flash: None,
            prev: None,
            times: 0,
        }
    }
}

impl<I, S> Named for ArgMutator<I, S> {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("arg_mutator")
    }
}

impl<I, S> HasFlash for ArgMutator<I, S> {
    fn flash(&self) -> &Option<FlashProvider> {
        &self.flash
    }
}

impl<I, S> Mutator<I, S> for ArgMutator<I, S>
where
    I: SuiInput,
    S: HasFuzzMetadata + HasRand + HasMetadata + HasExtraState,
{
    fn mutate(&mut self, state: &mut S, input: &mut I) -> Result<MutationResult, libafl::Error> {
        self.flash = input.flash().as_ref().map(|f| f.provider.clone());
        if let Some(ex) = state
            .extra_state()
            .global_outcome
            .as_ref()
            .map(|o| &o.extra)
        {
            input.update_magic_number(ex);

            self.times += 1;
            if !ex.success
                && self.times % 1000 != 0
                && let Some(prev) = &self.prev
            {
                *input.ptb_mut() = prev.clone();
            }
        }

        let res = mutate_arg(self, state, input, false, &None);
        self.prev = Some(input.ptb().clone());
        *input.outcome_mut() = None;
        Ok(res)
    }

    fn post_exec(
        &mut self,
        _state: &mut S,
        _new_corpus_id: Option<libafl::corpus::CorpusId>,
    ) -> Result<(), libafl::Error> {
        Ok(())
    }
}
