use std::{collections::BTreeSet, marker::PhantomData};

use belobog_fork::ObjectTypesStore;
use belobog_types::error::BelobogError;
use libafl::executors::ExitKind;
use libafl_bolts::Named;
use move_core_types::language_storage::ModuleId;
use move_trace_format::format::TraceEvent;
use move_vm_stack::Stack;
use sui_types::storage::BackingStore;

use crate::{concolic::SymbolValue, meta::HasFuzzMetadata};

pub mod bool_judgement;
mod common;
pub mod infinite_loop;
pub mod overflow;
pub mod precision_loss;
pub mod proceeds;
pub mod type_conversion;
pub mod unchecked_return;
pub mod unused_const;
pub mod unused_private_fun;
pub mod unused_struct;

pub trait Oracle<S>: Named
where
    S: HasFuzzMetadata,
{
    fn event(
        &mut self,
        event: &TraceEvent,
        state: &mut S,
        stack: Option<&Stack>,
        symbol_stack: &Vec<SymbolValue>,
        current_function: Option<&(ModuleId, String)>,
    ) -> Result<(ExitKind, Vec<String>), BelobogError>;
    fn done_execution<T>(
        &mut self,
        db: &T,
        state: &mut S,
        stack: Option<&Stack>,
    ) -> Result<(ExitKind, Vec<String>), BelobogError>
    where
        T: BackingStore + ObjectTypesStore;
}

pub trait OracleTuple<S> {
    fn event_all(
        &mut self,
        event: &TraceEvent,
        state: &mut S,
        stack: Option<&Stack>,
        symbol_stack: &Vec<SymbolValue>,
        current_function: Option<&(ModuleId, String)>,
    ) -> Result<(ExitKind, BTreeSet<String>), BelobogError>;

    fn done_execution<T>(
        &mut self,
        db: &T,
        state: &mut S,
        stack: Option<&Stack>,
    ) -> Result<(ExitKind, BTreeSet<String>), BelobogError>
    where
        T: BackingStore + ObjectTypesStore;
}

impl<S> OracleTuple<S> for () {
    fn event_all(
        &mut self,
        _event: &TraceEvent,
        _state: &mut S,
        _stack: Option<&Stack>,
        _symbol_stack: &Vec<SymbolValue>,
        _current_function: Option<&(ModuleId, String)>,
    ) -> Result<(ExitKind, BTreeSet<String>), BelobogError> {
        Ok((ExitKind::Ok, BTreeSet::new()))
    }

    fn done_execution<T>(
        &mut self,
        _db: &T,
        _state: &mut S,
        _stack: Option<&Stack>,
    ) -> Result<(ExitKind, BTreeSet<String>), BelobogError>
    where
        T: ObjectTypesStore + BackingStore,
    {
        Ok((ExitKind::Ok, BTreeSet::new()))
    }
}

impl<S, L, R> OracleTuple<S> for (L, R)
where
    L: Oracle<S>,
    R: OracleTuple<S>,
    S: HasFuzzMetadata,
{
    fn event_all(
        &mut self,
        event: &TraceEvent,
        state: &mut S,
        stack: Option<&Stack>,
        symbol_stack: &Vec<SymbolValue>,
        current_function: Option<&(ModuleId, String)>,
    ) -> Result<(ExitKind, BTreeSet<String>), BelobogError> {
        let (lhs_exit, lhs_infos) =
            self.0
                .event(event, state, stack, symbol_stack, current_function)?;
        let mut aggregated = lhs_infos.into_iter().collect::<BTreeSet<_>>();
        let (rhs_exit, rhs_infos) =
            self.1
                .event_all(event, state, stack, symbol_stack, current_function)?;
        aggregated.extend(rhs_infos.into_iter());
        let exit = if lhs_exit == ExitKind::Crash {
            lhs_exit
        } else {
            rhs_exit
        };
        Ok((exit, aggregated))
    }

    fn done_execution<T>(
        &mut self,
        db: &T,
        state: &mut S,
        stack: Option<&Stack>,
    ) -> Result<(ExitKind, BTreeSet<String>), BelobogError>
    where
        T: BackingStore + ObjectTypesStore,
    {
        let (lhs_exit, lhs_infos) = self.0.done_execution(db, state, stack)?;
        let mut aggregated = lhs_infos.into_iter().collect::<BTreeSet<_>>();
        let (rhs_exit, rhs_infos) = self.1.done_execution(db, state, stack)?;
        aggregated.extend(rhs_infos.into_iter());
        let exit = if lhs_exit == ExitKind::Crash {
            lhs_exit
        } else {
            rhs_exit
        };
        Ok((exit, aggregated))
    }
}

pub struct CouldDisabledOralce<O, S> {
    pub oracle: O,
    pub disabled: bool,
    pub ph: PhantomData<S>,
}

impl<O, S> CouldDisabledOralce<O, S> {
    pub fn new(oracle: O, disabled: bool) -> Self {
        Self {
            oracle,
            disabled,
            ph: PhantomData,
        }
    }
}

impl<O, S> Named for CouldDisabledOralce<O, S>
where
    O: Oracle<S>,
    S: HasFuzzMetadata,
{
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        self.oracle.name()
    }
}
impl<O, S> Oracle<S> for CouldDisabledOralce<O, S>
where
    O: Oracle<S>,
    S: HasFuzzMetadata,
{
    fn done_execution<T>(
        &mut self,
        db: &T,
        state: &mut S,
        stack: Option<&Stack>,
    ) -> Result<(ExitKind, Vec<String>), BelobogError>
    where
        T: BackingStore + ObjectTypesStore,
    {
        if self.disabled {
            Ok((ExitKind::Ok, Vec::new()))
        } else {
            self.oracle.done_execution(db, state, stack)
        }
    }

    fn event(
        &mut self,
        event: &TraceEvent,
        state: &mut S,
        stack: Option<&Stack>,
        symbol_stack: &Vec<SymbolValue>,
        current_function: Option<&(ModuleId, String)>,
    ) -> Result<(ExitKind, Vec<String>), BelobogError> {
        if self.disabled {
            Ok((ExitKind::Ok, Vec::new()))
        } else {
            self.oracle
                .event(event, state, stack, symbol_stack, current_function)
        }
    }
}
