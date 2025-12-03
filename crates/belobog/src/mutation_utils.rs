use itertools::Itertools;
use libafl::{
    Error, HasMetadata,
    inputs::{HasMutatorBytes, Input},
    mutators::{DwordInterestingMutator, HavocScheduledMutator, MutationResult, Mutator},
    state::HasRand,
};
use libafl_bolts::{HasLen, Named, rands::Rand, tuples::tuple_list};
use move_vm_types::values::{Container, ContainerRef, Value, ValueImpl};
use std::{collections::BTreeSet, ops::Deref};

use crate::{r#const::MAX_STACK_POW, meta::HasCaller};

/// [`MagicNumberMutator`] is a mutator that mutates the input to a constant
/// in the contract
///
/// We discover that sometimes directly setting the bytes to the constants allow
/// us to increase test coverage.
#[derive(Default)]
pub struct MagicNumberMutator {
    magic_number_pool: Vec<Vec<u8>>,
}

impl Named for MagicNumberMutator {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        &std::borrow::Cow::Borrowed("constant_hinted_mutator")
    }
}

impl MagicNumberMutator {
    pub fn new(magic_number_pool: BTreeSet<Vec<u8>>) -> Self {
        Self {
            magic_number_pool: magic_number_pool.into_iter().collect(),
        }
    }
}

impl<I, S> Mutator<I, S> for MagicNumberMutator
where
    S: HasRand,
    I: HasMutatorBytes,
{
    /// Mutate the input to a magic number in the contract
    /// This always entirely overwrites the input (unless it skips mutation)
    fn mutate(&mut self, state: &mut S, input: &mut I) -> Result<MutationResult, Error> {
        if self.magic_number_pool.is_empty() {
            return Ok(MutationResult::Skipped);
        }
        let input_bytes = input.mutator_bytes_mut();
        let input_len = input_bytes.len();
        let fit_pool = self
            .magic_number_pool
            .iter()
            .filter(|x| x.len() == input_len)
            .collect_vec();
        if fit_pool.is_empty() {
            return Ok(MutationResult::Skipped);
        }
        let magic_number = fit_pool[state.rand_mut().below_or_zero(fit_pool.len())].clone();

        let magic_number_len = magic_number.len();

        if input_len < magic_number_len {
            input_bytes.copy_from_slice(&magic_number[0..input_len]);
        } else {
            input_bytes.copy_from_slice(
                &[vec![0; input_len - magic_number_len], magic_number.clone()].concat(),
            );
        }

        Ok(MutationResult::Mutated)
    }

    fn post_exec(
        &mut self,
        _state: &mut S,
        _new_corpus_id: Option<libafl::corpus::CorpusId>,
    ) -> Result<(), libafl::Error> {
        Ok(())
    }
}

pub struct MutableValue {
    pub value: Value,
    pub bytes: Vec<u8>,
}

impl HasLen for MutableValue {
    fn len(&self) -> usize {
        self.bytes.len()
    }
}

impl HasMutatorBytes for MutableValue {
    fn mutator_bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn mutator_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

impl MutableValue {
    pub fn new(value: Value) -> Self {
        let bytes = vec![];

        MutableValue { value, bytes }
    }

    fn sync(&mut self) {
        macro_rules! leb_tns {
            ($v: expr) => {{
                self.bytes = &$v.to_le_bytes().to_vec();
            }};
            (vec, $v: expr) => {{
                self.bytes = $v
                    .borrow()
                    .deref()
                    .iter()
                    .map(|x| x.to_le_bytes())
                    .flatten()
                    .collect::<Vec<u8>>();
            }};
        }
        match &self.value.0 {
            ValueImpl::Container(v) => {
                match v {
                    Container::VecU8(v) => {
                        // debug!("{:?}", v.borrow().deref());
                        leb_tns!(vec, v)
                    }
                    Container::VecU64(v) => leb_tns!(vec, v),
                    Container::VecU128(v) => leb_tns!(vec, v),
                    Container::VecU16(v) => leb_tns!(vec, v),
                    Container::VecU32(v) => leb_tns!(vec, v),
                    Container::VecU256(v) => leb_tns!(vec, v),
                    // cant be mutated
                    _ => unreachable!(),
                }
            }
            ValueImpl::U128(v) => {
                self.bytes = v.to_le_bytes().to_vec();
            }
            ValueImpl::U256(v) => {
                self.bytes = v.to_le_bytes().to_vec();
            }
            ValueImpl::U64(v) => {
                self.bytes = v.to_le_bytes().to_vec();
            }
            ValueImpl::U32(v) => {
                self.bytes = v.to_le_bytes().to_vec();
            }
            ValueImpl::U16(v) => {
                self.bytes = v.to_le_bytes().to_vec();
            }
            ValueImpl::U8(v) => {
                self.bytes = v.to_le_bytes().to_vec();
            }
            // ValueImpl::ContainerRef(_) => {}
            // ValueImpl::IndexedRef(_) => {}
            _ => unreachable!("type: {:?}", self.value),
        }
    }

    fn commit(&mut self) {
        macro_rules! from_le {
            ($ty: ty, $v: expr) => {{
                let num_bytes = (<$ty>::BITS / 8) as usize;
                let vc = self
                    .bytes
                    .chunks(num_bytes)
                    .filter(|x| x.len() == num_bytes)
                    .map(|x| <$ty>::from_le_bytes(x.try_into().unwrap()))
                    .collect_vec();
                (*(*$v)).borrow_mut().copy_from_slice(vc.as_slice());
            }};

            (u256, $ty: ty, $v: expr) => {{
                let num_bytes = 32;
                let vc = self
                    .bytes
                    .chunks(num_bytes)
                    .filter(|x| x.len() == num_bytes)
                    .map(|x| <$ty>::from_le_bytes(x.try_into().unwrap()))
                    .collect_vec();
                (*(*$v)).borrow_mut().copy_from_slice(vc.as_slice());
            }};
        }
        match &mut self.value.0 {
            ValueImpl::Container(v) => match v {
                Container::VecU8(v) => {
                    from_le!(u8, v);
                }
                Container::VecU64(v) => {
                    from_le!(u64, v)
                }
                Container::VecU128(v) => {
                    from_le!(u128, v)
                }
                Container::VecU16(v) => {
                    from_le!(u16, v)
                }
                Container::VecU32(v) => {
                    from_le!(u32, v)
                }
                Container::VecU256(v) => {
                    from_le!(u256, move_core_types::u256::U256, v)
                }
                _ => unreachable!(),
            },
            ValueImpl::U128(v) => {
                **v = u128::from_le_bytes(self.bytes.as_slice().try_into().unwrap())
            }
            ValueImpl::U64(v) => *v = u64::from_le_bytes(self.bytes.as_slice().try_into().unwrap()),
            ValueImpl::U32(v) => *v = u32::from_le_bytes(self.bytes.as_slice().try_into().unwrap()),
            ValueImpl::U16(v) => *v = u16::from_le_bytes(self.bytes.as_slice().try_into().unwrap()),
            ValueImpl::U8(v) => *v = u8::from_le_bytes(self.bytes.as_slice().try_into().unwrap()),
            ValueImpl::U256(v) => {
                **v = move_core_types::u256::U256::from_le_bytes(
                    self.bytes.as_slice().try_into().unwrap(),
                )
            }
            _ => unreachable!(),
        };

        self.bytes.clear();
    }

    /// Mutator that mutates the `CONSTANT SIZE` input bytes (e.g., uint256) in
    /// various ways provided by [`libafl::mutators`]. It also uses the
    /// [`ConstantHintedMutator`] and [`VMStateHintedMutator`]
    fn mutate_by(
        &mut self,
        state: &mut impl HasRand,
        magic_number_pool: &BTreeSet<Vec<u8>>,
        split: bool,
    ) -> MutationResult {
        self.sync();

        let mutations = tuple_list!(MagicNumberMutator::new(magic_number_pool.clone()),);

        let mut mutator = HavocScheduledMutator::with_max_stack_pow(mutations, MAX_STACK_POW);
        let mut res = mutator.mutate(state, self).unwrap();

        if self.bytes.len() == 4 {
            if state.rand_mut().below_or_zero(10) > 0 {
                self.bytes = (state.rand_mut().below_or_zero(443636) as u32)
                    .to_le_bytes()
                    .to_vec();
                res = MutationResult::Mutated;
            }
            // let mut i = 0x10000u32;
            // if state.rand_mut().below_or_zero(2) == 0 {
            //     i -= 0x1000;
            //     self.bytes = i.to_le_bytes().to_vec();
            // } else {
            //     i += 0x1000;
            //     self.bytes = i.to_le_bytes().to_vec();
            // }
        } else if self.bytes.len() == 8 {
            if state.rand_mut().below_or_zero(10) > 5 {
                self.bytes = (1u64 << (state.rand_mut().below_or_zero(64) as u32))
                    .to_le_bytes()
                    .to_vec();
                res = MutationResult::Mutated;
            }
        } else if self.bytes.len() == 16 {
            if state.rand_mut().below_or_zero(10) > 0 {
                self.bytes = (1u128 << (state.rand_mut().below_or_zero(128) as u32))
                    .to_le_bytes()
                    .to_vec();
                res = MutationResult::Mutated;
            }
        } else if split && magic_number_pool.is_empty() {
            self.bytes = 0u64.to_le_bytes().to_vec();
            res = MutationResult::Mutated;
        } else {
            let mutations = tuple_list!(DwordInterestingMutator::new());

            let mut mutator = HavocScheduledMutator::with_max_stack_pow(mutations, MAX_STACK_POW);
            res = mutator.mutate(state, self).unwrap();
        }

        self.commit();
        res
    }

    pub fn mutate<S>(
        &mut self,
        state: &mut S,
        magic_number_pool: &BTreeSet<Vec<u8>>,
        split: bool,
    ) -> MutationResult
    where
        S: HasRand + HasCaller,
    {
        macro_rules! mutate_u {
            ($ty: ty, $v: expr) => {{
                let orig = *$v;
                while *$v == orig {
                    *$v = state.rand_mut().below_or_zero(<$ty>::MAX as usize) as $ty;
                    // debug!("mutate_u: {} {}", $v, orig);
                }
                MutationResult::Mutated
            }};
        }

        enum MutateType<'a> {
            Container(&'a mut Container),
            Indexed(&'a mut Container, usize),
        }

        let further_mutation = match self.value.0 {
            ValueImpl::Invalid => {
                unreachable!()
            }
            // value level mutation
            ValueImpl::Bool(ref mut v) => {
                *v = state.rand_mut().below_or_zero(2) == 1;
                return MutationResult::Mutated;
            }
            ValueImpl::U8(ref mut v) => {
                return mutate_u!(u8, v);
            }
            ValueImpl::U16(_)
            | ValueImpl::U32(_)
            | ValueImpl::U64(_)
            | ValueImpl::U128(_)
            | ValueImpl::U256(_) => {
                return self.mutate_by(state, magic_number_pool, split);
            }
            ValueImpl::Address(ref mut v) => {
                if state.rand_mut().below_or_zero(2) == 1 {
                    **v = state.get_rand_address().into();
                } else {
                    **v = state.get_rand_caller().into();
                }
                return MutationResult::Mutated;
            }
            ValueImpl::Container(ref mut cont) => MutateType::Container(cont),
            ValueImpl::ContainerRef(ref mut cont) => match **cont {
                ContainerRef::Local(ref mut v) => MutateType::Container(v),
                ContainerRef::Global { .. } => {
                    unreachable!("global cant be mutated")
                }
            },
            ValueImpl::IndexedRef(ref mut cont) => {
                match &mut cont.container_ref {
                    ContainerRef::Local(vec_container) => {
                        // MutateType::Indexed(v, cont.idx)
                        MutateType::Indexed(vec_container, cont.idx)
                    }
                    ContainerRef::Global { .. } => {
                        unreachable!("global cant be mutated")
                    }
                }
            }
        };

        match further_mutation {
            MutateType::Container(cont) => {
                match cont {
                    Container::Struct(s) | Container::Locals(s) | Container::Vec(s) => {
                        let fields = std::mem::take(&mut *s.borrow_mut());
                        let mut mutated_fields = vec![];
                        let mut result = MutationResult::Skipped;
                        for field in fields {
                            let mut mutable_value = MutableValue {
                                value: Value(field),
                                bytes: vec![],
                            };
                            let res = mutable_value.mutate(state, magic_number_pool, split);
                            mutated_fields.push(mutable_value.value.0);
                            if res == MutationResult::Mutated {
                                result = MutationResult::Mutated;
                            }
                        }
                        *s.borrow_mut() = mutated_fields;
                        result
                    }
                    Container::VecU8(_)
                    | Container::VecU16(_)
                    | Container::VecU32(_)
                    | Container::VecU64(_)
                    | Container::VecU128(_)
                    | Container::VecU256(_) => self.mutate_by(state, magic_number_pool, split),
                    Container::VecBool(v) => {
                        v.borrow_mut().iter_mut().for_each(|b| {
                            if state.rand_mut().below_or_zero(2) == 1 {
                                *b = !*b;
                            } else {
                                *b = state.rand_mut().below_or_zero(2) == 1;
                            }
                        });
                        MutationResult::Mutated
                    }
                    Container::VecAddress(v) => {
                        for address in v.borrow_mut().iter_mut() {
                            *address = state.get_rand_caller().into();
                        }
                        MutationResult::Mutated
                    }
                    Container::Variant(_) => {
                        // Variants are not supported for mutation
                        unreachable!("Variants are not supported for mutation");
                    }
                }
            }
            MutateType::Indexed(vec_container, index) => match vec_container {
                Container::Vec(inner_vec) => {
                    let mut mutable_value = MutableValue {
                        value: Value(inner_vec.borrow_mut().remove(index)),
                        bytes: vec![],
                    };
                    let res = mutable_value.mutate(state, magic_number_pool, split);
                    inner_vec.borrow_mut().insert(index, mutable_value.value.0);
                    res
                }
                _ => unreachable!("Unsupported container type for indexed mutation"),
            },
        }
    }

    pub fn sample_magic_number<S>(
        &mut self,
        state: &mut S,
        magic_number_pool: &BTreeSet<Vec<u8>>,
    ) -> MutationResult
    where
        S: HasRand + HasCaller,
    {
        match self.value.0 {
            ValueImpl::Invalid => {
                unreachable!()
            }
            ValueImpl::U8(_)
            | ValueImpl::U16(_)
            | ValueImpl::U32(_)
            | ValueImpl::U64(_)
            | ValueImpl::U128(_)
            | ValueImpl::U256(_)
            | ValueImpl::Container(Container::VecU8(_))
            | ValueImpl::Container(Container::VecU16(_))
            | ValueImpl::Container(Container::VecU32(_))
            | ValueImpl::Container(Container::VecU64(_))
            | ValueImpl::Container(Container::VecU128(_))
            | ValueImpl::Container(Container::VecU256(_)) => {
                self.sync();
                let fit_values = magic_number_pool
                    .iter()
                    .filter(|num| num.len() == self.bytes.len())
                    .collect::<Vec<_>>();
                if fit_values.is_empty() {
                    return MutationResult::Skipped;
                }
                self.bytes = state.rand_mut().choose(fit_values).unwrap().clone();
                self.commit();
                MutationResult::Mutated
            }
            _ => MutationResult::Skipped,
        }
    }
}
