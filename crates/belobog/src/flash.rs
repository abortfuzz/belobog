use std::{fmt::Display, ops::Deref, str::FromStr};

use belobog_types::error::BelobogError;
use color_eyre::eyre::{OptionExt, eyre};
use itertools::Itertools;
use log::debug;
use move_core_types::language_storage::StructTag;
use serde::{Deserialize, Serialize};
use sui_types::{
    Identifier, SUI_CLOCK_OBJECT_ID, TypeTag,
    base_types::ObjectID,
    execution_status::MoveLocation,
    object::{Data, Object, Owner},
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    storage::BackingStore,
    transaction::{Argument, Command, ObjectArg, ProgrammableTransaction},
};

use crate::mutators::movecall_is_split;

// TODO: Deepbook v3
#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct FlashWrapper {
    pub provider: FlashProvider,
    pub flash_coin: StructTag,
    pub initial_flash_amount: u64,
}

impl Display for FlashWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Flash(source={}, coin={}, initial_amount={})",
            &self.provider,
            self.flash_coin.to_canonical_string(true),
            self.initial_flash_amount
        ))
    }
}

impl FlashWrapper {
    pub fn from_str_with_store<T: BackingStore>(s: &str, store: &T) -> Result<Self, BelobogError> {
        let mut toks = s.split(",").collect_vec();
        if toks.is_empty() {
            return Err(eyre!("nothing for flash?!").into());
        }

        match toks[0].to_lowercase().as_str() {
            "cetus" => {
                if toks.len() != 6 {
                    return Err(eyre!("cetus expected: cetus,<package>,<global_config>,<pool>,<coin>,<amount> but get {}", s).into());
                }
                let amount = u64::from_str(toks.pop().unwrap())
                    .map_err(|e| eyre!("can not parse flash amount {}", e))?;
                let coin = StructTag::from_str(toks.pop().unwrap())?;
                let pool = ObjectID::from_str(toks.pop().unwrap())?;
                let config = ObjectID::from_str(toks.pop().unwrap())?;
                let package = ObjectID::from_str(toks.pop().unwrap())?;

                Ok(Self {
                    provider: FlashProvider::cetus_from_store(package, config, pool, store)?,
                    flash_coin: coin,
                    initial_flash_amount: amount,
                })
            }
            "nemo" => {
                if toks.len() != 6 {
                    return Err(eyre!("nemo expected: nemo,<package>,<version>,<py_state>,<coin>,<amount> but get {}", s).into());
                }
                let amount = u64::from_str(toks.pop().unwrap())
                    .map_err(|e| eyre!("can not parse flash amount {}", e))?;
                let coin = StructTag::from_str(toks.pop().unwrap())?;
                let py_state = ObjectID::from_str(toks.pop().unwrap())?;
                let version = ObjectID::from_str(toks.pop().unwrap())?;
                let package = ObjectID::from_str(toks.pop().unwrap())?;

                Ok(Self {
                    provider: FlashProvider::nemo_from_store(package, version, py_state, store)?,
                    flash_coin: coin,
                    initial_flash_amount: amount,
                })
            }
            _ => Err(eyre!("unknown flash source {}", toks[0]).into()),
        }
    }
}

impl Deref for FlashWrapper {
    type Target = FlashProvider;
    fn deref(&self) -> &Self::Target {
        &self.provider
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum FlashProvider {
    Cetus {
        package: ObjectID,
        coin_a: TypeTag,
        coin_b: TypeTag,
        global_config: ObjectArg,
        pool: ObjectArg,
        clock: ObjectArg,
    },
    Nemo {
        package: ObjectID,
        coin: TypeTag,
        version: ObjectArg,
        py_state: ObjectArg,
        clock: ObjectArg,
    },
}

impl Display for FlashProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cetus {
                package,
                coin_a,
                coin_b,
                global_config,
                pool,
                clock: _,
            } => f.write_fmt(format_args!(
                "Cetus(package={}, coins={}/{}, config={}, pool={})",
                package,
                coin_a.to_canonical_string(true),
                coin_b.to_canonical_string(true),
                global_config.id(),
                pool.id()
            )),
            Self::Nemo {
                package,
                coin,
                version,
                py_state,
                clock: _,
            } => f.write_fmt(format_args!(
                "Nemo(package={}, coin={}, version={}, py_state={})",
                package,
                coin.to_canonical_string(true),
                version.id(),
                py_state.id()
            )),
        }
    }
}

impl FlashProvider {
    pub fn cetus_from_store<T: BackingStore>(
        package: ObjectID,
        global_config: ObjectID,
        pool: ObjectID,
        store: &T,
    ) -> Result<Self, BelobogError> {
        let pool = store
            .get_object(&pool)
            .ok_or_eyre(eyre!("pool {} not found", &pool))?;
        let clock = store
            .get_object(&SUI_CLOCK_OBJECT_ID)
            .ok_or_eyre(eyre!("no clock?!"))?;
        let global_config = store
            .get_object(&global_config)
            .ok_or_eyre(eyre!("no global config {}", global_config))?;
        match &pool.data {
            Data::Move(obj) => {
                let mut params = obj.type_().type_params();
                let coin_b = params
                    .pop()
                    .ok_or_eyre(eyre!("{} no a cetus pool", pool.id()))?;
                let coin_a = params
                    .pop()
                    .ok_or_eyre(eyre!("{} no a cetus pool", pool.id()))?;
                Ok(Self::Cetus {
                    package,
                    coin_a,
                    coin_b,
                    global_config: object_to_object_arg(&global_config, false)
                        .ok_or_eyre(eyre!("fail to convert to object arg?!"))?,
                    pool: object_to_object_arg(&pool, true)
                        .ok_or_eyre(eyre!("fail to convert to object arg?!"))?,
                    clock: object_to_object_arg(&clock, false)
                        .ok_or_eyre(eyre!("fail to convert to object arg?!"))?,
                })
            }
            _ => Err(eyre!("{} no an object", pool.id()).into()),
        }
    }

    pub fn nemo_from_store<T: BackingStore>(
        package: ObjectID,
        version: ObjectID,
        py_state: ObjectID,
        store: &T,
    ) -> Result<Self, BelobogError> {
        let version = store
            .get_object(&version)
            .ok_or_eyre(eyre!("no nemo version {}", version))?;
        let py_state = store
            .get_object(&py_state)
            .ok_or_eyre(eyre!("no nemo py_state {}", py_state))?;
        let clock = store
            .get_object(&SUI_CLOCK_OBJECT_ID)
            .ok_or_eyre(eyre!("no clock?!"))?;
        match &py_state.data {
            Data::Move(obj) => {
                let mut params = obj.type_().type_params();
                let coin = params
                    .pop()
                    .ok_or_eyre(eyre!("{} no a nemo py_state", py_state.id()))?;
                return Ok(Self::Nemo {
                    package,
                    coin,
                    version: object_to_object_arg(&version, false)
                        .ok_or_eyre(eyre!("fail to convert to object arg?!"))?,
                    py_state: object_to_object_arg(&py_state, true)
                        .ok_or_eyre(eyre!("fail to convert to object arg?!"))?,
                    clock: object_to_object_arg(&clock, false)
                        .ok_or_eyre(eyre!("fail to convert to object arg?!"))?,
                });
            }
            _ => return Err(eyre!("{} no an object", py_state.id()).into()),
        }
    }

    pub fn repay_split_coin0<'b>(
        &self,
        ptb: &'b mut ProgrammableTransaction,
    ) -> Option<&'b mut Argument> {
        let ptb_cmd_len = ptb.commands.len();
        match self {
            Self::Cetus {
                package: _,
                coin_a: _,
                coin_b: _,
                global_config: _,
                pool: _,
                clock: _,
            } => ptb.commands.get_mut(ptb_cmd_len - 3).and_then(|v| match v {
                Command::MoveCall(mc) => mc.arguments.get_mut(0),
                _ => None,
            }),
            Self::Nemo { .. } => None,
        }
    }

    pub fn repay_split_coin1<'b>(
        &self,
        ptb: &'b mut ProgrammableTransaction,
    ) -> Option<&'b mut Argument> {
        let ptb_cmd_len = ptb.commands.len();
        match self {
            Self::Cetus {
                package: _,
                coin_a: _,
                coin_b: _,
                global_config: _,
                pool: _,
                clock: _,
            } => ptb.commands.get_mut(ptb_cmd_len - 2).and_then(|v| match v {
                Command::MoveCall(mc) => mc.arguments.get_mut(0),
                _ => None,
            }),
            Self::Nemo { .. } => None,
        }
    }

    pub fn repay_coin0<'b>(
        &self,
        ptb: &'b mut ProgrammableTransaction,
    ) -> Option<&'b mut Argument> {
        match self {
            Self::Cetus {
                package: _,
                coin_a: _,
                coin_b: _,
                global_config: _,
                pool: _,
                clock: _,
            } => ptb.commands.last_mut().and_then(|v| match v {
                Command::MoveCall(mc) => mc.arguments.get_mut(2),
                _ => None,
            }),
            Self::Nemo { .. } => None,
        }
    }

    pub fn flash_amount<'b>(
        &self,
        ptb: &'b mut ProgrammableTransaction,
    ) -> Option<&'b mut Argument> {
        match self {
            Self::Cetus {
                package: _,
                coin_a: _,
                coin_b: _,
                global_config: _,
                pool: _,
                clock: _,
            } => ptb.commands.first_mut().and_then(|v| match v {
                Command::MoveCall(mc) => mc.arguments.get_mut(4),
                _ => None,
            }),
            Self::Nemo { .. } => None,
        }
    }

    pub fn repay_coin1<'b>(
        &self,
        ptb: &'b mut ProgrammableTransaction,
    ) -> Option<&'b mut Argument> {
        match self {
            Self::Cetus {
                package: _,
                coin_a: _,
                coin_b: _,
                global_config: _,
                pool: _,
                clock: _,
            } => ptb.commands.last_mut().and_then(|v| match v {
                Command::MoveCall(mc) => mc.arguments.get_mut(3),
                _ => None,
            }),
            Self::Nemo { .. } => None,
        }
    }

    pub fn allowed_abort(
        &self,
        loc: &MoveLocation,
        command: &Option<usize>,
        ptb: &ProgrammableTransaction,
    ) -> bool {
        match self {
            Self::Cetus { .. } => {
                if &loc.module.name().to_string() == "pool"
                    && loc
                        .function_name
                        .as_ref()
                        .map(|n| n == "repay_flash_loan")
                        .unwrap_or_default()
                {
                    return true;
                }
                if &loc.module.name().to_string() == "balance"
                    && loc
                        .function_name
                        .as_ref()
                        .map(|n| n == "split")
                        .unwrap_or_default()
                {
                    if let Some(idx) = command {
                        for cmd in ptb.commands.iter().skip(idx + 1) {
                            if let Command::MoveCall(mc) = cmd
                                && !movecall_is_split(mc)
                            {
                                return mc.module == "pool" && mc.function == "repay_flash_loan";
                            }
                        }
                        return false;
                    }
                    return true;
                }
                false
            }
            Self::Nemo { .. } => {
                if &loc.module.name().to_string() == "py"
                    && loc
                        .function_name
                        .as_ref()
                        .map(|n| n == "repay_pt_amount")
                        .unwrap_or_default()
                {
                    return true;
                }
                command.is_some_and(|idx| {
                    ptb.commands.get(idx).map_or(false, |cmd| match cmd {
                        Command::MoveCall(mc) => {
                            mc.module == "py" && mc.function == "repay_pt_amount"
                        }
                        _ => false,
                    })
                })
            }
        }
    }

    pub fn flash_seed(
        &self,
        coin: &StructTag,
        amount: u64,
    ) -> Result<ProgrammableTransaction, BelobogError> {
        match self {
            Self::Cetus {
                package,
                coin_a,
                coin_b,
                global_config,
                pool,
                clock,
            } => {
                let coin = TypeTag::from(coin.clone());
                let a2b = if coin_b == &coin {
                    true
                } else if coin_a == &coin {
                    false
                } else {
                    return Err(eyre!("expeceted {} or {} but get {}", coin_a, coin_b, coin).into());
                };
                let mut builder = ProgrammableTransactionBuilder::new();
                let a2b_arg = builder.pure(a2b).unwrap();
                let config_arg = builder.obj(*global_config).unwrap();
                let pool_arg = builder.obj(*pool).unwrap();
                let clock_arg = builder.obj(*clock).unwrap();
                let amount_is_input = builder.pure(false).unwrap();
                let sqrt = if a2b {
                    builder.pure(4295048016 + 1_u128).unwrap()
                } else {
                    builder
                        .pure(79226673515401279992447579055 - 1_u128)
                        .unwrap()
                };
                let amount_repay = builder.pure(amount + amount * 100 / 1000000 + 1).unwrap();
                let amount = builder.pure(amount).unwrap();
                let amount_0 = builder.pure(0_u64).unwrap();

                builder.programmable_move_call(
                    *package,
                    Identifier::new("pool").unwrap(),
                    Identifier::new("flash_loan").unwrap(),
                    vec![coin_a.clone(), coin_b.clone()],
                    vec![config_arg, pool_arg, a2b_arg, amount],
                );

                // x
                // 0: flash_loan (....)
                // <mutation> <abort> =? discard
                // 1: repay_flash_loan (...) <abort> => queue
                // x

                builder.programmable_move_call(
                    ObjectID::from_str("0x2").unwrap(),
                    Identifier::new("balance").unwrap(),
                    Identifier::new("split").unwrap(),
                    vec![coin_a.clone()],
                    vec![Argument::NestedResult(0, 0), amount_repay],
                );

                builder.programmable_move_call(
                    ObjectID::from_str("0x2").unwrap(),
                    Identifier::new("balance").unwrap(),
                    Identifier::new("split").unwrap(),
                    vec![coin_b.clone()],
                    vec![Argument::NestedResult(0, 1), amount_0],
                );

                builder.programmable_move_call(
                    *package,
                    Identifier::new("pool").unwrap(),
                    Identifier::new("repay_flash_loan").unwrap(),
                    vec![coin_a.clone(), coin_b.clone()],
                    vec![
                        config_arg,
                        pool_arg,
                        Argument::Result(1),
                        Argument::Result(2),
                        Argument::NestedResult(0, 2),
                    ],
                );

                Ok(builder.finish())
            }
            Self::Nemo {
                package,
                coin,
                version,
                py_state,
                clock,
            } => {
                let mut builder = ProgrammableTransactionBuilder::new();
                let version_arg = builder.obj(*version).unwrap();
                let py_state_arg = builder.obj(*py_state).unwrap();
                let amount_arg = builder.pure(amount).unwrap();
                let clock_arg = builder.obj(*clock).unwrap();
                let coin = TypeTag::from(coin.clone());

                builder.programmable_move_call(
                    *package,
                    Identifier::new("py").unwrap(),
                    Identifier::new("init_py_position").unwrap(),
                    vec![coin.clone()],
                    vec![version_arg, py_state_arg, clock_arg],
                );

                builder.programmable_move_call(
                    *package,
                    Identifier::new("py").unwrap(),
                    Identifier::new("borrow_pt_amount").unwrap(),
                    vec![coin.clone()],
                    vec![Argument::Result(0), amount_arg, py_state_arg, clock_arg],
                );

                // x
                // 0: flash_loan (....)
                // <mutation> <abort> =? discard
                // 1: repay_pt_amount (...) <abort> => queue
                // x

                builder.programmable_move_call(
                    *package,
                    Identifier::new("py").unwrap(),
                    Identifier::new("repay_pt_amount").unwrap(),
                    vec![coin.clone()],
                    vec![
                        Argument::Result(0),
                        py_state_arg,
                        Argument::NestedResult(1, 1),
                        clock_arg,
                    ],
                );

                Ok(builder.finish())
            }
        }
    }
}

fn object_to_object_arg(obj: &Object, mutable: bool) -> Option<ObjectArg> {
    match obj.owner() {
        Owner::Shared {
            initial_shared_version,
        } => Some(ObjectArg::SharedObject {
            id: obj.id(),
            initial_shared_version: *initial_shared_version,
            mutable,
        }),
        Owner::Immutable | Owner::AddressOwner(_) => {
            Some(ObjectArg::ImmOrOwnedObject(obj.compute_object_reference()))
        }
        _ => None,
    }
}
