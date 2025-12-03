use itertools::Itertools;
use log::warn;
use move_core_types::account_address::AccountAddress;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt::Display;
use sui_types::{base_types::ObjectID, event::Event};

// unsafe but fine with our fuzzing
pub fn may_be_logging(ev: &Event) -> Option<MoveBelobogLog> {
    may_be_ty_event(None, None, "log", "Log", ev)
}

pub fn may_be_oracle(ev: &Event) -> Option<MoveCrash> {
    may_be_ty_event(None, None, "oracle", "Crash", ev)
}

pub fn may_be_ty_event<T: DeserializeOwned>(
    package: Option<&ObjectID>,
    module_id: Option<&AccountAddress>,
    module: &str,
    name: &str,
    ev: &Event,
) -> Option<T> {
    if let Some(package) = package
        && package != &ev.package_id
    {
        return None;
    }

    if let Some(module_id) = module_id
        && module_id != &ev.type_.address
    {
        return None;
    }
    if ev.type_.module.to_string() == module && ev.type_.name.to_string() == name {
        match bcs::from_bytes::<T>(&ev.contents) {
            Ok(v) => Some(v),
            Err(e) => {
                warn!(
                    "Has an ev: sender={}, ty={}, content={} but failed to decode due to {}",
                    ev.sender,
                    ev.type_,
                    const_hex::encode(&ev.contents),
                    e
                );
                None
            }
        }
    } else {
        None
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MoveMayKeyedString {
    pub key: Option<String>,
    pub value: String,
}

impl Display for MoveMayKeyedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(key) = self.key.as_ref() {
            f.write_fmt(format_args!("{}={:?}", key, &self.value))
        } else {
            f.write_str(&self.value)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MoveBelobogLog {
    pub msg: Vec<MoveMayKeyedString>,
}

impl Display for MoveBelobogLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "BelobogLog({})",
            self.msg.iter().map(|t| t.to_string()).join(", ")
        ))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MoveCrash {
    pub reason: MoveBelobogLog,
}

impl Display for MoveCrash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Crash({})", &self.reason))
    }
}
