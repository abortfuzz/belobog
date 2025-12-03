module belobog::log;
use sui::event;
use std::string::String;
use std::option::none;
use std::option::some;

// Not using generics by design

public struct MayKeyedString has copy, drop {
    key: Option<String>,
    value: String
}

public struct Log has copy, drop {
    msg: vector<MayKeyedString>
}

public fun make_log(msg: vector<MayKeyedString>): Log {
    Log {
        msg: msg
    }
}

// Utils for making a single log entry
public fun make_entry(msg: String): MayKeyedString {
    MayKeyedString {
            key: none(),
            value: msg
    }
}

public fun make_keyed_entry(key: String, msg: String): MayKeyedString {
    MayKeyedString {
            key: some(key),
            value: msg
    }
}

// Quick helpers to emit a single entry message

public fun log_string(msg: String) {
    log(vector[make_entry(msg)])
}

public fun log_keyed_string(key: String, msg: String) {
    log(vector[make_keyed_entry(key, msg)])
}

// Can we have T: Display ?!

public fun log_u8(v: u8) {
    log_string(v.to_string())
}

public fun log_keyed_u8(key: String, v: u8) {
    log_keyed_string(key, v.to_string())
}

public fun log_u16(v: u16) {
    log_string(v.to_string())
}

public fun log_keyed_u16(key: String, v: u16) {
    log_keyed_string(key, v.to_string())
}

public fun log_u32(v: u32) {
    log_string(v.to_string())
}

public fun log_keyed_u32(key: String, v: u32) {
    log_keyed_string(key, v.to_string())
}

public fun log_u64(v: u64) {
    log_string(v.to_string())
}

public fun log_keyed_u64(key: String, v: u64) {
    log_keyed_string(key, v.to_string())
}

public fun log_u128(v: u128) {
    log_string(v.to_string())
}

public fun log_keyed_u128(key: String, v: u128) {
    log_keyed_string(key, v.to_string())
}

public fun log_id(v: ID) {
    log_string(v.to_address().to_string())
}

public fun log_keyed_id(key: String, v: ID) {
    log_keyed_string(key, v.to_address().to_string())
}

// Raw loggers
public fun log(msg: vector<MayKeyedString>) {
    event::emit(make_log(msg));
}