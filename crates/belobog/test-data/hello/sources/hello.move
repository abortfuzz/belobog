/// Module: todo_list
module hello::hello;

use belobog::log::make_keyed_entry;
use sui::coin::Coin;
use sui::event::emit;

public struct SelfEvent has copy, drop, store {
    epoch: u64,
    b: u64,
    sender: address,
}

public struct SelfStruct has drop {
    value: u64,
}

fun init(ctx: &mut TxContext) {
    belobog::log::log_string(b"Init from hello!".to_string());
    belobog::log::log(
        vector[
            make_keyed_entry(
                b"epoch".to_string(),
                ctx.epoch().to_string()
            ),
            make_keyed_entry(
                b"sender".to_string(),
                ctx.sender().to_string()
            )
        ]
    );
    emit(SelfEvent {
        epoch: ctx.epoch(),
        b: 0,
        sender: ctx.sender(),
    });
    hello_dep::dep::hello_world_dep();
}

// public fun flawed() {
//     belobog::oracle::crash_because(b"It works".to_string());
// }

public fun gen_struct(a: u64, b: u64): SelfStruct {
    // assert!(a > 254032157);
    // assert!(a < 18446744073709551615);
    if (a < b) {
        emit(SelfEvent {
            epoch: a,
            b: b,
            sender: @0x42,
        });
    };
    emit(SelfEvent {
            epoch: a,
            b: b,
            sender: @0x42,
        });
    SelfStruct { value: a }
}

public fun overflow(a: u64, s: SelfStruct, b: u8): u64 {
    assert!(a == s.value);
    a << b
}
