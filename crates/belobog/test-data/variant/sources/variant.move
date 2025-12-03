/*
/// Module: variant
*/

// For Move coding conventions, see
// https://docs.sui.io/concepts/sui-move-concepts/conventions


module variant::variant;

public enum V {
    A(u64),
    B {x: u64, y: u64},
    C
}

public fun pack_a(val: u64): V {
    V::A(val)
}

public fun pack_b(x: u64, y: u64): V {
    V::B {x, y}
}

public fun pack_c(): V {
    V::C
}

public fun unpack_v(v: V): u64 {
    match (v) {
        V::A(val) => val,
        V::B {x, y} => x + y,
        V::C => 0
    }
}
