/*
/// Module: vector_ref
module vector_ref::vector_ref;
*/

// For Move coding conventions, see
// https://docs.sui.io/concepts/sui-move-concepts/conventions

module vector_ref::vector_ref;

public fun vector_ref_example(): u64 {
    let mut v = vector::empty<u64>();
    vector::push_back(&mut v, 10);
    let r = &v[0];
    *r
}
