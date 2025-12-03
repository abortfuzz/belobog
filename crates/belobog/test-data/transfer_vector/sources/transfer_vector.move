/*
/// Module: transfer_vector
module transfer_vector::transfer_vector;
*/

// For Move coding conventions, see
// https://docs.sui.io/concepts/sui-move-concepts/conventions

module transfer_vector::transfer_vector {
    use std::vector;
    use sui::transfer;
    use sui::object;
    use sui::tx_context::{Self, TxContext};
    
    public struct A has key, store {
        id: object::UID,
        value: u64,
    }

    public fun transfer_vector(ctx: &mut TxContext) {
        let mut vec = vector::empty<A>();

        let a1 = A { id: object::new(ctx), value: 10 };
        let a2 = A { id: object::new(ctx), value: 20 };
        let a3 = A { id: object::new(ctx), value: 30 };

        vector::push_back(&mut vec, a1);
        vector::push_back(&mut vec, a2);
        vector::push_back(&mut vec, a3);

        // Transfer the entire vector of A objects
        transfer::public_transfer(vec, ctx.sender());
    }
}
