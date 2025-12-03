// For Move coding conventions, see
// https://docs.sui.io/concepts/sui-move-concepts/conventions

module oracle_sets::oracle_sets;

// public fun test_precision_loss(a: u64, b: u64): u64 {
//     let c = a / b; // Precision loss here
//     let d = c;
//     let e = d * 10;
//     e
// }

// public fun test_bool_judgement(x: u8): bool {
//     let is_equal = x == 0;
//     let y = false;
//     is_equal == !y // Redundant comparison
// }

// public fun test_const_judgement(n: u64): bool {
//     let a = 10;
//     let is_greater = a > 100; // Comparison with constant
//     is_greater
// }

// public fun test_const_judgement_fp(n: u64): bool {
//     let is_greater = n > 100; // Comparison with constant
//     is_greater
// }

public fun test_infinite_loop(n: u64) {
    let mut i = 0;
    let mut sum = 0;
    while (i < n) {
        if (n > 1000) {
            sum = sum + i;
        }
    }
}

// public fun test_infinite_loop_fp(n: u64, y: u64) {
//     let mut i = 0;
//     let mut sum = 0;
//     if (n >= 0) {
//         sum = sum + i;
//         i = i + 1;
//     }
// }
