/*
/// Module: fat_loop
module fat_loop::fat_loop;
*/

// For Move coding conventions, see
// https://docs.sui.io/concepts/sui-move-concepts/conventions

module fat_loop::fat_loop;
use std::ascii::{Self, String, Char};

const HEX_SYMBOLS: vector<u8> = b"0123456789abcdef";

    public fun u128_to_hex_string_fixed_length(mut value: u128, length: u128): String {
        let mut buffer = vector[];
        let hex_symbols = HEX_SYMBOLS;

        let mut i: u128 = 0;
        while (i < length * 2) {
            buffer.push_back(hex_symbols[(value & 0xf as u64)]);
            value = value >> 4;
            i = i + 1;
        };
        assert!(value == 0, 1);
        buffer.append(b"x0");
        buffer.reverse();
        buffer.to_ascii_string()
    }

//     public fun pow(mut n: u256, mut e: u256): u256 {
//         if (e == 0) {
//             1
//         } else {
//             let mut p = 1;
//             while (e > 1) {
//                 if (e % 2 == 1) {
//                     p = p * n;
//                 };
//                 e = e / 2;
//                 n = n * n;
//             };
//             p * n
//         }
//     }

//   public fun empty_vector(x: u256): vector<u256> {
//     let mut data = vector::empty();

//     let mut i = 0;
//     let mut j = 0;
//     while (x > j) {
//       while (x > i) {
//         // data.push_back(0);
//         i = i + 1;
//       };
//       j = j + 1;
//     };

//     data
//   }
