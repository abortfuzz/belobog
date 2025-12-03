/*
/// Module: hello_dep

*/

// For Move coding conventions, see
// https://docs.sui.io/concepts/sui-move-concepts/conventions

module hello_dep::dep;
use belobog::log;

public fun hello_world_dep() {
    belobog::log::log_string(b"hello dependency!".to_string());
}