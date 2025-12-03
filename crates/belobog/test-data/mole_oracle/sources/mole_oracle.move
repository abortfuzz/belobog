module mole_vault::vault_oracle {
use belobog::oracle;
use mole::global_storage::GlobalStorage;
use mole::mole_math;
use mole_vault::vault;
use mole_vault::vault_config;
use sui::clock::Clock;
use sui::coin::Coin;

/// Detect issue (2): fee becomes zero despite substantive activity.
/// This checks both interest reserve fees and liquidation fees.
public fun assert_fee_invariants<CoinType>(
    storage: &GlobalStorage,
    config_addr: address,
    _clock: &Clock,
    _coin: &Coin<CoinType>,
    liquidation_back: u64
) {
    // --- Reserve fee on accrued interest should not be zero when interest is positive ---
    let debt = vault::get_vault_debt_value<CoinType>(storage);
    let balance = vault::get_vault_base_coin_balance<CoinType>(storage);
    let rate_per_sec = vault_config::get_interest_rate(storage, config_addr, debt, balance);
    let reserve_bps = vault_config::get_reserve_pool_bps(storage, config_addr);

    // If rate, debt, and reserve_bps are positive, a 1-second theoretical interest should imply a positive reserve fee after scaling.
    if (rate_per_sec > 0 && debt > 0 && reserve_bps > 0) {
        let interest_scale = vault_config::get_interest_rate_scale();
        // Minimal theoretical interest for >=1 second (rounding-down aware)
        let min_interest = mole_math::mul_div(debt, rate_per_sec, interest_scale);
        if (min_interest > 0) {
            let reserve_scale = vault_config::get_reserve_pool_bps_scale();
            let to_reserve = mole_math::mul_div(min_interest, reserve_bps, reserve_scale);
            if (to_reserve == 0) {
                belobog::oracle::crash_because(b"Zero reserve fee detected despite positive interest and nonzero reserve_bps".to_string());
            };
        };
    };

    // --- Liquidation fees (liquidator prize + treasury fee) should not be zero when proceeds are substantive ---
    let kill_bps = vault_config::get_kill_bps(storage, config_addr);
    let kill_scale = vault_config::get_kill_bps_scale();
    let kill_treasury_bps = vault_config::get_kill_treasury_bps(storage, config_addr);
    let kill_treasury_scale = vault_config::get_kill_treasury_bps_scale();

    if (liquidation_back > 0 && (kill_bps > 0 || kill_treasury_bps > 0)) {
        let liquidator_prize = mole_math::mul_div(liquidation_back, kill_bps, kill_scale);
        let treasury_fee = mole_math::mul_div(liquidation_back, kill_treasury_bps, kill_treasury_scale);
        let total_fee = liquidator_prize + treasury_fee;

        if (total_fee == 0) {
            belobog::oracle::crash_because(b"Zero liquidation fees detected despite positive proceeds and nonzero fee bps".to_string());
        };

        // Stronger invariant: if proceeds exceed respective scales and bps>0, each component should be nonzero (detect rounding-to-zero risks).
        if (kill_bps > 0 && liquidation_back >= kill_scale && liquidator_prize == 0) {
            belobog::oracle::crash_because(b"Liquidator prize rounded to zero unexpectedly".to_string());
        };
        if (kill_treasury_bps > 0 && liquidation_back >= kill_treasury_scale && treasury_fee == 0) {
            belobog::oracle::crash_because(b"Treasury fee rounded to zero unexpectedly".to_string());
        };
    };
}

}
