use rs_shared::*;
use crate::state::EngineState;
use super::reply;

pub async fn handle_signup(
    state:     &mut EngineState,
    publisher: Option<&mut redis::aio::MultiplexedConnection>,
    msg:       SignupMsg,
) {
    // Initialise zero balances for the new user.
    state.usd_balance.entry(msg.user_id).or_default();
    state.stock_balance.entry(msg.user_id).or_default();

    reply(publisher, &msg.queue_id, &BalanceReply {
        identifier:    msg.identifier,
        usd_balance:   state.usd_balance[&msg.user_id].clone(),
        stock_balance: state.stock_balance[&msg.user_id].clone(),
    }).await;
}

pub async fn handle_onramp(
    state:     &mut EngineState,
    publisher: Option<&mut redis::aio::MultiplexedConnection>,
    msg:       OnrampMsg,
) {
    state.usd_mut(msg.user_id).available += msg.qty;

    let usd   = state.usd_balance.get(&msg.user_id).cloned().unwrap_or_default();
    let stock = state.stock_balance.get(&msg.user_id).cloned().unwrap_or_default();

    reply(publisher, &msg.queue_id, &BalanceReply {
        identifier:    msg.identifier,
        usd_balance:   usd,
        stock_balance: stock,
    }).await;
}

pub async fn handle_deposit(
    state:     &mut EngineState,
    publisher: Option<&mut redis::aio::MultiplexedConnection>,
    msg:       DepositMsg,
) {
    let symbol_balance = state.stock_balance
        .entry(msg.user_id)
        .or_default()
        .entry(msg.symbol.clone())
        .or_default();

    symbol_balance.available += msg.qty;

    let usd   = state.usd_balance.get(&msg.user_id).cloned().unwrap_or_default();
    let stock = state.stock_balance.get(&msg.user_id).cloned().unwrap_or_default();

    reply(publisher, &msg.queue_id, &BalanceReply {
        identifier:    msg.identifier,
        usd_balance:   usd,
        stock_balance: stock,
    }).await;
}

pub async fn handle_balance(
    state:     &EngineState,
    publisher: Option<&mut redis::aio::MultiplexedConnection>,
    msg:       BalanceQueryMsg,
) {
    let usd   = state.usd_balance.get(&msg.user_id).cloned().unwrap_or_default();
    let stock = state.stock_balance.get(&msg.user_id).cloned().unwrap_or_default();

    reply(publisher, &msg.queue_id, &BalanceReply {
        identifier:    msg.identifier,
        usd_balance:   usd,
        stock_balance: stock,
    }).await;
}
