use super::book::Orderbook;
use super::types::*;

fn book() -> Orderbook { Orderbook::new("SOL-USDT") }

#[test]
fn bid_rests_when_no_asks() {
    let mut b = book();
    let r = b.add_order(1, Side::Bid, 100, 10).unwrap();
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0], MatchResult::OrderAccepted(_)));
    assert_eq!(b.best_bid(), Some(100));
    assert_eq!(b.best_ask(), None);
}

#[test]
fn ask_rests_when_no_bids() {
    let mut b = book();
    b.add_order(1, Side::Ask, 110, 10).unwrap();
    assert_eq!(b.best_ask(), Some(110));
    assert_eq!(b.best_bid(), None);
}

#[test]
fn exact_match() {
    let mut b = book();
    // Maker: resting ask at 100, qty 5
    b.add_order(1, Side::Ask, 100, 5).unwrap();

    // Taker: bid at 100, qty 5 — exact match
    let r = b.add_order(2, Side::Bid, 100, 5).unwrap();
    assert_eq!(r.len(), 1);

    match &r[0] {
        MatchResult::Trade(t) => {
            assert_eq!(t.buyer,  2);
            assert_eq!(t.seller, 1);
            assert_eq!(t.price,  100);
            assert_eq!(t.qty,    5);
        }
        _ => panic!("expected trade"),
    }

    assert_eq!(b.best_bid(), None);
    assert_eq!(b.best_ask(), None);
    assert_eq!(b.order_count(), 0);
}

#[test]
fn partial_fill_maker_partially_consumed() {
    let mut b = book();
    // Maker: ask at 100, qty 10
    b.add_order(1, Side::Ask, 100, 10).unwrap();

    // Taker: bid at 100, qty 4
    let r = b.add_order(2, Side::Bid, 100, 4).unwrap();
    assert_eq!(r.len(), 1);

    match &r[0] {
        MatchResult::Trade(t) => {
            assert_eq!(t.qty, 4);
        }
        _ => panic!("expected trade"),
    }

    // Maker still rests with remaining 6
    assert_eq!(b.best_ask(), Some(100));
    let snap = b.get_state();
    assert_eq!(snap.asks[0].qty, 6);
}

#[test]
fn partial_taker_rests_remainder() {
    let mut b = book();
    // Maker: ask at 100, qty 4
    b.add_order(1, Side::Ask, 100, 4).unwrap();

    // Taker: bid at 100, qty 10 — fills 4, rests 6 as bid
    let r = b.add_order(2, Side::Bid, 100, 10).unwrap();
    assert_eq!(r.len(), 2);

    match &r[0] {
        MatchResult::Trade(t) => assert_eq!(t.qty, 4),
        _ => panic!("expected trade"),
    }
    match &r[1] {
        MatchResult::OrderAccepted(acc) => {
            assert_eq!(acc.remaining_qty, 6);
            assert_eq!(acc.side, Side::Bid);
        }
        _ => panic!("expected OrderAccepted"),
    }

    assert_eq!(b.best_ask(), None);
    assert_eq!(b.best_bid(), Some(100));
}

#[test]
fn incoming_order_fills_multiple_makers() {
    let mut b = book();
    b.add_order(1, Side::Ask, 100, 3).unwrap();
    b.add_order(2, Side::Ask, 101, 4).unwrap();

    // Taker bid at 102 for qty 6: fills 3@100, then 3@101
    let r = b.add_order(3, Side::Bid, 102, 6).unwrap();
    assert_eq!(r.len(), 2);

    match &r[0] {
        MatchResult::Trade(t) => { assert_eq!(t.price, 100); assert_eq!(t.qty, 3); }
        _ => panic!("expected trade"),
    }
    match &r[1] {
        MatchResult::Trade(t) => { assert_eq!(t.price, 101); assert_eq!(t.qty, 3); }
        _ => panic!("expected trade"),
    }

    // 1 unit remains at ask 101
    assert_eq!(b.best_ask(), Some(101));
    let snap = b.get_state();
    assert_eq!(snap.asks[0].qty, 1);
}

#[test]
fn preserves_fifo_at_same_price() {
    let mut b = book();
    // Two asks at 100: user 1 first, user 2 second
    b.add_order(1, Side::Ask, 100, 5).unwrap();
    b.add_order(2, Side::Ask, 100, 5).unwrap();

    // Taker takes 6: should fully fill user 1 (5), then take 1 from user 2
    let r = b.add_order(3, Side::Bid, 100, 6).unwrap();
    assert_eq!(r.len(), 2);

    match &r[0] {
        MatchResult::Trade(t) => {
            assert_eq!(t.seller, 1);
            assert_eq!(t.qty, 5);
        }
        _ => panic!("expected trade with user 1"),
    }
    match &r[1] {
        MatchResult::Trade(t) => {
            assert_eq!(t.seller, 2);
            assert_eq!(t.qty, 1);
        }
        _ => panic!("expected trade with user 2"),
    }

    // User 2 has 4 remaining
    let snap = b.get_state();
    assert_eq!(snap.asks[0].qty, 4);
}

#[test]
fn matches_best_price_first() {
    let mut b = book();
    b.add_order(1, Side::Ask, 105, 5).unwrap();
    b.add_order(2, Side::Ask, 100, 5).unwrap(); // better ask

    // Taker bid at 110: must match 100 first, not 105
    let r = b.add_order(3, Side::Bid, 110, 3).unwrap();
    assert_eq!(r.len(), 1);

    match &r[0] {
        MatchResult::Trade(t) => assert_eq!(t.price, 100),
        _ => panic!("expected trade"),
    }
}

#[test]
fn non_crossing_order_rests() {
    let mut b = book();
    b.add_order(1, Side::Ask, 105, 5).unwrap();

    // Bid at 100 does not cross ask at 105: both rest
    let r = b.add_order(2, Side::Bid, 100, 5).unwrap();
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0], MatchResult::OrderAccepted(_)));

    assert_eq!(b.best_bid(), Some(100));
    assert_eq!(b.best_ask(), Some(105));
}

#[test]
fn cancel_bid() {
    let mut b = book();
    let r = b.add_order(1, Side::Bid, 100, 5).unwrap();
    let order_id = match &r[0] {
        MatchResult::OrderAccepted(acc) => acc.order_id,
        _ => panic!("expected OrderAccepted"),
    };

    let cancelled = b.cancel_order(order_id, 1).unwrap();
    assert_eq!(cancelled.order_id, order_id);
    assert_eq!(cancelled.remaining_qty, 5);
    assert_eq!(b.best_bid(), None);
    assert_eq!(b.order_count(), 0);
}

#[test]
fn cannot_cancel_others_order() {
    let mut b = book();
    let r = b.add_order(1, Side::Bid, 100, 5).unwrap();
    let order_id = match &r[0] {
        MatchResult::OrderAccepted(acc) => acc.order_id,
        _ => panic!("expected OrderAccepted"),
    };

    let res = b.cancel_order(order_id, 2); // user 2 tries to cancel user 1's order
    assert_eq!(res, Err(OrderbookError::NotOrderOwner));
}

#[test]
fn cannot_cancel_nonexistent() {
    let mut b = book();
    let res = b.cancel_order(999, 1);
    assert_eq!(res, Err(OrderbookError::OrderNotFound));
}

#[test]
fn rejects_zero_price() {
    let mut b = book();
    assert_eq!(b.add_order(1, Side::Bid, 0, 10), Err(OrderbookError::InvalidPrice));
}

#[test]
fn rejects_zero_qty() {
    let mut b = book();
    assert_eq!(b.add_order(1, Side::Bid, 100, 0), Err(OrderbookError::InvalidQuantity));
}

#[test]
fn get_user_orders_filtered() {
    let mut b = book();
    b.add_order(1, Side::Bid, 100, 5).unwrap();
    b.add_order(2, Side::Bid, 100, 5).unwrap();
    b.add_order(1, Side::Ask, 110, 3).unwrap();

    let u1_orders = b.get_user_orders(1);
    assert_eq!(u1_orders.len(), 2);

    let u2_orders = b.get_user_orders(2);
    assert_eq!(u2_orders.len(), 1);
}

#[test]
fn get_user_orders_remaining_qty_after_partial_fill() {
    let mut b = book();
    // Maker rests 10
    b.add_order(1, Side::Ask, 100, 10).unwrap();
    // Taker takes 4
    b.add_order(2, Side::Bid, 100, 4).unwrap();

    let orders = b.get_user_orders(1);
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].remaining_qty, 6);
}

#[test]
fn test_market_buy_sweeps_multiple_levels_no_resting() {
    let mut b = book();
    // Setup asks: 5 SOL @ 100, 5 SOL @ 110
    b.add_order(1, Side::Ask, 100, 5).unwrap();
    b.add_order(2, Side::Ask, 110, 5).unwrap();

    // Market buy 7 SOL (takes 5 @ 100, 2 @ 110)
    let results = b.execute_market_order(3, Side::Bid, 7, None).unwrap();
    assert_eq!(results.len(), 2);

    match &results[0] {
        MatchResult::Trade(t) => {
            assert_eq!(t.price, 100);
            assert_eq!(t.qty, 5);
            assert_eq!(t.buyer, 3);
            assert_eq!(t.seller, 1);
        }
        _ => panic!("expected trade"),
    }
    match &results[1] {
        MatchResult::Trade(t) => {
            assert_eq!(t.price, 110);
            assert_eq!(t.qty, 2);
            assert_eq!(t.buyer, 3);
            assert_eq!(t.seller, 2);
        }
        _ => panic!("expected trade"),
    }

    // Remaining on book: 3 SOL @ 110
    assert_eq!(b.best_ask(), Some(110));
    let snap = b.get_state();
    assert_eq!(snap.asks[0].qty, 3);
    // Market order NEVER rests in bids
    assert_eq!(b.best_bid(), None);
    assert_eq!(b.get_user_orders(3).len(), 0);
}

#[test]
fn test_market_order_partial_fill_remainder_killed() {
    let mut b = book();
    // Only 4 SOL available in bids @ 95
    b.add_order(1, Side::Bid, 95, 4).unwrap();

    // Market sell 10 SOL: fills 4, kills 6
    let results = b.execute_market_order(2, Side::Ask, 10, None).unwrap();
    assert_eq!(results.len(), 1);
    match &results[0] {
        MatchResult::Trade(t) => {
            assert_eq!(t.price, 95);
            assert_eq!(t.qty, 4);
        }
        _ => panic!("expected trade"),
    }

    // Book is now completely empty
    assert_eq!(b.best_bid(), None);
    assert_eq!(b.best_ask(), None);
    // Seller has NO resting order
    assert_eq!(b.get_user_orders(2).len(), 0);
}

#[test]
fn test_quote_cost_for_market_buy_calculation() {
    let mut b = book();
    b.add_order(1, Side::Ask, 100, 5).unwrap();
    b.add_order(2, Side::Ask, 110, 5).unwrap();

    // 7 SOL: 5*100 + 2*110 = 500 + 220 = 720 USD, fillable: 7
    let (cost, fillable) = b.quote_cost_for_market_buy(7, None);
    assert_eq!(cost, 720);
    assert_eq!(fillable, 7);

    // 15 SOL (more than total depth 10): 5*100 + 5*110 = 1050 USD, fillable: 10
    let (cost_over, fillable_over) = b.quote_cost_for_market_buy(15, None);
    assert_eq!(cost_over, 1050);
    assert_eq!(fillable_over, 10);
}

#[test]
fn test_market_buy_slippage_cap_stops_matching() {
    let mut b = book();
    // Setup asks: 5 SOL @ 100, 5 SOL @ 120
    b.add_order(1, Side::Ask, 100, 5).unwrap();
    b.add_order(2, Side::Ask, 120, 5).unwrap();

    // Market buy 10 SOL, but cap worst price at 105:
    // Should fill 5 @ 100, but reject the 120 level because 120 > 105!
    let results = b.execute_market_order(3, Side::Bid, 10, Some(105)).unwrap();
    assert_eq!(results.len(), 1);
    match &results[0] {
        MatchResult::Trade(t) => {
            assert_eq!(t.price, 100);
            assert_eq!(t.qty, 5);
        }
        _ => panic!("expected trade"),
    }

    // The 120 ask level was NOT touched
    assert_eq!(b.best_ask(), Some(120));
    assert_eq!(b.get_state().asks[0].qty, 5);
}

#[test]
fn test_market_sell_slippage_floor_stops_matching() {
    let mut b = book();
    // Setup bids: 5 SOL @ 100, 5 SOL @ 80
    b.add_order(1, Side::Bid, 100, 5).unwrap();
    b.add_order(2, Side::Bid, 80, 5).unwrap();

    // Market sell 10 SOL, but cap worst price at 95 floor:
    // Should fill 5 @ 100, but refuse to sell at 80 because 80 < 95!
    let results = b.execute_market_order(3, Side::Ask, 10, Some(95)).unwrap();
    assert_eq!(results.len(), 1);
    match &results[0] {
        MatchResult::Trade(t) => {
            assert_eq!(t.price, 100);
            assert_eq!(t.qty, 5);
        }
        _ => panic!("expected trade"),
    }

    // The 80 bid level was NOT touched
    assert_eq!(b.best_bid(), Some(80));
    assert_eq!(b.get_state().bids[0].qty, 5);
}
