use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rs_engine::orderbook::{Orderbook, Side, TimeInForce};

/// Benchmark inserting non-crossing resting limit orders into the orderbook.
fn bench_resting_order_placement(c: &mut Criterion) {
    let mut group = c.benchmark_group("Orderbook Ingestion");
    const NUM_ORDERS: u64 = 1_000;
    group.throughput(Throughput::Elements(NUM_ORDERS));

    group.bench_function("place_1000_resting_orders", |b| {
        b.iter_batched(
            || Orderbook::new("SOL_USD"),
            |mut book| {
                for i in 1..=NUM_ORDERS {
                    // Descending bid prices: none will cross
                    let price = 10_000 - (i % 500);
                    let _ = black_box(book.add_order(i, Side::Bid, price, 10, TimeInForce::Gtc));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark matching taker orders against resting maker orders.
fn bench_order_matching(c: &mut Criterion) {
    let mut group = c.benchmark_group("Orderbook Matching");
    const NUM_MAKERS: u64 = 1_000;
    group.throughput(Throughput::Elements(NUM_MAKERS));

    group.bench_function("match_1000_fills", |b| {
        b.iter_batched(
            || {
                let mut book = Orderbook::new("SOL_USD");
                // Pre-populate with 1,000 resting asks at price 100
                for i in 1..=NUM_MAKERS {
                    let _ = book.add_order(i, Side::Ask, 100, 1, TimeInForce::Gtc);
                }
                book
            },
            |mut book| {
                // One large taker order that matches all 1,000 resting asks
                let results = book.add_order(99_999, Side::Bid, 100, NUM_MAKERS, TimeInForce::Gtc);
                black_box(results).unwrap()
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark O(1) order lookup and cancellation.
fn bench_order_cancellation(c: &mut Criterion) {
    let mut group = c.benchmark_group("Orderbook Cancellation");
    const NUM_ORDERS: u64 = 1_000;
    group.throughput(Throughput::Elements(NUM_ORDERS));

    group.bench_function("cancel_1000_orders", |b| {
        b.iter_batched(
            || {
                let mut book = Orderbook::new("SOL_USD");
                for i in 1..=NUM_ORDERS {
                    let _ = book.add_order(i, Side::Bid, 500 + (i % 100), 5, TimeInForce::Gtc);
                }
                book
            },
            |mut book| {
                for i in 1..=NUM_ORDERS {
                    let _ = black_box(book.cancel_order(i, i));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_resting_order_placement,
    bench_order_matching,
    bench_order_cancellation
);
criterion_main!(benches);
