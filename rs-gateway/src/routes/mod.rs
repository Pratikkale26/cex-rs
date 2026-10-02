pub mod admin;
pub mod auth;
pub mod balance;
pub mod orderbook;
pub mod orders;

use actix_web::web;

/// Configures all HTTP API routes for the gateway.
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg
        // Authentication routes (Public)
        .route("/signup", web::post().to(auth::signup))
        .route("/signin", web::post().to(auth::signin))
        // Account balance & funds routes (Bearer auth)
        .route("/balance", web::get().to(balance::get_balance))
        .route("/onramp", web::post().to(balance::onramp))
        .route("/deposit/{asset}", web::post().to(balance::deposit))
        // Orders & Trading routes (Bearer auth)
        .route("/order", web::post().to(orders::place_order))
        .route("/order/{order_id}", web::delete().to(orders::cancel_order))
        .route("/orders/open", web::get().to(orders::get_open_orders))
        // Market depth routes (Public)
        .route("/orderbook/{asset}", web::get().to(orderbook::get_orderbook))
        // Admin / Test suite reset (Public)
        .route("/reset", web::post().to(admin::reset));
}
