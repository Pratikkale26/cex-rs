use serde::{Deserialize, Serialize};
use rs_shared::OpenOrderInfo;

#[derive(Clone)]
pub struct ExchangeClient {
    base_url: String,
    http: reqwest::Client,
}

#[derive(Serialize)]
struct AuthBody<'a> {
    username: &'a str,
    password: &'a str,
}

#[derive(Deserialize)]
struct SigninResponse {
    token: String,
}

#[derive(Serialize)]
struct OnrampBody {
    qty: u64,
}

#[derive(Serialize)]
struct DepositBody {
    qty: u64,
}

#[derive(Serialize)]
pub struct OrderRequest {
    pub asset: String,
    pub side: String,
    pub order_type: String,
    pub price: u64,
    pub qty: u64,
    pub time_in_force: String,
}

#[derive(Deserialize)]
struct OrderResponse {
    #[serde(rename = "orderId")]
    order_id: Option<u64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct OpenOrdersResponse {
    #[serde(default)]
    orders: Vec<OpenOrderInfo>,
}

#[derive(Deserialize)]
pub struct OrderbookLevel {
    pub price: u64,
    pub qty: u64,
}

#[derive(Deserialize)]
pub struct OrderbookInner {
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
}

#[derive(Deserialize)]
pub struct OrderbookResponse {
    pub orderbook: OrderbookInner,
}

impl ExchangeClient {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            http: reqwest::Client::new(),
        }
    }

    pub async fn wait_for_gateway(&self, max_retries: usize) -> Result<(), String> {
        let metrics_url = format!("{}/metrics", self.base_url);
        for attempt in 1..=max_retries {
            match self.http.get(&metrics_url).send().await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                _ => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }
            if attempt % 6 == 0 {
                println!("[rs-bot] Waiting for rs-gateway at {} (attempt {}/{})...", self.base_url, attempt, max_retries);
            }
        }
        Err(format!("Timed out waiting for gateway at {}", self.base_url))
    }

    pub async fn signup(&self, u: &str, p: &str) -> Result<(), String> {
        let url = format!("{}/signup", self.base_url);
        let _ = self.http.post(&url)
            .json(&AuthBody { username: u, password: p })
            .send()
            .await;
        Ok(())
    }

    pub async fn signin(&self, u: &str, p: &str) -> Result<String, String> {
        let url = format!("{}/signin", self.base_url);
        let resp = self.http.post(&url)
            .json(&AuthBody { username: u, password: p })
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(format!("Signin failed ({}): {}", u, txt));
        }

        let body: SigninResponse = resp.json().await.map_err(|e| e.to_string())?;
        Ok(body.token)
    }

    pub async fn onramp(&self, token: &str, qty: u64) -> Result<(), String> {
        let url = format!("{}/onramp", self.base_url);
        let resp = self.http.post(&url)
            .bearer_auth(token)
            .json(&OnrampBody { qty })
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(format!("Onramp failed: {}", txt));
        }
        Ok(())
    }

    pub async fn deposit(&self, token: &str, asset: &str, qty: u64) -> Result<(), String> {
        let url = format!("{}/deposit/{}", self.base_url, asset.to_lowercase());
        let resp = self.http.post(&url)
            .bearer_auth(token)
            .json(&DepositBody { qty })
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(format!("Deposit {} failed: {}", asset, txt));
        }
        Ok(())
    }

    pub async fn place_order(&self, token: &str, order: &OrderRequest) -> Result<Option<u64>, String> {
        let url = format!("{}/order", self.base_url);
        let resp = self.http.post(&url)
            .bearer_auth(token)
            .json(order)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(format!("Place order failed: {}", txt));
        }

        let res: OrderResponse = resp.json().await.map_err(|e| e.to_string())?;
        Ok(res.order_id)
    }

    #[allow(dead_code)]
    pub async fn get_open_orders(&self, token: &str) -> Result<Vec<OpenOrderInfo>, String> {
        let url = format!("{}/orders/open", self.base_url);
        let resp = self.http.get(&url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            return Ok(Vec::new());
        }

        let res: OpenOrdersResponse = resp.json().await.unwrap_or(OpenOrdersResponse { orders: Vec::new() });
        Ok(res.orders)
    }

    #[allow(dead_code)]
    pub async fn cancel_order(&self, token: &str, order_id: u64) -> Result<(), String> {
        let url = format!("{}/order/{}", self.base_url, order_id);
        let resp = self.http.delete(&url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(format!("Cancel order #{} failed: {}", order_id, txt));
        }
        Ok(())
    }

    pub async fn get_orderbook(&self, asset: &str) -> Result<OrderbookInner, String> {
        let url = format!("{}/orderbook/{}", self.base_url, asset.to_lowercase());
        let resp = self.http.get(&url)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let res: OrderbookResponse = resp.json().await.map_err(|e| e.to_string())?;
        Ok(res.orderbook)
    }
}
