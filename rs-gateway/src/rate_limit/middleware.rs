use std::future::{ready, Future, Ready};
use std::pin::Pin;
use std::rc::Rc;
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::{Error, HttpResponse};
use super::{extract_client_ip, RateLimiter};

/// Configuration for rate limiting policies across different endpoint groups.
#[derive(Clone)]
pub struct RateLimitMiddleware {
    pub auth_limiter:    RateLimiter,
    pub trading_limiter: RateLimiter,
    pub general_limiter: RateLimiter,
}

impl Default for RateLimitMiddleware {
    fn default() -> Self {
        Self {
            // Auth endpoints (signup, signin): burst of 20, refills 2 tokens/sec
            auth_limiter: RateLimiter::new(20.0, 2.0),
            // Trading endpoints (/order, /order/:id): burst of 100, refills 20 tokens/sec
            trading_limiter: RateLimiter::new(100.0, 20.0),
            // General endpoints (balance, orderbook): burst of 100, refills 20 tokens/sec
            general_limiter: RateLimiter::new(100.0, 20.0),
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for RateLimitMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: 'static + actix_web::body::MessageBody,
{
    type Response = ServiceResponse<actix_web::body::BoxBody>;
    type Error = Error;
    type InitError = ();
    type Transform = RateLimitService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RateLimitService {
            service: Rc::new(service),
            policy:  self.clone(),
        }))
    }
}

pub struct RateLimitService<S> {
    service: Rc<S>,
    policy:  RateLimitMiddleware,
}

impl<S, B> Service<ServiceRequest> for RateLimitService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: 'static + actix_web::body::MessageBody,
{
    type Response = ServiceResponse<actix_web::body::BoxBody>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let path = req.path().to_string();
        let client_ip = extract_client_ip(&req);
        let srv = Rc::clone(&self.service);
        let policy = self.policy.clone();

        Box::pin(async move {
            // Bypass rate limiting for test reset endpoint
            if path == "/reset" {
                let res = srv.call(req).await?;
                return Ok(res.map_into_boxed_body());
            }

            let check_result = if path.starts_with("/signup") || path.starts_with("/signin") {
                policy.auth_limiter.check(&client_ip).await
            } else if path.starts_with("/order") {
                policy.trading_limiter.check(&client_ip).await
            } else {
                policy.general_limiter.check(&client_ip).await
            };

            match check_result {
                Ok(()) => {
                    let res = srv.call(req).await?;
                    Ok(res.map_into_boxed_body())
                }
                Err(wait_secs) => {
                    let response = HttpResponse::TooManyRequests()
                        .insert_header(("Retry-After", wait_secs.to_string()))
                        .json(serde_json::json!({
                            "message": "Too many requests. Please slow down.",
                            "retryAfterSeconds": wait_secs
                        }));
                    Ok(req.into_response(response))
                }
            }
        })
    }
}
