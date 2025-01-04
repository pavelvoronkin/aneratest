use actix_web::{get, web, HttpRequest};
use actix_web::error::QueryPayloadError;
use actix_web::web::Query;
use log::{error, info};
use serde::Deserialize;

pub fn init(cfg: &mut web::ServiceConfig) {
    cfg.service(health);
    cfg.service(demo_callback);
}

#[get("/health")]
async fn health() -> &'static str {
    "ok"
}

#[derive(Deserialize)]
struct CallbackParameters {
    price: String,
    asset: String,
}

#[get("/callback")]
async fn demo_callback(req: HttpRequest) -> &'static str {
    match Query::<CallbackParameters>::from_query(req.query_string()) {
        Ok(params) => {
            info!("For the sake of demo price={}, asset={}", params.price, params.asset);
        }
        Err(e) => {
            error!("Error parsing callback parameters {}", e);
        }
    }
    "ok"
}