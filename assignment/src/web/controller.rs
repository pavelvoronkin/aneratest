use actix_web::web::Query;
use actix_web::{get, web, HttpRequest};
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

/**
    Downstream mock endpoint to demonstrate how app works

    Can be used in the config, like this:

    "downstream": {
        "url": "http://127.0.0.1:8080/api/v1/callback"
      }
*/
#[get("/callback")]
async fn demo_callback(req: HttpRequest) -> &'static str {
    match Query::<CallbackParameters>::from_query(req.query_string()) {
        Ok(params) => {
            info!(
                "For the sake of demo price={}, asset={}",
                params.price, params.asset
            );
        }
        Err(e) => {
            error!("Error parsing callback parameters {}", e);
        }
    }
    "ok"
}
