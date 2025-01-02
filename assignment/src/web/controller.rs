use crate::web::state::WebAppState;
use actix_web::{get, post, web};
use log::info;

pub fn init(cfg: &mut web::ServiceConfig) {
    cfg.service(health);
    cfg.service(shutdown);
}

#[get("/health")]
async fn health() -> &'static str {
    "ok"
}

#[post("/shutdown")]
async fn shutdown(data: web::Data<WebAppState>) -> &'static str {
    data.control.stop();
    info!("Received shutdown, stop message has been sent");
    "ok"
}
