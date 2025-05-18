extern crate core;

use actix_web::{web, App, HttpServer};
use actix_web_prometheus::PrometheusMetricsBuilder;
use app_config::get_app_config;
use assignment::app_config::app_config;
use assignment::app_config::app_config::AppConfig;
use assignment::app_config::config_watcher::{
    start_config_watcher_task, ConfigPollerMessage, DELAY_BETWEEN_ATTEMPTS,
};
use assignment::app_config::signal_handler::start_signal_handler_thread;
use assignment::arb_bot::processor;
use assignment::arb_bot::processor::ProcessorMessage;
use assignment::persistence::persistence_sender;
use assignment::persistence::persistence_sender::PersisterMessage;
use assignment::upstream::price_feed::{
    start_price_feed_man_control_task, PriceFeedManager, PriceFeedManagerMessage,
};
use assignment::web::controller;
use assignment::web::state::WebAppState;
use crossbeam_channel::unbounded;
use log::{error, info};
use structopt::StructOpt;
use tokio::sync::mpsc;
use tokio::time::sleep;

#[derive(StructOpt, Debug)]
#[structopt(name = "rust-arb-bot", about = "A Rust iArb Boy service")]
struct Command {
    #[structopt(name = "app_config key in etcd", long = "--app_config", short = "c")]
    pub config_key: Option<String>,
    #[structopt(name = "etcd url", long = "--etcd_url", short = "u")]
    pub etcd_url: Option<String>,
}

#[tokio::main]
async fn main() {
    log4rs::init_file("log4rs.yml", Default::default()).expect("logging init failed");

    let args: Command = Command::from_args();
    info!("args: {:?}", args);

    let config = get_config(&args).await;

    info!("app_config: {:?}", config);

    let (proc_tx, proc_rx) = unbounded::<ProcessorMessage>();
    let (persister_tx, persister_rx) = unbounded::<PersisterMessage>();
    let (upstream_tx, upstream_rx) = mpsc::unbounded_channel::<PriceFeedManagerMessage>();
    let (config_watcher_tx, config_watcher_rx) = unbounded::<ConfigPollerMessage>();

    start_signal_handler_thread(
        proc_tx.clone(),
        persister_tx.clone(),
        upstream_tx.clone(),
        config_watcher_tx,
    );

    start_config_watcher_task(
        config.clone(),
        upstream_tx.clone(),
        proc_tx.clone(),
        persister_tx.clone(),
        config_watcher_rx,
        args.config_key,
        args.etcd_url,
    );

    let mut price_feed_man = PriceFeedManager::new(proc_tx.clone(), persister_tx.clone());
    price_feed_man.init(config.price_feeds.clone());

    start_price_feed_man_control_task(price_feed_man, upstream_rx);

    processor::start(proc_rx, config);

    persistence_sender::start(persister_rx);

    start_http_server().await;
}

async fn get_config(args: &Command) -> AppConfig {
    loop {
        match get_app_config(args.config_key.clone(), args.etcd_url.clone()).await {
            Ok(c) => return c,
            Err(e) => {
                error!("Error loading config {}", e);
                sleep(DELAY_BETWEEN_ATTEMPTS).await;
            }
        }
    }
}

async fn start_http_server() {
    let prometheus = PrometheusMetricsBuilder::new("arb_bot")
        .endpoint("/metrics")
        .build()
        .unwrap();

    let http_server = HttpServer::new(move || {
        App::new()
            .wrap(prometheus.clone())
            .app_data(web::Data::new(WebAppState::new()))
            .service(web::scope("/api/v1").configure(controller::init))
    });

    http_server
        .workers(2)
        .bind(("0.0.0.0", 8080))
        .expect("Unable to bind address")
        .shutdown_timeout(5)
        .disable_signals()
        .run()
        .await
        .expect("Unable to start web_controller server");
}
