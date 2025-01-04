extern crate core;

use actix_web::{web, App, HttpServer};
use actix_web_prometheus::PrometheusMetricsBuilder;
use assignment::app_config::app_config;
use assignment::app_config::app_config::{
    start_config_poller_task, IndexCollectorAppConfig, PriceFeedConfig,
};
use assignment::app_config::control::{start_signal_handler_thread, Control};
use assignment::downstream::downstream_sender;
use assignment::downstream::downstream_sender::DownstreamMessage;
use assignment::index_collector::index_collector::{IndexCollector, SmoothingAlgorithm, Source};
use assignment::index_collector::processor;
use assignment::index_collector::processor::ProcessorMessage;
use assignment::index_collector::smoothing::{EMASmoothing, SMASmoothing};
use assignment::infra::clock;
use assignment::persistence;
use assignment::persistence::persistence_sender;
use assignment::upstream::price_feed;
use assignment::upstream::price_feed::{start_price_feed_man_control_task, PriceFeed, PriceFeedManager, PriceFeedManagerMessage};
use assignment::web::controller;
use assignment::web::state::WebAppState;
use crossbeam_channel::{unbounded, Sender};
use log::{debug, error, info, trace, warn};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use signal_hook::low_level::exit;
use std::alloc::System;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "rust-index-collector",
    about = "A Rust index collector service"
)]
struct Command {
    #[structopt(name = "app_config", long = "--app_config")]
    pub config: Option<String>,
}

#[tokio::main]
async fn main() {
    log4rs::init_file("conf/log4rs.yml", Default::default()).expect("logging init failed");

    let args: Command = Command::from_args();
    info!("args: {:?}", args);

    match app_config::get_app_config(args.config.clone()) {
        Ok(config) => {
            info!("app_config: {:?}", config);

            let (proc_tx, proc_rx) = unbounded::<ProcessorMessage>();
            let (persister_tx, persister_rx) = unbounded::<ProcessorMessage>();
            let (downstream_tx, downstream_rx) = unbounded::<DownstreamMessage>();
            let (upstream_tx, upstream_rx) = mpsc::unbounded_channel::<PriceFeedManagerMessage>();

            let control = Arc::new(Control::new(
                proc_tx.clone(),
                persister_tx.clone(),
                downstream_tx.clone(),
                upstream_tx.clone()
            ));

            start_signal_handler_thread(control.clone());

            start_config_poller_task(
                control.clone(),
                config.clone(),
                upstream_tx.clone(),
                proc_tx.clone(),
                persister_tx.clone(),
                downstream_tx.clone(),

                args.config,
            );

            let mut price_feed_man = PriceFeedManager::new(control.clone(), proc_tx.clone(), persister_tx.clone());
            price_feed_man.init(config.price_feeds.clone());

            start_price_feed_man_control_task(price_feed_man, upstream_rx);

            processor::start(proc_rx, downstream_tx, config.price_feeds);
            downstream_sender::start(downstream_rx, config.downstream);
            persistence_sender::start(persister_rx);

            start_http_server().await;
        }
        Err(e) => {
            error!("Error loading app_config: {:?}", e);
            exit(1);
        }
    }
}

async fn start_http_server() {
    let prometheus = PrometheusMetricsBuilder::new("index_collector")
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
