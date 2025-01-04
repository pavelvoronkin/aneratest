extern crate core;

use actix_web::{web, App, HttpServer};
use actix_web_prometheus::PrometheusMetricsBuilder;
use assignment::app_config::app_config;
use assignment::app_config::app_config::{
    start_config_poller_task, ConfigPollerMessage,
};
use assignment::app_config::signal_handler::start_signal_handler_thread;
use assignment::downstream::downstream_sender;
use assignment::downstream::downstream_sender::DownstreamMessage;
use assignment::index_collector::processor;
use assignment::index_collector::processor::ProcessorMessage;
use assignment::persistence::persistence_sender;
use assignment::persistence::persistence_sender::PersisterMessage;
use assignment::upstream::price_feed::{
    start_price_feed_man_control_task, PriceFeedManager, PriceFeedManagerMessage,
};
use assignment::web::controller;
use assignment::web::state::WebAppState;
use crossbeam_channel::unbounded;
use log::{error, info};
use signal_hook::low_level::exit;
use structopt::StructOpt;
use tokio::sync::mpsc;

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
            let (persister_tx, persister_rx) = unbounded::<PersisterMessage>();
            let (downstream_tx, downstream_rx) = mpsc::unbounded_channel::<DownstreamMessage>();
            let (upstream_tx, upstream_rx) = mpsc::unbounded_channel::<PriceFeedManagerMessage>();
            let (config_poller_tx, config_poller_rx) = unbounded::<ConfigPollerMessage>();

            start_signal_handler_thread(
                proc_tx.clone(),
                persister_tx.clone(),
                downstream_tx.clone(),
                upstream_tx.clone(),
                config_poller_tx,
            );

            start_config_poller_task(
                config.clone(),
                upstream_tx.clone(),
                proc_tx.clone(),
                persister_tx.clone(),
                downstream_tx.clone(),
                config_poller_rx,
                args.config,
            );

            let mut price_feed_man = PriceFeedManager::new(proc_tx.clone(), persister_tx.clone());
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
