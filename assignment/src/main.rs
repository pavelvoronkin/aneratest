extern crate core;

use actix_web::{web, App, HttpServer};
use actix_web_prometheus::PrometheusMetricsBuilder;
use assignment::app_config::app_config;
use assignment::app_config::app_config::PriceFeedConfig;
use assignment::app_config::control::Control;
use assignment::index_collector::index_collector::{IndexCollector, SmoothingAlgorithm, Source};
use assignment::index_collector::processor;
use assignment::index_collector::processor::ProcessorMessage;
use assignment::index_collector::smoothing::{EMASmoothing, SMASmoothing};
use assignment::infra::clock;
use assignment::upstream::price_feed;
use assignment::upstream::price_feed::PriceFeed;
use assignment::web::controller;
use assignment::web::state::WebAppState;
use crossbeam_channel::{unbounded, Sender};
use log::{debug, error, info, trace, warn};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::thread;
use structopt::StructOpt;
use tokio::task::JoinHandle;
use assignment::downstream::downstream_sender;
use assignment::downstream::downstream_sender::DownstreamMessage;

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

    let config = app_config::get_app_config(args.config);
    debug!("app_config: {:?}", config);

    let control = Arc::new(Control::new());

    let (proc_snd, proc_rcv) = unbounded::<ProcessorMessage>();
    let (d_snd, d_rcv) = unbounded::<DownstreamMessage>();

    for (_, price_feed_configs) in config.price_feeds.clone() {
        for price_feed_cfg in price_feed_configs {
            price_feed::spawn_fetch_task(&price_feed_cfg, control.clone(), proc_snd.clone());
        }
    }

    processor::start(proc_rcv, d_snd, config.price_feeds);

    downstream_sender::start(d_rcv);

    start_http_server(control).await;
}

async fn start_http_server(control: Arc<Control>) {
    let prometheus = PrometheusMetricsBuilder::new("index_collector")
        .endpoint("/metrics")
        .build()
        .unwrap();

    let http_server = HttpServer::new(move || {
        App::new()
            .wrap(prometheus.clone())
            .app_data(web::Data::new(WebAppState::new(control.clone())))
            .service(web::scope("/api/v1").configure(controller::init))
    });

    http_server
        .workers(2)
        .bind(("0.0.0.0", 8080))
        .expect("Unable to bind address")
        .shutdown_timeout(5)
        .run()
        .await
        .expect("Unable to start web_controller server");
}
