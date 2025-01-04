use crate::app_config::app_config::{IndexCollectorAppConfig, PriceFeedConfig};
use crate::app_config::control::Control;
use crate::index_collector::index_collector::{Asset, FeedId, Source};
use crate::index_collector::processor::ProcessorMessage;
use crate::index_collector::smoothing::Smoothing;
use crate::infra::clock;
use crate::upstream::coinbase_price_feed::CoinbasePriceFeed;
use crate::upstream::price_feed;
use async_trait::async_trait;
use crossbeam_channel::Sender;
use log::{debug, error, info, trace, warn};
use reqwest::StatusCode;
use serde::Deserialize;
use std::collections::HashMap;
use std::num::ParseFloatError;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub enum PriceFeedManagerMessage {
    ConfigChange(IndexCollectorAppConfig),
    Stop,
}

pub struct PriceFeedManager {
    ctrl: Arc<Control>,
    proc_snd: Sender<ProcessorMessage>,
    persister_snd: Sender<ProcessorMessage>,
    pub tasks: HashMap<FeedId, Arc<AtomicBool>>,
}

impl PriceFeedManager {
    pub fn new(
        ctrl: Arc<Control>,
        proc_snd: Sender<ProcessorMessage>,
        persister_snd: Sender<ProcessorMessage>,
    ) -> Self {
        Self {
            ctrl,
            proc_snd,
            persister_snd,
            tasks: HashMap::new(),
        }
    }

    pub fn init(&mut self, feeds: HashMap<Asset, Vec<PriceFeedConfig>>) {
        let mut map = HashMap::new();
        for (_, price_feed_configs) in feeds {
            for price_feed_cfg in price_feed_configs {
                let key = price_feed_cfg.key();
                map.insert(key.clone(), price_feed_cfg.clone());
                if !self.tasks.contains_key(&key) {
                    let stop_flag = start_fetch_task(
                        price_feed_cfg,
                        self.proc_snd.clone(),
                        self.persister_snd.clone(),
                    );
                    self.tasks.insert(key, stop_flag);
                }


            }
        }
        self.tasks.iter()
            .filter(|item| !map.contains_key(item.0))
            .for_each(|item| {
                info!("Stopping upstream task {}", item.0);
                item.1.store(false, SeqCst);
            });

        self.tasks.retain(|feed_id, _| map.contains_key(feed_id));
    }

    pub fn stop_all(&self) {
        info!("Stopping all upstream tasks");
        for (_, tx) in &self.tasks {
            tx.store(false, SeqCst);
        }
    }
}

pub fn start_price_feed_man_control_task(mut price_feed_man: PriceFeedManager, mut rcv: UnboundedReceiver<PriceFeedManagerMessage>) {
    tokio::spawn(async move {
        loop {
            match rcv.recv().await {
                Some(msg) => match msg {
                    PriceFeedManagerMessage::ConfigChange(conf) => price_feed_man.init(conf.price_feeds),
                    PriceFeedManagerMessage::Stop => price_feed_man.stop_all(),
                },
                None => {}
            }
        }
    });
}


fn start_fetch_task(
    price_feed_cfg: PriceFeedConfig,
    proc_sender: Sender<ProcessorMessage>,
    persister_sender: Sender<ProcessorMessage>,
) -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(true));
    let f = flag.clone();
    tokio::spawn(async move {
        let key = price_feed_cfg.key();
        let mut fail_count = 0;
        let fail_count_warn = price_feed_cfg.fail_count_warn.unwrap_or(5);
        let source = price_feed_cfg.source.clone();
        let asset = price_feed_cfg.asset.clone();
        let price_feed = new_price_feed(&price_feed_cfg);
        info!("Price upstream {} started", &key);

        loop {
            if !f.load(Relaxed) {
                info!("Price upstream {} stopped", &key);
                break;
            }

            match price_feed.fetch().await {
                Ok(price) => {
                    fail_count = 0;
                    trace!("Price upstream {} fetched {}", key, price);

                    // maybe replace with fan?
                    if let Err(e) = proc_sender.try_send(ProcessorMessage::Price(
                        price,
                        asset.clone(),
                        source.clone(),
                    )) {
                        error!("Error try_send {} to processor: {}", key, e);
                    }

                    if let Err(e) = persister_sender.try_send(ProcessorMessage::Price(
                        price,
                        asset.clone(),
                        source.clone(),
                    )) {
                        error!("Error try_send {} to persister: {}", key, e);
                    }
                }
                Err(e) => {
                    debug!(
                        "Error fetching price upstream {}, code: {:?}, message: {}",
                        &source, e.code, e.message
                    );
                    fail_count = fail_count + 1;
                    if fail_count > fail_count_warn {
                        warn!("Error fetching price upstream {}", key);
                        fail_count = 0;
                    }
                }
            };
        }
    });
    flag
}

fn new_price_feed(cfg: &PriceFeedConfig) -> Box<dyn PriceFeed> {
    match cfg.source {
        Source::Coinbase => Box::new(CoinbasePriceFeed::new(cfg.url())),
        Source::Kraken => {
            panic!("Kraken price upstream not supported yet");
        }
    }
}

#[async_trait]
pub trait PriceFeed: Send + Sync + 'static {
    async fn fetch(&self) -> Result<f64, FeedErr>;
}

#[derive(Debug)]
pub struct FeedErr {
    pub code: Option<u16>,
    pub message: String,
}

impl FeedErr {
    pub fn new(code: Option<u16>, message: String) -> FeedErr {
        FeedErr { code, message }
    }
}
