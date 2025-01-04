use crate::app_config::app_config::PriceFeedConfig;
use crate::app_config::control::Control;
use crate::index_collector::index_collector::Source;
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
use tokio::task::JoinHandle;

pub fn spawn_fetch_task(
    price_feed_cfg: &PriceFeedConfig,
    ctrl: Arc<Control>,
    proc_sender: Sender<ProcessorMessage>,
    persister_sender: Sender<ProcessorMessage>,
) -> JoinHandle<()> {
    let fail_count_warn = price_feed_cfg.fail_count_warn.unwrap_or(5);
    let source = price_feed_cfg.source.clone();
    let asset = price_feed_cfg.asset.clone();
    let price_feed = new_price_feed(&price_feed_cfg);
    tokio::spawn(async move {
        let mut fail_count = 0;
        loop {
            if ctrl.is_stopped() {
                info!("price upstream {} stopped", &source);
                break;
            }

            match price_feed.fetch().await {
                Ok(price) => {
                    fail_count = 0;
                    trace!("price upstream {} fetched {}", source, price);

                    // maybe replace with fan?
                    if let Err(e) = proc_sender.try_send(ProcessorMessage::Price(
                        price,
                        asset.clone(),
                        source.clone(),
                    )) {
                        error!("Error try_send {} to processor: {}", source, e);
                    }

                    if let Err(e) = persister_sender.try_send(ProcessorMessage::Price(
                        price,
                        asset.clone(),
                        source.clone(),
                    )) {
                        error!("Error try_send {} to persister: {}", source, e);
                    }
                }
                Err(e) => {
                    debug!(
                        "error fetching price upstream {}, code: {:?}, message: {}",
                        &source, e.code, e.message
                    );
                    fail_count = fail_count + 1;
                    if fail_count > fail_count_warn {
                        warn!("error fetching price upstream {}", &source);
                        fail_count = 0;
                    }
                }
            };
        }
    })
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

// TODO implement trait for Kraken, Binance, etc
