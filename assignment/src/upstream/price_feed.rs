use crate::app_config::app_config::PriceFeedConfig;
use crate::index_collector::index_collector::Source;
use crate::index_collector::smoothing::Smoothing;
use async_trait::async_trait;
use log::{debug, error, info, trace, warn};
use reqwest::StatusCode;
use serde::Deserialize;
use std::collections::HashMap;
use std::num::ParseFloatError;
use std::sync::Arc;
use crossbeam_channel::Sender;
use tokio::task::JoinHandle;
use crate::app_config::control::Control;
use crate::index_collector::processor::ProcessorMessage;
use crate::infra::clock;
use crate::upstream::price_feed;

pub fn spawn_fetch_task(
    price_feed_cfg: &PriceFeedConfig,
    ctl: Arc<Control>,
    sender: Sender<ProcessorMessage>,
) -> JoinHandle<()> {
    let fail_count_warn = price_feed_cfg.fail_count_warn.unwrap_or(5);
    let source = price_feed_cfg.source.clone();
    let asset = price_feed_cfg.asset.clone();
    let price_feed = new_price_feed(&price_feed_cfg);
    tokio::spawn(async move {
        let mut last_paused_msg = clock::current_timestamp();
        let mut fail_count = 0;
        loop {
            if ctl.is_stopped() {
                info!("price upstream {} stopped", &source);
                break;
            }

            if ctl.is_paused() {
                let now = clock::current_timestamp();
                if now - last_paused_msg > 1000 {
                    info!("price upstream {} paused", &source);
                    last_paused_msg = now;
                }
            }

            match price_feed.fetch().await {
                Ok(price) => {
                    fail_count = 0;
                    trace!("price upstream {} fetched {}", source, price);
                    if let Err(e) = sender.try_send(ProcessorMessage::Price(
                        price,
                        asset.clone(),
                        source.clone(),
                    )) {
                        error!("Error try_send price fetched {}: {}", source, e);
                    }
                }
                Err(e) => {
                    debug!(
                        "error fetching price upstream {}, code: {:?}, message: {}",
                        &source, e.code, e.message
                    );
                    fail_count = fail_count + 1;
                    if fail_count > fail_count_warn {
                        warn!("error fetching price upstream {}",&source);
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

struct CoinbasePriceFeed {
    url: String,
}

impl CoinbasePriceFeed {
    pub fn new(url: String) -> CoinbasePriceFeed {
        CoinbasePriceFeed { url }
    }
}

#[derive(Deserialize, Debug)]
struct CoinbasePriceDataResponse {
    data: CoinbasePriceDataData,
}

#[derive(Deserialize, Debug)]
struct CoinbasePriceDataData {
    rates: HashMap<String, String>,
}

#[async_trait]
impl PriceFeed for CoinbasePriceFeed {
    async fn fetch(&self) -> Result<f64, FeedErr> {
        // TODO: circuit breaker
        match reqwest::get(self.url.as_str()).await {
            Ok(data) => {
                if data.status() != StatusCode::OK {
                    return Err(FeedErr::new(
                        Some(data.status().as_u16()),
                        data.text().await.unwrap_or("".to_string()),
                    ));
                }

                match data.bytes().await {
                    Ok(bytes) => {
                        match serde_json::from_slice::<CoinbasePriceDataResponse>(
                            bytes.iter().as_slice(),
                        ) {
                            Ok(x) => match x.data.rates.get("USD") {
                                None => Err(FeedErr::new(None, String::from("No data"))),
                                Some(value) => match value.parse::<f64>() {
                                    Ok(f) => Ok(f),
                                    Err(e) => Err(FeedErr::new(None, e.to_string())),
                                },
                            },
                            Err(e) => Err(FeedErr::new(None, e.to_string())),
                        }
                    }
                    Err(e) => {
                        error!("Error parsing CoinbasePriceFeed: {}", e);
                        Err(FeedErr::new(
                            e.status().map(|sc| sc.as_u16()),
                            e.to_string(),
                        ))
                    }
                }
            }
            Err(e) => {
                error!("Error fetching CoinbasePriceFeed: {}", e);
                Err(FeedErr::new(
                    e.status().map(|sc| sc.as_u16()),
                    e.to_string(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::upstream::price_feed::{CoinbasePriceFeed, PriceFeed};
    use log::info;

    //#[tokio::test]
    #[ignore]
    // uncomment to quickly smoke coinbase price upstream
    async fn test_coinbase_price_feed_integration() {
        // setup
        log4rs::init_file("conf/log4rs.yml", Default::default()).expect("logging init failed");

        // given
        let url = String::from("https://api.coinbase.com/v2/exchange-rates?currency=BTC");
        let mut feed = CoinbasePriceFeed::new(url);

        // when
        let price = feed.fetch().await.expect("price expected");

        // then
        info!("{:?}", price);
        assert_eq!(price > 0.0, true);
    }
}

// TODO implement trait for Kraken, Binance, etc
