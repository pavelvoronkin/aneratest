use crate::app_config::app_config;
use crate::app_config::control::Control;
use crate::downstream::downstream_sender::DownstreamMessage;
use crate::index_collector::index_collector::{Asset, FeedId, SmoothingAlgorithm, Source};
use crate::index_collector::processor::ProcessorMessage;
use crate::upstream::price_feed::{FeedErr, PriceFeedManagerMessage};
use crossbeam_channel::Sender;
use log::{debug, error, info};
use serde::Deserialize;
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use signal_hook::low_level::exit;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs, thread};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub const LOCAL: &'static str = "local";
pub const QA: &'static str = "qa";
const TESTNET: &'static str = "testnet";
const PROD: &'static str = "prod";

pub fn start_config_poller_task(
    ctrl: Arc<Control>,
    initial_config: IndexCollectorAppConfig,
    upstream_tx: mpsc::UnboundedSender<PriceFeedManagerMessage>,
    prc_snd: Sender<ProcessorMessage>,
    persister_snd: Sender<ProcessorMessage>,
    downstream_snd: Sender<DownstreamMessage>,
    config: Option<String>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Config poller started");
        let mut current = initial_config.clone();
        loop {
            if ctrl.is_stopped() {
                break;
            }

            match get_app_config(config.clone()) {
                Ok(config) => {
                    if !current.eq(&config) {
                        if let Err(e) = upstream_tx.send(PriceFeedManagerMessage::ConfigChange(config.clone()))
                        {
                            error!("Error sending config change to processor: {}", e);
                        }

                        if let Err(e) = prc_snd.send(ProcessorMessage::ConfigChange(config.clone()))
                        {
                            error!("Error sending config change to processor: {}", e);
                        }
                        if let Err(e) =
                            persister_snd.send(ProcessorMessage::ConfigChange(config.clone()))
                        {
                            error!("Error sending config change to processor: {}", e);
                        }
                        if let Err(e) =
                            downstream_snd.send(DownstreamMessage::ConfigChange(config.clone()))
                        {
                            error!("Error sending config change to processor: {}", e);
                        }
                        current = config;
                    } else {
                        debug!("No config changed")
                    }
                }
                Err(e) => {
                    error!("Failed to reload app config: {}", e);
                }
            }

            sleep(Duration::from_secs(1)).await;
        }
        info!("Config poller stopped");
    })
}

pub fn get_config_file_path() -> String {
    let env = get_env();
    match env.as_str() {
        QA => String::from("conf/app_config.qa.json"),
        TESTNET => String::from("conf/app_config.testnet.json"),
        PROD => String::from("conf/app_config.prod.json"),
        _ => String::from("conf/app_config.local.json"),
    }
}

pub fn get_app_config(config: Option<String>) -> Result<IndexCollectorAppConfig, String> {
    let config_file = config.unwrap_or_else(|| get_config_file_path());

    debug!("Using configuration file from {0}", config_file);

    IndexCollectorAppConfig::from_file(config_file)
}

pub fn get_env() -> String {
    env::var("ENV").unwrap_or(LOCAL.to_string())
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct IndexCollectorAppConfig {
    #[serde(alias = "priceFeeds")]
    pub price_feeds: HashMap<Asset, Vec<PriceFeedConfig>>,
    pub downstream: DownstreamConfig,
}

impl IndexCollectorAppConfig {
    pub fn from_file(path: String) -> Result<IndexCollectorAppConfig, String> {
        match fs::read_to_string(path.clone()) {
            Ok(string) => match serde_json::from_str::<IndexCollectorAppConfig>(&string) {
                Ok(config) => match config.validate() {
                    Ok(_) => Ok(config),
                    Err(validation_error) => {
                        error!("Failed to validate app_config: {}", validation_error);
                        Err(validation_error)
                    }
                },
                Err(e) => Err(format!("Failed to parse app config file: {} {}", path, e)),
            },
            Err(_) => Err(format!("Failed to read app_config file: {}", path)),
        }
    }

    fn validate(&self) -> Result<(), String> {
        for (_, feeds) in &self.price_feeds {
            let mut total = 0;
            let mut set = HashSet::new();
            for feed in feeds {
                total = total + feed.weight;
                if !set.insert(feed.source.to_string().clone()) {
                    return Err(format!("Only one source per asset {} allowed", feed.asset));
                }
            }

            if total != 100 {
                return Err(format!(
                    "Total weight {total} is not equal to 100",
                    total = total
                ));
            }
        }

        Ok(())
    }
}

// Price upstream struct to hold data and configurations
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct PriceFeedConfig {
    pub source: Source,
    // TODO: support list of assets
    pub asset: Asset,
    pub smoothing: Option<SmoothingAlgorithm>,
    #[serde(alias = "urlPattern")]
    pub url_pattern: String,
    pub weight: u8,
    pub enabled: bool,
    pub fail_count_warn: Option<u8>,
}

impl PriceFeedConfig {
    pub fn url(&self) -> String {
        self.url_pattern.clone().replace("{{asset}}", &self.asset)
    }

    pub fn key(&self) -> FeedId {
        format!("{}_{}", self.source, self.asset)
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DownstreamConfig {
    pub url: String,
}

#[cfg(test)]
mod tests {
    use crate::app_config::app_config::{
        DownstreamConfig, IndexCollectorAppConfig, PriceFeedConfig,
    };
    use crate::index_collector::index_collector::{Asset, SmoothingAlgorithm, Source};
    use std::collections::HashMap;
    use std::str::FromStr;

    #[test]
    fn test_invalid_weights() {
        // given
        let mut price_feeds = HashMap::new();
        price_feeds.insert(
            Asset::from_str("BTC").unwrap(),
            vec![
                PriceFeedConfig {
                    source: Source::Coinbase,
                    asset: "BTC".to_string(),
                    smoothing: Some(SmoothingAlgorithm::SMA),
                    url_pattern: "".to_string(),
                    weight: 60,
                    enabled: true,
                    fail_count_warn: None,
                },
                PriceFeedConfig {
                    source: Source::Kraken,
                    asset: "BTC".to_string(),
                    smoothing: Some(SmoothingAlgorithm::SMA),
                    url_pattern: "".to_string(),
                    weight: 20,
                    enabled: true,
                    fail_count_warn: None,
                },
            ],
        );

        // when
        let config = IndexCollectorAppConfig {
            price_feeds,
            downstream: DownstreamConfig {
                url: "".to_string(),
            },
        };

        // then
        if let Err(validation_error) = config.validate() {
            assert_eq!(validation_error, "Total weight 80 is not equal to 100");
        } else {
            panic!("Error expected");
        }
    }

    #[test]
    fn test_duplicated_source() {
        // given
        let mut price_feeds = HashMap::new();
        price_feeds.insert(
            Asset::from_str("BTC").unwrap(),
            vec![
                PriceFeedConfig {
                    source: Source::Coinbase,
                    asset: "BTC".to_string(),
                    smoothing: Some(SmoothingAlgorithm::SMA),
                    url_pattern: "".to_string(),
                    weight: 60,
                    enabled: true,
                    fail_count_warn: None,
                },
                PriceFeedConfig {
                    source: Source::Coinbase,
                    asset: "BTC".to_string(),
                    smoothing: Some(SmoothingAlgorithm::SMA),
                    url_pattern: "".to_string(),
                    weight: 20,
                    enabled: true,
                    fail_count_warn: None,
                },
            ],
        );

        // when
        let config = IndexCollectorAppConfig {
            price_feeds,
            downstream: DownstreamConfig {
                url: "".to_string(),
            },
        };

        // then
        if let Err(validation_error) = config.validate() {
            assert_eq!(validation_error, "Only one source per asset BTC allowed");
        } else {
            panic!("Error expected");
        }
    }

    #[test]
    fn test_url_resolution() {
        // given
        let cfg = PriceFeedConfig {
            source: Source::Coinbase,
            asset: "BTC".to_string(),
            smoothing: Some(SmoothingAlgorithm::SMA),
            url_pattern: "https://api.coinbase.com/v2/exchange-rates?currency={{asset}}"
                .to_string(),
            weight: 0,
            enabled: true,
            fail_count_warn: None,
        };

        // when then
        assert_eq!(
            cfg.url().as_str(),
            "https://api.coinbase.com/v2/exchange-rates?currency=BTC"
        );
    }
}
