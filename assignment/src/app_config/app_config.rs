use crate::downstream::downstream_sender::DownstreamMessage;
use crate::index_collector::index_collector::{Asset, FeedId, SmoothingAlgorithm, Source};
use crate::index_collector::processor::ProcessorMessage;
use crate::persistence::persistence_sender::PersisterMessage;
use crate::upstream::price_feed::PriceFeedManagerMessage;
use crossbeam_channel::{Receiver, Sender};
use etcd_client::Client;
use log::{debug, error, info};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use std::{env, fs};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub const LOCAL: &'static str = "local";
pub const QA: &'static str = "qa";
const TESTNET: &'static str = "testnet";
const PROD: &'static str = "prod";

pub enum ConfigPollerMessage {
    Stop,
}

pub fn start_config_poller_task(
    initial_config: IndexCollectorAppConfig,
    upstream_tx: UnboundedSender<PriceFeedManagerMessage>,
    prc_tx: Sender<ProcessorMessage>,
    persister_tx: Sender<PersisterMessage>,
    downstream_tx: UnboundedSender<DownstreamMessage>,
    poller_rx: Receiver<ConfigPollerMessage>,
    config: Option<String>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Config poller started");
        let mut current = initial_config.clone();
        loop {
            match poller_rx.try_recv() {
                Ok(_) => {
                    break;
                }
                Err(_) => {}
            }

            /*match get_app_config(config.clone(), ) {
                Ok(config) => {
                    if !current.eq(&config) {
                        if let Err(e) =
                            upstream_tx.send(PriceFeedManagerMessage::ConfigChange(config.clone()))
                        {
                            error!("Error sending config change to processor: {}", e);
                        }

                        if let Err(e) = prc_tx.send(ProcessorMessage::ConfigChange(config.clone()))
                        {
                            error!("Error sending config change to processor: {}", e);
                        }
                        if let Err(e) =
                            persister_tx.send(PersisterMessage::ConfigChange(config.clone()))
                        {
                            error!("Error sending config change to processor: {}", e);
                        }
                        if let Err(e) =
                            downstream_tx.send(DownstreamMessage::ConfigChange(config.clone()))
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
            }*/

            sleep(Duration::from_secs(1)).await;
        }
        info!("Config poller stopped");
    })
}

pub fn get_config_key() -> String {
    let env = get_env();
    match env.as_str() {
        QA => String::from("index_collector/qa"),
        TESTNET => String::from("index_collector/testnet"),
        PROD => String::from("index_collector/prod"),
        _ => String::from("index_collector/local"),
    }
}

pub async fn get_app_config(
    config_key: Option<String>,
    etcd_url: Option<String>,
) -> Result<IndexCollectorAppConfig, String> {
    let key = config_key.unwrap_or_else(|| get_config_key());
    let url = etcd_url.unwrap_or_else(|| "127.0.0.1:2379".to_string());

    info!("Using configuration file from key {}, url {}", key, url);

    IndexCollectorAppConfig::from_etcd(url, key).await
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

    pub async fn from_etcd(url: String, key: String) -> Result<IndexCollectorAppConfig, String> {
        match Client::connect([&url], None).await {
            Ok(mut client) => match client.get(key.as_bytes(), None).await {
                Ok(resp) => match resp.kvs().first() {
                    None => Err(format!("No keys found for {}", key)),
                    Some(kv) => {
                        match serde_json::from_slice::<IndexCollectorAppConfig>(kv.value()) {
                            Ok(config) => match config.validate() {
                                Ok(_) => Ok(config),
                                Err(validation_error) => {
                                    error!("Failed to validate app_config: {}", validation_error);
                                    Err(validation_error)
                                }
                            },
                            Err(e) => Err(format!(
                                "Failed to parse app config: {} {}",
                                kv.value_str().unwrap_or("unparseable string"),
                                e
                            )),
                        }
                    }
                },
                Err(e) => Err(format!("Error reading {} from etcd: {}, {:?}", key, url, e)),
            },
            Err(e) => Err(format!("Error connecting to etcd: {}, {:?}", url, e)),
        }

        /*client.get("foo", None).await;
        match fs::read_to_string(key.clone()) {
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
        }*/
    }

    fn validate(&self) -> Result<(), String> {
        for (_, feeds) in &self.price_feeds {
            let mut total = 0;
            let mut set = HashSet::new();
            for feed in feeds {
                if feed.weight < 1 {
                    return Err(format!(
                        "Weight should be at least 1 for source {}",
                        feed.source
                    ));
                }

                if feed.weight > 100 {
                    return Err(format!(
                        "Weight should be 100 max for source {}",
                        feed.source
                    ));
                }

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
    fn test_min_weight() {
        // given
        let mut price_feeds = HashMap::new();
        price_feeds.insert(
            Asset::from_str("BTC").unwrap(),
            vec![PriceFeedConfig {
                source: Source::Coinbase,
                asset: "BTC".to_string(),
                smoothing: Some(SmoothingAlgorithm::SMA),
                url_pattern: "".to_string(),
                weight: 0,
                enabled: true,
                fail_count_warn: None,
            }],
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
            assert_eq!(
                validation_error,
                "Weight should be at least 1 for source Coinbase"
            );
        } else {
            panic!("Error expected");
        }
    }

    #[test]
    fn test_max_weight() {
        // given
        let mut price_feeds = HashMap::new();
        price_feeds.insert(
            Asset::from_str("BTC").unwrap(),
            vec![PriceFeedConfig {
                source: Source::Coinbase,
                asset: "BTC".to_string(),
                smoothing: Some(SmoothingAlgorithm::SMA),
                url_pattern: "".to_string(),
                weight: 101,
                enabled: true,
                fail_count_warn: None,
            }],
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
            assert_eq!(
                validation_error,
                "Weight should be 100 max for source Coinbase"
            );
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
