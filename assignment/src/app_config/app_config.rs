use crate::index_collector::index_collector::{Asset, FeedId, SmoothingAlgorithm, Source};
use etcd_client::{Client, KeyValue};
use log::{error, info};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::env;

pub const LOCAL: &'static str = "local";
pub const QA: &'static str = "qa";
const TESTNET: &'static str = "testnet";
const PROD: &'static str = "prod";

pub fn get_config_key() -> String {
    let env = get_env();
    info!("Using ENV: {}", env);
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

    from_etcd(url, key).await
}

fn get_env() -> String {
    env::var("ENV").unwrap_or(LOCAL.to_string())
}

pub fn from_key_value(kv: &KeyValue) -> Result<IndexCollectorAppConfig, String> {
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

pub async fn from_etcd(url: String, key: String) -> Result<IndexCollectorAppConfig, String> {
    match Client::connect([&url], None).await {
        Ok(mut client) => match client.get(key.as_bytes(), None).await {
            Ok(resp) => match resp.kvs().first() {
                None => Err(format!("No keys found for {}", key)),
                Some(kv) => from_key_value(kv),
            },
            Err(e) => Err(format!("Error reading {} from etcd: {}, {:?}", key, url, e)),
        },
        Err(e) => Err(format!("Error connecting to etcd: {}, {:?}", url, e)),
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct IndexCollectorAppConfig {
    #[serde(alias = "priceFeeds")]
    pub price_feeds: HashMap<Asset, Vec<PriceFeedConfig>>,
    pub downstream: DownstreamConfig,
}

impl IndexCollectorAppConfig {
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
