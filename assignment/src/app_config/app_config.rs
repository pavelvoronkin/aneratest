use crate::index_collector::index_collector::{Asset, FeedId, SmoothingAlgorithm, Source};
use crate::upstream::price_feed::FeedErr;
use log::{error, info};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::{env, fs};

pub const LOCAL: &'static str = "local";
pub const QA: &'static str = "qa";
const TESTNET: &'static str = "testnet";
const PROD: &'static str = "prod";

pub fn get_config_file_path() -> String {
    let env = get_env();
    match env.as_str() {
        QA => String::from("conf/app_config.qa.json"),
        TESTNET => String::from("conf/app_config.testnet.json"),
        PROD => String::from("conf/app_config.prod.json"),
        _ => String::from("conf/app_config.local.json"),
    }
}

pub fn get_app_config(config: Option<String>) -> IndexCollectorAppConfig {
    let config_file = config.unwrap_or_else(|| get_config_file_path());

    info!("Using configuration file from {0}", config_file);

    IndexCollectorAppConfig::from_file(config_file)
}

pub fn get_env() -> String {
    env::var("ENV").unwrap_or(LOCAL.to_string())
}

#[derive(Deserialize, Debug)]
pub struct IndexCollectorAppConfig {
    #[serde(alias = "priceFeeds")]
    pub price_feeds: HashMap<Asset, Vec<PriceFeedConfig>>,
}

impl IndexCollectorAppConfig {
    pub fn from_file(path: String) -> IndexCollectorAppConfig {
        let config = fs::read_to_string(path.clone())
            .expect(format!("Failed to read app_config file: {}", path).as_str());

        let config: IndexCollectorAppConfig = serde_json::from_str(&config)
            .expect(format!("Failed to parse app_config file: {}", path).as_str());

        match config.validate() {
            Ok(_) => config,
            Err(validation_error) => {
                error!("Failed to validate app_config: {}", validation_error);
                panic!("{}", validation_error);
            }
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
#[derive(Debug, Clone, Deserialize)]
pub struct PriceFeedConfig {
    pub source: Source,
    // TODO: support list of assets
    pub asset: Asset,
    pub smoothing: Option<SmoothingAlgorithm>,
    #[serde(alias = "urlPattern")]
    pub url_pattern: String,
    pub weight: u8,
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

#[cfg(test)]
mod tests {
    use crate::app_config::app_config::{IndexCollectorAppConfig, PriceFeedConfig};
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
                    fail_count_warn: None,
                },
                PriceFeedConfig {
                    source: Source::Kraken,
                    asset: "BTC".to_string(),
                    smoothing: Some(SmoothingAlgorithm::SMA),
                    url_pattern: "".to_string(),
                    weight: 20,
                    fail_count_warn: None,
                },
            ],
        );

        // when
        let config = IndexCollectorAppConfig { price_feeds };

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
                    fail_count_warn: None,
                },
                PriceFeedConfig {
                    source: Source::Coinbase,
                    asset: "BTC".to_string(),
                    smoothing: Some(SmoothingAlgorithm::SMA),
                    url_pattern: "".to_string(),
                    weight: 20,
                    fail_count_warn: None,
                },
            ],
        );

        // when
        let config = IndexCollectorAppConfig { price_feeds };

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
            fail_count_warn: None,
        };

        // when then
        assert_eq!(
            cfg.url().as_str(),
            "https://api.coinbase.com/v2/exchange-rates?currency=BTC"
        );
    }
}
