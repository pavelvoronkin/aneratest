use crate::arb_bot::arb_bot::{Asset, FeedId, Source};
use etcd_client::{Client, KeyValue};
use log::{error, info};
use serde::Deserialize;
use std::collections::HashMap;
use std::env;

pub const LOCAL: &'static str = "local";
pub const QA: &'static str = "qa";
const TESTNET: &'static str = "testnet";
const PROD: &'static str = "prod";

pub fn get_config_key() -> String {
    let env = get_env();
    info!("Using ENV: {}", env);
    match env.as_str() {
        QA => String::from("arb_bot/qa"),
        TESTNET => String::from("arb_bot/testnet"),
        PROD => String::from("arb_bot/prod"),
        _ => String::from("arb_bot/local"),
    }
}

pub async fn get_app_config(
    config_key: Option<String>,
    etcd_url: Option<String>,
) -> Result<AppConfig, String> {
    let key = config_key.unwrap_or_else(|| get_config_key());
    let url = etcd_url.unwrap_or_else(|| "127.0.0.1:2379".to_string());

    info!("Using configuration file from key {}, url {}", key, url);

    from_etcd(url, key).await
}

fn get_env() -> String {
    env::var("ENV").unwrap_or(LOCAL.to_string())
}

pub fn from_key_value(kv: &KeyValue) -> Result<AppConfig, String> {
    match serde_json::from_slice::<AppConfig>(kv.value()) {
        Ok(config) => match config.validate() {
            Ok(()) => Ok(config),
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

pub async fn from_etcd(url: String, key: String) -> Result<AppConfig, String> {
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
pub struct AppConfig {
    #[serde(alias = "priceFeeds")]
    pub price_feeds: HashMap<Asset, Vec<PriceFeedConfig>>,
    #[serde(alias = "arbBot")]
    pub arb_bot_config: ArbBotConfig,
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), String> {
        // TODO: validate config later
        Ok(())
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArbBotConfig {
    pub time_diff: u64,
    pub price_threshold: u64,
    pub qty: i64,
    pub enabled: bool,
    pub lockout_period: u64,
}

impl Default for ArbBotConfig {
    fn default() -> Self {
        ArbBotConfig {
            time_diff: 1000,
            price_threshold: 1000,
            qty: 10,
            lockout_period: 2000,
            enabled: true,
        }
    }
}

// Price upstream struct to hold data and configurations
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct PriceFeedConfig {
    pub source: Source,
    pub asset: Asset,
    #[serde(alias = "urlPattern")]
    pub url_pattern: String,
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

#[cfg(test)]
mod tests {
    use crate::app_config::app_config::PriceFeedConfig;
    use crate::arb_bot::arb_bot::Source;
    use Source::Binance;

    #[test]
    fn test_url_resolution() {
        // given
        let cfg = PriceFeedConfig {
            source: Binance,
            asset: "BTC".to_string(),
            url_pattern: "wss://stream.binance.com:9443/ws/ethusdt@aggTrad".to_string(),
            enabled: true,
            fail_count_warn: None,
        };

        // when then
        assert_eq!(
            cfg.url().as_str(),
            "wss://stream.binance.com:9443/ws/ethusdt@aggTrad"
        );
    }
}
