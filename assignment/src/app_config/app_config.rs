use crate::order_book::order_book_collector::Source;
use etcd_client::{Client, KeyValue};
use log::{error, info};
use serde::Deserialize;
use std::env;

pub const LOCAL: &'static str = "local";
pub const QA: &'static str = "qa";
const TESTNET: &'static str = "testnet";
const PROD: &'static str = "prod";

pub fn get_config_key() -> String {
    let env = get_env();
    info!("Using ENV: {}", env);
    match env.as_str() {
        QA => String::from("order_book/qa"),
        TESTNET => String::from("order_book/testnet"),
        PROD => String::from("order_book/prod"),
        _ => String::from("order_book/local"),
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
    #[serde(alias = "feeds")]
    pub feeds: Vec<FeedConfig>,
    #[serde(alias = "orderBookCollector")]
    pub ob_collector_config: OrderBookCollectorConfig,
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), String> {
        // TODO: validate config later
        Ok(())
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OrderBookCollectorConfig {
    pub time_diff: i64,
    pub enabled: bool
}

impl Default for OrderBookCollectorConfig {
    fn default() -> Self {
        OrderBookCollectorConfig {
            time_diff: 1000,
            enabled: true,
        }
    }
}

// Price upstream struct to hold data and configurations
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct FeedConfig {
    pub source: Source,
    #[serde(alias = "urlPattern")]
    pub url_pattern: String,
    pub enabled: bool,
}

impl FeedConfig {
    pub fn url(&self) -> String {
        // todo replacements here to customize url
        self.url_pattern.clone()
    }

    pub fn key(&self) -> Source {
        self.source.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::app_config::app_config::FeedConfig;
    use crate::order_book::order_book_collector::Source;
    use Source::Binance;

    #[test]
    fn test_url_resolution() {
        // given
        let cfg = FeedConfig {
            source: Binance,
            url_pattern: "wss://stream.binance.com:9443/ws/ethusdt@aggTrade".to_string(),
            enabled: true,
        };

        // when then
        assert_eq!(
            cfg.url().as_str(),
            "wss://stream.binance.com:9443/ws/ethusdt@aggTrade"
        );
    }
}
