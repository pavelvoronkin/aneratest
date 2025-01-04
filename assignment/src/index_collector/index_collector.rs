use crate::app_config::app_config::{IndexCollectorAppConfig, PriceFeedConfig};
use crate::index_collector::smoothing::{EMASmoothing, SMASmoothing, Smoothing};
use log::info;
use serde::Deserialize;
use std::collections::HashMap;
use strum_macros::Display;

// Enum for smoothing algorithms
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Display)]
pub enum SmoothingAlgorithm {
    SMA,
    EMA,
}

// Enum for smoothing algorithms
#[derive(Debug, Clone, Copy, Deserialize, Display, PartialEq)]
pub enum Source {
    Coinbase,
    Kraken,
}

pub type Asset = String;
pub type FeedId = String;

pub struct IndexCollector {
    config: HashMap<FeedId, PriceFeedConfig>,
    smoothing: HashMap<FeedId, Box<dyn Smoothing>>,
    state: HashMap<FeedId, f64>,
    index: HashMap<Asset, f64>,
}

impl IndexCollector {
    pub fn new(map: HashMap<Asset, Vec<PriceFeedConfig>>) -> Self {
        let mut collector = IndexCollector {
            config: Default::default(),
            smoothing: Default::default(),
            state: Default::default(),
            index: Default::default(),
        };

        collector.init(map);

        collector
    }

    pub fn init(&mut self, map: HashMap<Asset, Vec<PriceFeedConfig>>) {
        let mut new_config = HashMap::new();
        for (_, price_feeds) in map {
            for cfg in price_feeds {
                new_config.insert(cfg.key(), cfg.clone());

                if let Some(existing) = self.config.get(&cfg.key()) {
                    if existing.eq(&cfg) {
                        info!("Skip config change for {}", cfg.key());
                        continue;
                    }
                }

                if let Some(old) = self.config.insert(cfg.key(), cfg.clone()) {
                    info!(
                        "Old config {:?} replaced with new {:?} for key {}",
                        old,
                        cfg,
                        cfg.key()
                    );
                }

                if let Some(smoothing_algorithm) = cfg.smoothing {
                    if let Some(_) = self.smoothing.insert(
                        cfg.key(),
                        match smoothing_algorithm {
                            SmoothingAlgorithm::SMA => Box::new(SMASmoothing::default()),
                            SmoothingAlgorithm::EMA => Box::new(EMASmoothing::default()),
                        },
                    ) {
                        info!(
                            "Old smoothing replaced with new {} for key {}",
                            smoothing_algorithm,
                            cfg.key()
                        );
                    }
                }
            }
        }

        // remove not existing settings
        self.config.retain(|k, _| new_config.contains_key(k));
        self.smoothing.retain(|k, _| new_config.contains_key(k));
    }

    pub fn collect_price(&mut self, price: f64, asset: &Asset, source: &Source) {
        let feed_id: FeedId = format!("{}_{}", source, asset);
        let smoothed_price = match self.smoothing.get_mut(&feed_id) {
            None => price,
            Some(x) => x.smooth(price),
        };
        self.state.insert(feed_id, smoothed_price);
    }

    pub fn get_index_price(&self, asset: &Asset) -> f64 {
        let mut weighted = 0.0;
        for (feed_id, price) in &self.state {
            if feed_id.contains(asset) {
                let weight = self.config.get(feed_id).expect("config expected").weight;
                weighted += price * weight as f64 / 100.0;
            }
        }
        weighted
    }
}

#[cfg(test)]
mod tests {
    use crate::app_config::app_config::PriceFeedConfig;
    use crate::index_collector::index_collector::{
        Asset, IndexCollector, SmoothingAlgorithm, Source,
    };
    use std::collections::HashMap;
    use crate::index_collector::index_collector::SmoothingAlgorithm::{EMA, SMA};

    #[test]
    fn test_index_with_smoothing() {
        // given
        let mut price_feeds = HashMap::new();
        let asset1 = "BTC".to_string();
        let asset2 = "ETH".to_string();
        price_feeds.insert(
            asset1.clone(),
            vec![
                PriceFeedConfig {
                    source: Source::Coinbase,
                    asset: asset1.clone(),
                    smoothing: Some(SmoothingAlgorithm::SMA),
                    url_pattern: "".to_string(),
                    weight: 60,
                    enabled: true,
                    fail_count_warn: None,
                },
                PriceFeedConfig {
                    source: Source::Kraken,
                    asset: asset1.clone(),
                    smoothing: Some(SmoothingAlgorithm::SMA),
                    url_pattern: "".to_string(),
                    weight: 40,
                    enabled: true,
                    fail_count_warn: None,
                },
            ],
        );
        price_feeds.insert(
            asset2.clone(),
            vec![
                PriceFeedConfig {
                    source: Source::Coinbase,
                    asset: asset2.clone(),
                    smoothing: Some(SmoothingAlgorithm::EMA),
                    url_pattern: "".to_string(),
                    weight: 60,
                    enabled: true,
                    fail_count_warn: None,
                },
                PriceFeedConfig {
                    source: Source::Kraken,
                    asset: asset2.clone(),
                    smoothing: Some(SmoothingAlgorithm::EMA),
                    url_pattern: "".to_string(),
                    weight: 40,
                    enabled: true,
                    fail_count_warn: None,
                },
            ],
        );

        // when
        let mut collector = IndexCollector::new(price_feeds);

        collector.collect_price(10.0, &asset1, &Source::Coinbase);
        collector.collect_price(20.0, &asset1, &Source::Coinbase);

        collector.collect_price(30.0, &asset1, &Source::Kraken);
        collector.collect_price(40.0, &asset1, &Source::Kraken);

        // then: (10+20)/2 * 0.6 + (30+40)/2 * 0.4
        assert_eq!(collector.get_index_price(&asset1), 23.0);
    }

    #[test]
    fn test_index_without_smoothing() {
        // given
        let mut price_feeds = HashMap::new();
        let asset1 = "BTC".to_string();
        let asset2 = "ETH".to_string();
        price_feeds.insert(
            asset1.clone(),
            vec![
                PriceFeedConfig {
                    source: Source::Coinbase,
                    asset: asset1.clone(),
                    smoothing: None,
                    url_pattern: "".to_string(),
                    weight: 60,
                    enabled: true,
                    fail_count_warn: None,
                },
                PriceFeedConfig {
                    source: Source::Kraken,
                    asset: asset1.clone(),
                    smoothing: None,
                    url_pattern: "".to_string(),
                    weight: 40,
                    enabled: true,
                    fail_count_warn: None,
                },
            ],
        );

        // when
        let mut collector = IndexCollector::new(price_feeds);

        collector.collect_price(10.0, &asset1, &Source::Coinbase);
        collector.collect_price(20.0, &asset1, &Source::Coinbase);

        collector.collect_price(30.0, &asset1, &Source::Kraken);
        collector.collect_price(40.0, &asset1, &Source::Kraken);

        // then: 20 * 0.6 + 40 * 0.4
        assert_eq!(collector.get_index_price(&asset1), 28.0);
    }

    #[test]
    fn test_removing_old_config() {
        // given
        let mut price_feeds = HashMap::new();
        let asset1 = "BTC".to_string();
        let asset2 = "ETH".to_string();
        price_feeds.insert(
            asset1.clone(),
            vec![
                PriceFeedConfig {
                    source: Source::Coinbase,
                    asset: asset1.clone(),
                    smoothing: Some(SMA),
                    url_pattern: "".to_string(),
                    weight: 60,
                    enabled: true,
                    fail_count_warn: None,
                },
            ],
        );
        price_feeds.insert(
            asset2.clone(),
            vec![
                PriceFeedConfig {
                    source: Source::Coinbase,
                    asset: asset2.clone(),
                    smoothing: Some(SMA),
                    url_pattern: "".to_string(),
                    weight: 60,
                    enabled: true,
                    fail_count_warn: None,
                },
            ],
        );

        // and
        let mut collector = IndexCollector::new(price_feeds);

        // when: config updated and old feed removed
        let mut new_price_feeds = HashMap::new();
        new_price_feeds.insert(
            asset1.clone(),
            vec![
                PriceFeedConfig {
                    source: Source::Coinbase,
                    asset: asset1.clone(),
                    smoothing: Some(EMA),
                    url_pattern: "".to_string(),
                    weight: 60,
                    enabled: true,
                    fail_count_warn: None,
                },
            ],
        );
        collector.init(new_price_feeds);

        // then
        assert_eq!(collector.config.get("Coinbase_BTC").is_some(), true);
        assert_eq!(collector.config.get("Coinbase_ETH").is_none(), true);
        assert_eq!(collector.smoothing.get("Coinbase_BTC").is_some(), true);
        assert_eq!(collector.smoothing.get("Coinbase_ETH").is_none(), true);
    }
}
