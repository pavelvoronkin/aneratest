use crate::app_config::app_config::PriceFeedConfig;
use crate::index_collector::smoothing::{EMASmoothing, SMASmoothing, Smoothing};
use serde::Deserialize;
use std::collections::HashMap;
use strum_macros::Display;

// Enum for smoothing algorithms
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum SmoothingAlgorithm {
    SMA,
    EMA,
}

// Enum for smoothing algorithms
#[derive(Debug, Clone, Copy, Deserialize, Display)]
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
        let mut config = HashMap::new();
        let mut smoothing: HashMap<FeedId, Box<dyn Smoothing>> = HashMap::new();

        for (_, vec) in map {
            for x in vec {
                config.insert(x.key(), x.clone());
                if let Some(v) = x.smoothing {
                    smoothing.insert(
                        x.key(),
                        match v {
                            SmoothingAlgorithm::SMA => Box::new(SMASmoothing::default()),
                            SmoothingAlgorithm::EMA => Box::new(EMASmoothing::default()),
                        },
                    );
                }
            }
        }

        IndexCollector {
            config,
            smoothing,
            state: Default::default(),
            index: Default::default(),
        }
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
                    fail_count_warn: None
                },
                PriceFeedConfig {
                    source: Source::Kraken,
                    asset: asset1.clone(),
                    smoothing: Some(SmoothingAlgorithm::SMA),
                    url_pattern: "".to_string(),
                    weight: 40,
                    enabled: true,
                    fail_count_warn: None
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
                    fail_count_warn: None
                },
                PriceFeedConfig {
                    source: Source::Kraken,
                    asset: asset2.clone(),
                    smoothing: Some(SmoothingAlgorithm::EMA),
                    url_pattern: "".to_string(),
                    weight: 40,
                    enabled: true,
                    fail_count_warn: None
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
                    fail_count_warn: None
                },
                PriceFeedConfig {
                    source: Source::Kraken,
                    asset: asset1.clone(),
                    smoothing: None,
                    url_pattern: "".to_string(),
                    weight: 40,
                    enabled: true,
                    fail_count_warn: None
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
}
