use crate::app_config::app_config::{AppConfig, FeedConfig, OrderBookCollectorConfig};
use crate::upstream::order_book_feed::OrderBook;
use log::{debug, info};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use strum_macros::Display;
use crate::infra::clock::Clock;

#[derive(Debug, Clone, Copy, Deserialize, Display, PartialEq, Eq, Hash)]
pub enum Source {
    Binance,
    Uniswap,
}

pub type Asset = String;

#[derive(Debug, PartialEq)]
pub enum Side {
    BUY,
    SELL,
}

pub struct OrderBookCollector {
    feed_config: HashMap<Source, FeedConfig>,
    config: OrderBookCollectorConfig,
    state: HashMap<Source, HashMap<Asset, OrderBook>>,
    clock: Arc<dyn Clock>,
}

impl OrderBookCollector {
    pub fn new(cfg: AppConfig, clock: Arc<dyn Clock>) -> Self {
        let mut collector = OrderBookCollector {
            feed_config: Default::default(),
            config: Default::default(),
            state: Default::default(),
            clock
        };

        collector.init(cfg);

        collector
    }

    pub fn init(&mut self, config: AppConfig) {
        let mut new_config = HashMap::new();
        for cfg in config.feeds {
            new_config.insert(cfg.key(), cfg.clone());
            if let Some(existing) = self.feed_config.get(&cfg.key()) {
                if existing.eq(&cfg) {
                    info!("Skip config change for {}", cfg.key());
                    continue;
                }
            }

            if let Some(old) = self.feed_config.insert(cfg.key(), cfg.clone()) {
                info!(
                    "Old config {:?} replaced with new {:?} for key {}",
                    old,
                    cfg,
                    cfg.key()
                );
            }
        }

        // remove not existing settings
        self.feed_config.retain(|k, _| new_config.contains_key(k));
        self.state.retain(|k, _| new_config.contains_key(k));
        self.config = config.ob_collector_config
    }

    pub fn collect_order_book(&mut self, order_book: OrderBook) -> bool {
        let ts =  self.clock.current_timestamp();
        if ts - order_book.timestamp > self.config.time_diff {
            debug!(
                "Skipping order book from {} for {}: too old, timestamp: {}, current: {}, diff: {}",
                order_book.source,
                order_book.asset,
                order_book.timestamp,
                ts,
                ts - order_book.timestamp
            );
            return false;
        }

        info!("collected order book for {:?} {:?}", order_book.source, order_book.asset);

        let inner_map = self
            .state
            .entry(order_book.source)
            .or_insert_with(HashMap::new);
        inner_map.insert(order_book.asset.clone(), order_book);
        
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_config::app_config::{AppConfig, FeedConfig, OrderBookCollectorConfig};
    use crate::upstream::order_book_feed::{OrderBook, OrderBookLevel};
    use Source::Binance;
    use crate::infra::clock::{Clock, FixedClock, SystemClock};

    fn given_config() -> AppConfig {
        let feed_cfg = FeedConfig {
            source: Binance,
            url_pattern: "mock".to_string(),
            enabled: true,
        };
        AppConfig {
            feeds: vec![feed_cfg],
            ob_collector_config: OrderBookCollectorConfig {
                time_diff: 1000,
                enabled: true
            },
        }
    }

    fn given_order_book(price: f64, timestamp: i64, asset: Asset) -> OrderBook {
        OrderBook {
            source: Binance,
            asset: asset.clone(),
            timestamp,
            bids: vec![OrderBookLevel { price, qty: 1.0 }],
            asks: vec![OrderBookLevel {
                price: price + 1.0,
                qty: 1.0,
            }],
        }
    }

    #[test]
    fn test_init_and_collect_order_book() {
        // given
        let config = given_config();
        let clock = Arc::new(SystemClock::new());
        let mut collector = OrderBookCollector::new(config.clone(), clock.clone());
        let order_book = given_order_book(100.0, clock.current_timestamp(), Asset::from("ETHUSDT"));

        // when
        let result = collector.collect_order_book(order_book.clone());

        // then
        assert_eq!(result, true);

        assert_eq!(collector.state.len(), 1);
        let order_book = collector
            .state
            .get(&Binance)
            .unwrap()
            .get("ETHUSDT")
            .unwrap();
        assert_eq!(order_book.bids.len(), 1);
        assert_eq!(
            order_book.bids.get(0).unwrap().price,
            order_book.bids.get(0).unwrap().price
        );
        assert_eq!(
            order_book.bids.get(0).unwrap().qty,
            order_book.bids.get(0).unwrap().qty
        );
        assert_eq!(order_book.asks.len(), 1);
        assert_eq!(
            order_book.asks.get(0).unwrap().price,
            order_book.asks.get(0).unwrap().price
        );
        assert_eq!(
            order_book.asks.get(0).unwrap().qty,
            order_book.asks.get(0).unwrap().qty
        );
    }

    #[test]
    fn test_init_and_collect_order_stale_book() {
        // given
        let config = given_config();
        let clock = Arc::new(FixedClock::new());
        let mut collector = OrderBookCollector::new(config.clone(), clock.clone());
        let order_book = given_order_book(100.0, clock.current_timestamp() - config.ob_collector_config.time_diff - 1, Asset::from("ETHUSDT"));

        // when
        let result = collector.collect_order_book(order_book.clone());

        // then
        assert_eq!(result, false);
        assert_eq!(collector.state.is_empty(), true);
    }
}
