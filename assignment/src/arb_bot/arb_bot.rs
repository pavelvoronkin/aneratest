use crate::app_config::app_config::{AppConfig, ArbBotConfig, PriceFeedConfig};
use crate::infra::clock::current_timestamp;
use crate::upstream::price_feed::PriceEvent;
use log::{debug, info};
use serde::Deserialize;
use std::collections::HashMap;
use strum_macros::Display;

#[derive(Debug, Clone, Copy, Deserialize, Display, PartialEq)]
pub enum Source {
    Binance,
    Uniswap,
}

pub type Asset = String;
pub type FeedId = String;

#[derive(Debug, PartialEq)]
pub enum Side {
    BUY,
    SELL,
}

pub struct ArbBot {
    feed_config: HashMap<FeedId, PriceFeedConfig>,
    config: ArbBotConfig,
    state: HashMap<FeedId, PriceEvent>,
    balance: HashMap<Asset, i64>,
    lockout_timestamp: i64,
}

impl ArbBot {
    pub fn new(map: AppConfig) -> Self {
        let mut bot = ArbBot {
            feed_config: Default::default(),
            config: Default::default(),
            state: Default::default(),
            balance: Default::default(),
            lockout_timestamp: 0,
        };

        bot.init(map);

        bot
    }

    pub fn init(&mut self, config: AppConfig) {
        let mut new_config = HashMap::new();
        for (_, price_feeds) in config.price_feeds {
            for cfg in price_feeds {
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
        }

        // remove not existing settings
        self.feed_config.retain(|k, _| new_config.contains_key(k));
        self.state.retain(|k, _| new_config.contains_key(k));
        self.config = config.arb_bot_config
    }

    pub fn collect_price(&mut self, price: PriceEvent, asset: &Asset, source: &Source) {
        let feed_id: FeedId = format!("{}_{}", source, asset);
        self.state.insert(feed_id, price);
    }

    pub fn take_opportunities(&mut self, asset: &Asset) -> Option<(f64, i64, Side)> {
        if !self.config.enabled {
            debug!("ArbBot is disabled");
            return None;
        }

        if current_timestamp() < self.lockout_timestamp {
            debug!("ArbBot is locked out until {}", self.lockout_timestamp);
            return None;
        }

        // TODO: support multiple CEX/DEX exchanges
        let binance: FeedId = format!("{}_{}", Source::Binance, asset);
        let uniswap: FeedId = format!("{}_{}", Source::Uniswap, asset);

        self.take_opportunity_between(&binance, &uniswap, asset)
    }

    fn take_opportunity_between(
        &mut self,
        cex_feed: &FeedId,
        dex_feed: &FeedId,
        asset: &Asset,
    ) -> Option<(f64, i64, Side)> {
        if let Some(cex) = self.state.get(cex_feed) {
            if let Some(dex) = self.state.get(dex_feed) {
                if i64::abs(cex.timestamp - dex.timestamp) > self.config.time_diff as i64 {
                    debug!(
                        "Price feeds are apart in time by more than {}",
                        self.config.time_diff
                    );
                    return None;
                }

                // TODO: use u64, convert price to u64 using symbol precision
                let price_diff = f64::abs(cex.price - dex.price) * 100.0;
                let threshold = price_diff / f64::max(cex.price, dex.price);
                if threshold < self.config.price_threshold as f64 {
                    debug!("Threshold is not triggered {}", self.config.price_threshold);
                    return None;
                }

                let result = if dex.price > cex.price {
                    info!(
                        "Buy on CEX {}:{} to sell on DEX {}:{}, price threshold {}, qty {}",
                        cex_feed, cex.price, dex_feed, dex.price, threshold, self.config.qty
                    );
                    (cex.price, self.config.qty, Side::BUY)
                } else {
                    info!(
                        "Sell on CEX {}:{} to buy on DEX {}:{}, price threshold {}, qty {}",
                        dex_feed, dex.price, cex_feed, cex.price, threshold, self.config.qty
                    );
                    (cex.price, self.config.qty, Side::SELL)
                };

                let trade_time = current_timestamp();
                info!(
                    "ARB P&L: {}, {}, CEX latency {}, DEX latency {}",
                    match result.2 {
                        Side::BUY => self.config.qty,
                        Side::SELL => -self.config.qty,
                    },
                    cex_feed,
                    trade_time - cex.timestamp,
                    trade_time - dex.timestamp
                );

                self.update_balance(asset, &result);

                self.lockout_timestamp = current_timestamp() + self.config.lockout_period as i64;

                return Some(result);
            }
        }
        None
    }

    fn update_balance(&mut self, asset: &Asset, result: &(f64, i64, Side)) {
        match self.balance.get(asset) {
            None => {
                match result.2 {
                    Side::BUY => self.balance.insert(asset.clone(), self.config.qty),
                    Side::SELL => self.balance.insert(asset.clone(), -self.config.qty),
                };
            }
            Some(e) => {
                self.balance.insert(
                    asset.clone(),
                    *e + match result.2 {
                        Side::BUY => self.config.qty,
                        Side::SELL => -self.config.qty,
                    },
                );
            }
        }

        info!(
            "Balance: {} {}",
            self.balance.get(asset).unwrap_or(&0),
            asset
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_config::app_config::{AppConfig, ArbBotConfig, PriceFeedConfig};
    use crate::upstream::price_feed::PriceEvent;
    use std::collections::HashMap;
    use std::thread::sleep;
    use std::time::Duration;

    fn given_config() -> AppConfig {
        let mut price_feeds = HashMap::new();
        let feed_cfg = PriceFeedConfig {
            source: Source::Binance,
            asset: "ETH".to_string(),
            url_pattern: "mock".to_string(),
            enabled: true,
            fail_count_warn: None,
        };
        price_feeds.insert("ETH".to_string(), vec![feed_cfg]);
        AppConfig {
            price_feeds,
            arb_bot_config: ArbBotConfig {
                time_diff: 1000,
                price_threshold: 1,
                qty: 10,
                enabled: true,
                lockout_period: 1000,
            },
        }
    }

    fn given_price_event(price: f64, timestamp: i64) -> PriceEvent {
        PriceEvent { price, timestamp }
    }

    #[test]
    fn test_init_and_collect_price() {
        // given
        let config = given_config();
        let mut bot = ArbBot::new(config.clone());

        // when
        bot.collect_price(
            given_price_event(100.0, 12345),
            &"ETH".to_string(),
            &Source::Binance,
        );

        let feed_id = format!("{}_{}", Source::Binance, "ETH");
        assert_eq!(bot.state.len(), 1);
        assert_eq!(bot.state.get(&feed_id).unwrap().price, 100.0);
        assert_eq!(bot.state.get(&feed_id).unwrap().timestamp, 12345);
    }

    #[test]
    fn test_find_opportunities_no_events() {
        // given
        let config = given_config();

        // when
        let mut bot = ArbBot::new(config);

        // then
        assert_eq!(bot.take_opportunities(&"ETH".to_string()), None);
        assert_eq!(bot.balance.len(), 0);
    }

    #[test]
    fn test_find_opportunities_no_opportunity_price_threshold_not_reached() {
        // given
        let config = given_config();
        let mut bot = ArbBot::new(config);

        // when
        bot.collect_price(
            given_price_event(100.0, 1000),
            &"ETH".to_string(),
            &Source::Binance,
        );
        bot.collect_price(
            given_price_event(101.0, 1000),
            &"ETH".to_string(),
            &Source::Uniswap,
        );

        // then
        assert_eq!(bot.take_opportunities(&"ETH".to_string()), None);
        assert_eq!(bot.balance.len(), 0);
    }

    #[test]
    fn test_find_opportunities_no_opportunity_stale_trade() {
        // given
        let config = given_config();
        let mut bot = ArbBot::new(config);

        // when
        bot.collect_price(
            given_price_event(100.0, 1000),
            &"ETH".to_string(),
            &Source::Binance,
        );
        bot.collect_price(
            given_price_event(102.0, 2001),
            &"ETH".to_string(),
            &Source::Uniswap,
        );

        // then
        assert_eq!(bot.take_opportunities(&"ETH".to_string()), None);
        assert_eq!(bot.balance.len(), 0);
    }

    #[test]
    fn test_find_opportunities_with_buy_opportunity() {
        // given
        let config = given_config();
        let mut bot = ArbBot::new(config);
        let cex_event = given_price_event(100.0, 1000);
        let dex_event = given_price_event(101.1, 1001);

        // when
        bot.collect_price(cex_event, &"ETH".to_string(), &Source::Binance);
        bot.collect_price(dex_event, &"ETH".to_string(), &Source::Uniswap);
        let result = bot.take_opportunities(&"ETH".to_string());

        // then
        assert_eq!(result, Some((100.0, 10, Side::BUY)));

        assert_eq!(bot.balance.len(), 1);
        assert_eq!(bot.balance.get(&"ETH".to_string()), Some(&10));
    }

    #[test]
    fn test_find_opportunities_with_sell_opportunity() {
        // given
        let config = given_config();
        let mut bot = ArbBot::new(config);
        let cex_event = given_price_event(102.0, 1000);
        let dex_event = given_price_event(100.0, 1001);

        // when
        bot.collect_price(cex_event, &"ETH".to_string(), &Source::Binance);
        bot.collect_price(dex_event, &"ETH".to_string(), &Source::Uniswap);
        let result = bot.take_opportunities(&"ETH".to_string());

        // then
        assert_eq!(result, Some((102.0, 10, Side::SELL)));

        assert_eq!(bot.balance.len(), 1);
        assert_eq!(bot.balance.get(&"ETH".to_string()), Some(&-10));
    }

    #[test]
    fn test_find_opportunities_with_opportunity_lockout() {
        // given
        let config = given_config();
        let mut bot = ArbBot::new(config);
        let cex_event = given_price_event(100.0, 1000);
        let dex_event = given_price_event(101.1, 1001);

        // when
        bot.collect_price(cex_event, &"ETH".to_string(), &Source::Binance);
        bot.collect_price(dex_event, &"ETH".to_string(), &Source::Uniswap);

        // then
        assert_eq!(
            bot.take_opportunities(&"ETH".to_string()),
            Some((100.0, 10, Side::BUY))
        );
        assert_eq!(bot.take_opportunities(&"ETH".to_string()), None);

        assert_eq!(bot.balance.get(&"ETH".to_string()), Some(&10));
    }

    #[test]
    fn test_find_opportunities_with_opportunity_after_lockout() {
        // given
        let config = given_config();
        let mut bot = ArbBot::new(config);
        let cex_event = given_price_event(100.0, 1000);
        let dex_event = given_price_event(101.1, 1001);

        // when
        bot.collect_price(cex_event, &"ETH".to_string(), &Source::Binance);
        bot.collect_price(dex_event, &"ETH".to_string(), &Source::Uniswap);
        assert_eq!(
            bot.take_opportunities(&"ETH".to_string()),
            Some((100.0, 10, Side::BUY))
        );

        // and: wait for lockout period to expire
        sleep(Duration::from_millis(1001));

        // then
        assert_eq!(
            bot.take_opportunities(&"ETH".to_string()),
            Some((100.0, 10, Side::BUY))
        );

        assert_eq!(bot.balance.get(&"ETH".to_string()), Some(&20));
    }
}
