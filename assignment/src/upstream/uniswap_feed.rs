use crate::infra::clock::current_timestamp;
use crate::upstream::price_feed::{FeedErr, PriceEvent, PriceFeed};
use async_trait::async_trait;
use std::time::Duration;
use tokio::time::sleep;

pub struct UniswapFeed {
}

impl UniswapFeed {
    pub fn new() -> UniswapFeed {
        UniswapFeed { }
    }
}

#[async_trait]
impl PriceFeed for UniswapFeed {
    async fn fetch(&mut self) -> Result<Option<PriceEvent>, FeedErr> {
        sleep(Duration::from_millis(100)).await;
        Ok(Some(PriceEvent {
            price: 10000.0,
            timestamp: current_timestamp(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::upstream::price_feed::PriceFeed;
    use crate::upstream::uniswap_feed::UniswapFeed;
    use log::info;
    use std::cmp::{Ordering, PartialOrd};

    #[tokio::test]
    // uncomment to quickly smoke uniswap price upstream, for now it's mock
    async fn test_uniswap_price_feed_integration() {
        // setup
        log4rs::init_file("log4rs.yml", Default::default()).expect("logging init failed");

        // given
        let mut feed = UniswapFeed::new();

        // when
        let price = feed.fetch().await.expect("price expected");

        // then
        info!("{:?}", price);
        assert_eq!(price.unwrap().price > 0.0, true);
    }
}
