use crate::order_book::order_book_collector::Source::Uniswap;
use crate::upstream::order_book_feed::{FeedErr, OrderBook, OrderBookFeed};
use async_trait::async_trait;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

pub struct UniswapFeed {}

impl UniswapFeed {
    pub fn new() -> UniswapFeed {
        UniswapFeed {}
    }
}

#[async_trait]
impl OrderBookFeed for UniswapFeed {
    async fn fetch(&mut self) -> Result<Option<OrderBook>, FeedErr> {
        sleep(Duration::from_millis(100)).await;
        Ok(Some(OrderBook {
            source: Uniswap,
            asset: "BTCUSDT".to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as i64,
            bids: vec![],
            asks: vec![],
        }))
    }
}
