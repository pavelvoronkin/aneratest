use crate::order_book::order_book_collector::Source::Binance;
use crate::upstream::order_book_feed::{FeedErr, OrderBook, OrderBookFeed, OrderBookLevel};
use async_trait::async_trait;
use futures_util::stream::SplitStream;
use futures_util::StreamExt;
use log::{error, info};
use reqwest::Url;
use serde::Deserialize;
use serde_json::from_slice;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

pub struct BinanceOrderBookFeed {
    url: String,
    read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl BinanceOrderBookFeed {
    pub async fn new(url: String) -> BinanceOrderBookFeed {
        let u = Url::parse(url.as_str()).unwrap();
        let (ws_stream, _) = connect_async(u).await.expect("Failed to connect");
        println!("Connected to Binance WebSocket");

        let (_, read) = ws_stream.split();

        BinanceOrderBookFeed { url, read }
    }
}

fn parse(depth_update: DepthUpdate) -> Result<Option<OrderBook>, FeedErr> {
    let mut bids = Vec::<OrderBookLevel>::with_capacity(depth_update.b.len());

    for b in depth_update.b {
        if b.len() != 2 {
            return Err(FeedErr::new(
                Some(400),
                "Invalid bid format in depth update".to_string(),
            ));
        }

        let price;
        match b[0].parse() {
            Ok(p) => {
                price = p;
            }
            Err(_) => {
                return Err(FeedErr::new(Some(400), "Invalid price in bid".to_string()));
            }
        }

        let qty;
        match b[1].parse() {
            Ok(q) => {
                qty = q;
            }
            Err(_) => {
                return Err(FeedErr::new(Some(400), "Invalid qty in bid".to_string()));
            }
        }

        bids.push(OrderBookLevel { price, qty })
    }

    let mut asks = Vec::<OrderBookLevel>::with_capacity(depth_update.a.len());
    for a in depth_update.a {
        if a.len() != 2 {
            return Err(FeedErr::new(
                Some(400),
                "Invalid bid format in depth update".to_string(),
            ));
        }

        let price;
        match a[0].parse() {
            Ok(p) => {
                price = p;
            }
            Err(_) => {
                return Err(FeedErr::new(Some(400), "Invalid price in ask".to_string()));
            }
        }

        let qty;
        match a[1].parse() {
            Ok(q) => {
                qty = q;
            }
            Err(_) => {
                return Err(FeedErr::new(Some(400), "Invalid qty in ask".to_string()));
            }
        }

        asks.push(OrderBookLevel { price, qty })
    }

    Ok(Some(OrderBook {
        source: Binance,
        asset: depth_update.s,
        timestamp: depth_update.E,
        bids,
        asks,
    }))
}

#[derive(Debug, Deserialize)]
pub struct DepthUpdate {
    pub E: i64,              // Event time
    pub s: String,           // Symbol
    pub b: Vec<Vec<String>>, // Bids to be updated [price, qty]
    pub a: Vec<Vec<String>>, // Asks to be updated [price, qty]
}

#[async_trait]
impl OrderBookFeed for BinanceOrderBookFeed {
    async fn fetch(&mut self) -> Result<Option<OrderBook>, FeedErr> {
        if let Some(msg) = self.read.next().await {
            match msg {
                Ok(msg) => match msg.into_text() {
                    Ok(str) => match from_slice::<DepthUpdate>(str.as_bytes()) {
                        Ok(depth_update) => return parse(depth_update),
                        Err(e) => {
                            error!("Error parsing JSON: {}", e);
                        }
                    },
                    Err(e) => {
                        error!("Error parsing text: {}", e);
                    }
                },
                Err(e) => {
                    info!("WebSocket error: {}", e);
                }
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::upstream::binance_order_book_feed::BinanceOrderBookFeed;
    use crate::upstream::order_book_feed::OrderBookFeed;
    use log::info;

    // #[tokio::test]
    #[ignore]
    // uncomment to quickly smoke binance price upstream
    async fn test_binance_depth_feed_integration() {
        // setup
        log4rs::init_file("log4rs.yml", Default::default()).expect("logging init failed");

        // given
        let url = String::from("wss://stream.binance.com/ws/btcusdt@depth");
        let mut feed = BinanceOrderBookFeed::new(url);

        // when
        let price = feed.await.fetch().await.expect("price expected");

        // then
        info!("{:?}", price);
        assert_eq!(price.unwrap().bids.is_empty(), false);
    }
}
