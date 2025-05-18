use crate::upstream::price_feed::{FeedErr, PriceEvent, PriceFeed};
use async_trait::async_trait;
use futures_util::stream::SplitStream;
use futures_util::StreamExt;
use log::{error, info};
use reqwest::Url;
use serde::Deserialize;
use serde_json::from_slice;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

pub struct BinancePriceFeed {
    url: String,
    read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl BinancePriceFeed {
    pub async fn new(url: String) -> BinancePriceFeed {
        let u = Url::parse(url.as_str()).unwrap();
        let (ws_stream, _) = connect_async(u).await.expect("Failed to connect");
        println!("Connected to Binance WebSocket");

        let (_, read) = ws_stream.split();

        BinancePriceFeed { url, read }
    }
}

#[derive(Debug, Deserialize)]
pub struct AggTrade {
    pub p: String, // Price
    pub T: i64,    // Trade time
}

#[async_trait]
impl PriceFeed for BinancePriceFeed {
    async fn fetch(&mut self) -> Result<Option<PriceEvent>, FeedErr> {
        if let Some(msg) = self.read.next().await {
            match msg {
                Ok(msg) => match msg.into_text() {
                    Ok(x) => match from_slice::<AggTrade>(x.as_bytes()) {
                        Ok(agg_trade) => match agg_trade.p.parse::<f64>() {
                            Ok(_) => {
                                return Ok(Some(PriceEvent {
                                    price: agg_trade.p.parse::<f64>().unwrap(),
                                    timestamp: agg_trade.T,
                                }))
                            }
                            Err(e) => {
                                error!("Error parsing price: {}", e);
                            }
                        },
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
    use crate::upstream::binance_price_feed::BinancePriceFeed;
    use crate::upstream::price_feed::PriceFeed;
    use log::info;

    //#[tokio::test]
    #[ignore]
    // uncomment to quickly smoke coinbase price upstream
    async fn test_binance_price_feed_integration() {
        // setup
        log4rs::init_file("log4rs.yml", Default::default()).expect("logging init failed");

        // given
        let url = String::from("wss://stream.binance.com:9443/ws/ethusdt@aggTrade");
        let mut feed = BinancePriceFeed::new(url);

        // when
        let price = feed.await.fetch().await.expect("price expected");

        // then
        info!("{:?}", price);
        assert_eq!(price.unwrap().price > 0.0, true);
    }
}
