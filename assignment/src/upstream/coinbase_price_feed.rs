use crate::upstream::price_feed::{FeedErr, PriceFeed};
use async_trait::async_trait;
use log::error;
use reqwest::StatusCode;
use serde::Deserialize;
use std::collections::HashMap;

pub struct CoinbasePriceFeed {
    url: String,
}

impl CoinbasePriceFeed {
    pub fn new(url: String) -> CoinbasePriceFeed {
        CoinbasePriceFeed { url }
    }
}

#[derive(Deserialize, Debug)]
struct CoinbasePriceDataResponse {
    data: CoinbasePriceDataData,
}

#[derive(Deserialize, Debug)]
struct CoinbasePriceDataData {
    rates: HashMap<String, String>,
}

#[async_trait]
impl PriceFeed for CoinbasePriceFeed {
    async fn fetch(&self) -> Result<f64, FeedErr> {
        // TODO: introduce circuit breaker here, potentially Coinbase can ban if we will be persistent enough
        match reqwest::get(self.url.as_str()).await {
            Ok(data) => {
                if data.status() != StatusCode::OK {
                    return Err(FeedErr::new(
                        Some(data.status().as_u16()),
                        data.text().await.unwrap_or("".to_string()),
                    ));
                }

                match data.bytes().await {
                    Ok(bytes) => {
                        match serde_json::from_slice::<CoinbasePriceDataResponse>(
                            bytes.iter().as_slice(),
                        ) {
                            Ok(x) => match x.data.rates.get("USD") {
                                None => Err(FeedErr::new(None, String::from("No data"))),
                                Some(value) => match value.parse::<f64>() {
                                    Ok(f) => Ok(f),
                                    Err(e) => Err(FeedErr::new(None, e.to_string())),
                                },
                            },
                            Err(e) => Err(FeedErr::new(None, e.to_string())),
                        }
                    }
                    Err(e) => {
                        error!("Error parsing CoinbasePriceFeed: {}", e);
                        Err(FeedErr::new(
                            e.status().map(|sc| sc.as_u16()),
                            e.to_string(),
                        ))
                    }
                }
            }
            Err(e) => {
                error!("Error fetching CoinbasePriceFeed: {}", e);
                Err(FeedErr::new(
                    e.status().map(|sc| sc.as_u16()),
                    e.to_string(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::upstream::coinbase_price_feed::CoinbasePriceFeed;
    use crate::upstream::price_feed::PriceFeed;
    use log::info;

    //#[tokio::test]
    #[ignore]
    // uncomment to quickly smoke coinbase price upstream
    async fn test_coinbase_price_feed_integration() {
        // setup
        log4rs::init_file("log4rs.yml", Default::default()).expect("logging init failed");

        // given
        let url = String::from("https://api.coinbase.com/v2/exchange-rates?currency=BTC");
        let feed = CoinbasePriceFeed::new(url);

        // when
        let price = feed.fetch().await.expect("price expected");

        // then
        info!("{:?}", price);
        assert_eq!(price > 0.0, true);
    }
}
