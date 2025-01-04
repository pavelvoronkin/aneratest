use std::collections::HashMap;
use crossbeam_channel::Receiver;
use log::{debug, error, info};
use tokio::task::JoinHandle;
use crate::app_config::app_config::{DownstreamConfig, PriceFeedConfig};
use crate::index_collector::index_collector::{Asset, IndexCollector};
use crate::index_collector::processor::ProcessorMessage;

pub enum DownstreamMessage {
    Index(f64, Asset),
    Stop,
}

pub fn start(
    receiver: Receiver<DownstreamMessage>,
    downstream_config: DownstreamConfig
) -> JoinHandle<()> {
    let name = String::from("downstream_sender");
    tokio::spawn(async move {
        info!("started {}", name);
        let mut stop_flag = false;
        loop {
            match receiver.try_recv() {
                Ok(DownstreamMessage::Index(index_price, asset)) => {
                    let url = format!("{}?price={}&asset={}", downstream_config.url.as_str(), index_price, asset.as_str());
                    match reqwest::get(&url).await {
                        Ok(_) => {
                            info!("Index price sent to url: {} for asset {}", url, asset);
                        }
                        Err(e) => {
                            error!("Error sending index price to url: {} for asset {}, {:?}", url, asset, e);
                        }
                    }
                }
                Ok(DownstreamMessage::Stop) => {
                    info!("{} received stop", name);
                    stop_flag = true;
                }
                Err(_) => {}
            }

            if stop_flag && receiver.is_empty() {
                break;
            }
        }
        info!("{} stopped", name);
    })
}

