use crate::app_config::app_config::{DownstreamConfig, IndexCollectorAppConfig, PriceFeedConfig};
use crate::index_collector::index_collector::{Asset, IndexCollector};
use crate::index_collector::processor::ProcessorMessage;
use crossbeam_channel::{Receiver, TryRecvError};
use log::{debug, error, info};
use reqwest::StatusCode;
use std::collections::HashMap;
use tokio::task::JoinHandle;

pub enum DownstreamMessage {
    Index(f64, Asset),
    ConfigChange(IndexCollectorAppConfig),
    Stop,
}

pub fn start(
    receiver: Receiver<DownstreamMessage>,
    downstream_config: DownstreamConfig,
) -> JoinHandle<()> {
    let name = String::from("downstream_sender");
    tokio::spawn(async move {
        info!("started {}", name);
        let mut stop_flag = false;
        let mut current_config = downstream_config.clone();
        loop {
            match receiver.try_recv() {
                Ok(DownstreamMessage::Index(index_price, asset)) => {
                    let url = format!(
                        "{}?price={}&asset={}",
                        current_config.url.as_str(),
                        index_price,
                        asset.as_str()
                    );
                    match reqwest::get(&url).await {
                        Ok(resp) => {
                            let code = resp.status();
                            if code != StatusCode::OK {
                                error!(
                                    "Error sending index price to url: {} for asset {}, errorCode {}",
                                    url, asset, code
                                );
                            }
                            debug!("Index price sent to url: {} for asset {}", url, asset);
                        }
                        Err(e) => {
                            error!(
                                "Error sending index price to url: {} for asset {}, {:?}",
                                url, asset, e
                            );
                        }
                    }
                }
                Ok(DownstreamMessage::Stop) => {
                    info!("{} received stop", name);
                    stop_flag = true;
                }
                Ok(DownstreamMessage::ConfigChange(config)) => {
                    current_config = config.downstream.clone();
                    info!(
                        "Downstream sender received config change {:?}",
                        current_config
                    );
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
