use log::{debug, error, info};
use reqwest::StatusCode;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;
use crate::app_config::app_config::{DownstreamConfig, IndexCollectorAppConfig};
use crate::index_collector::index_collector::Asset;

pub enum DownstreamMessage {
    Index(f64, Asset),
    ConfigChange(IndexCollectorAppConfig),
    Stop,
}

pub fn start(
    mut rx: UnboundedReceiver<DownstreamMessage>,
    downstream_config: DownstreamConfig,
) -> JoinHandle<()> {
    let name = String::from("downstream_sender");
    tokio::spawn(async move {
        info!("started {}", name);
        let mut stop_flag = false;
        let mut current_config = downstream_config.clone();
        loop {
            match rx.recv().await {
                Some(DownstreamMessage::Index(index_price, asset)) => {
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
                Some(DownstreamMessage::Stop) => {
                    info!("{} received stop", name);
                    stop_flag = true;
                }
                Some(DownstreamMessage::ConfigChange(config)) => {
                    current_config = config.downstream.clone();
                    info!(
                        "Downstream sender received config change {:?}",
                        current_config
                    );
                }
                _ => {}
            }

            if stop_flag && rx.is_empty() {
                break;
            }
        }
        info!("{} stopped", name);
    })
}
