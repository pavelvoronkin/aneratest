use crate::app_config::app_config::AppConfig;
use crate::order_book::processor::ProcessorMessage;
use crate::persistence::persistence_sender::PersisterMessage;
use crate::upstream::order_book_feed::PriceFeedManagerMessage;
use crossbeam_channel::{Receiver, Sender};
use etcd_client::Client;
use log::{error, info};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

pub enum ConfigPollerMessage {
    Stop,
}

pub const DELAY_BETWEEN_ATTEMPTS: Duration = Duration::from_secs(1);

pub fn start_config_watcher_task(
    initial_config: AppConfig,
    upstream_tx: UnboundedSender<PriceFeedManagerMessage>,
    prc_tx: Sender<ProcessorMessage>,
    persister_tx: Sender<PersisterMessage>,
    config_watcher_rx: Receiver<ConfigPollerMessage>,
    config: Option<String>,
    etcd_url: Option<String>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Config watcher started");
        let key = config.unwrap_or_else(|| crate::app_config::app_config::get_config_key());
        let url = etcd_url.unwrap_or_else(|| "127.0.0.1:2379".to_string());
        let mut current = initial_config.clone();

        let mut watch_client_opt = None;
        while watch_client_opt.is_none() {
            if config_watcher_rx.try_recv().is_ok() {
                break;
            }

            match Client::connect([&url], None).await {
                Ok(client) => {
                    watch_client_opt = Some(client.watch_client());
                }
                Err(e) => {
                    error!("Unable to connect to etcd server: {}", e);
                    tokio::time::sleep(DELAY_BETWEEN_ATTEMPTS).await;
                }
            }
        }

        match watch_client_opt {
            None => {}
            Some(mut watch_client) => {
                info!("Start watching config {} on url {}", key, url);
                loop {
                    match watch_client.watch(key.as_bytes(), None).await {
                        Ok((_, mut ws)) => {
                            if config_watcher_rx.try_recv().is_ok() {
                                break;
                            }
                            info!("Got watch stream for config {} on url {}", key, url);
                            match ws.message().await {
                                Ok(watch_response_opt) => {
                                    if let Some(watch_response) = watch_response_opt {
                                        info!("Config update received");
                                        for event in watch_response.events() {
                                            if let Some(kv) = event.kv() {
                                                match crate::app_config::app_config::from_key_value(
                                                    &kv,
                                                ) {
                                                    Ok(config) => {
                                                        if !current.eq(&config) {
                                                            if let Err(e) = upstream_tx.send(
                                                                PriceFeedManagerMessage::ConfigChange(
                                                                    config.clone(),
                                                                ),
                                                            ) {
                                                                error!("Error sending config change to processor: {}", e);
                                                            }

                                                            if let Err(e) = prc_tx.send(
                                                                ProcessorMessage::ConfigChange(
                                                                    config.clone(),
                                                                ),
                                                            ) {
                                                                error!("Error sending config change to processor: {}", e);
                                                            }
                                                            if let Err(e) = persister_tx.send(
                                                                PersisterMessage::ConfigChange(
                                                                    config.clone(),
                                                                ),
                                                            ) {
                                                                error!("Error sending config change to processor: {}", e);
                                                            }
                                                            current = config;
                                                        } else {
                                                            info!("No config changed since last update")
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!(
                                                            "Failed to parse config change : {}",
                                                            e
                                                        )
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        info!("No watch response");
                                    }
                                }
                                Err(e) => {
                                    error!("Error get data from watch stream key {}: {}", key, e);
                                }
                            }
                        }
                        Err(e) => {
                            if config_watcher_rx.try_recv().is_ok() {
                                break;
                            }
                            error!("Unable to watch key {} on etcd server {}: {}", key, url, e);
                            tokio::time::sleep(DELAY_BETWEEN_ATTEMPTS).await;
                        }
                    }
                }
            }
        }
        info!("Config watcher stopped");
    })
}
