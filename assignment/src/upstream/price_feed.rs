use crate::app_config::app_config::{IndexCollectorAppConfig, PriceFeedConfig};
use crate::index_collector::index_collector::{Asset, FeedId, Source};
use crate::index_collector::processor::ProcessorMessage;
use crate::infra::clock::current_timestamp;
use crate::persistence::persistence_sender::PersisterMessage;
use crate::upstream::coinbase_price_feed::CoinbasePriceFeed;
use async_trait::async_trait;
use crossbeam_channel::Sender;
use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::sleep;

const STATE_RUNNING: u8 = 0;
const STATE_PAUSED: u8 = 1;
const STATE_STOPPED: u8 = 2;

pub enum PriceFeedManagerMessage {
    ConfigChange(IndexCollectorAppConfig),
    Stop,
}

pub struct PriceFeedManager {
    proc_tx: Sender<ProcessorMessage>,
    persister_tx: Sender<PersisterMessage>,
    tasks: HashMap<FeedId, Arc<AtomicU8>>,
}

impl PriceFeedManager {
    pub fn new(proc_tx: Sender<ProcessorMessage>, persister_tx: Sender<PersisterMessage>) -> Self {
        Self {
            proc_tx,
            persister_tx,
            tasks: HashMap::new(),
        }
    }

    pub fn init(&mut self, feeds: HashMap<Asset, Vec<PriceFeedConfig>>) {
        let mut map = HashMap::new();

        for (_, price_feed_configs) in feeds {
            for price_feed_cfg in price_feed_configs {
                let key = price_feed_cfg.key();
                map.insert(key.clone(), price_feed_cfg.clone());
                if let Some(task_state) = self.tasks.get(&key) {
                    info!(
                        "Update task {} state to enabled = {}",
                        key, price_feed_cfg.enabled
                    );
                    task_state.store(
                        if price_feed_cfg.enabled {
                            STATE_RUNNING
                        } else {
                            STATE_PAUSED
                        },
                        SeqCst,
                    );
                } else {
                    let task_state = start_fetch_task(
                        price_feed_cfg,
                        self.proc_tx.clone(),
                        self.persister_tx.clone(),
                    );
                    self.tasks.insert(key, task_state);
                }
            }
        }

        // stop tasks that no longer exists after config update
        self.tasks
            .iter()
            .filter(|item| !map.contains_key(item.0))
            .for_each(|item| {
                info!("Stopping upstream task {}", item.0);
                item.1.store(STATE_STOPPED, SeqCst);
            });

        self.tasks.retain(|feed_id, _| map.contains_key(feed_id));
    }

    pub fn stop_all(&self) {
        info!("Stopping all upstream tasks");
        for (_, tx) in &self.tasks {
            tx.store(STATE_STOPPED, SeqCst);
        }
    }
}

const PAUSE_MESSAGE_INTERVAL_MS: i64 = 5000;

pub fn start_price_feed_man_control_task(
    mut price_feed_man: PriceFeedManager,
    mut rcv: UnboundedReceiver<PriceFeedManagerMessage>,
) {
    tokio::spawn(async move {
        loop {
            match rcv.recv().await {
                Some(msg) => match msg {
                    PriceFeedManagerMessage::ConfigChange(conf) => {
                        price_feed_man.init(conf.price_feeds)
                    }
                    PriceFeedManagerMessage::Stop => price_feed_man.stop_all(),
                },
                None => {}
            }
        }
    });
}

fn start_fetch_task(
    price_feed_cfg: PriceFeedConfig,
    proc_tx: Sender<ProcessorMessage>,
    persister_tx: Sender<PersisterMessage>,
) -> Arc<AtomicU8> {
    let state = Arc::new(AtomicU8::new(if price_feed_cfg.enabled {
        STATE_RUNNING
    } else {
        STATE_PAUSED
    }));
    let state_clone = state.clone();

    tokio::spawn(async move {
        let key = price_feed_cfg.key();
        let mut fail_count = 0;
        let fail_count_warn = price_feed_cfg.fail_count_warn.unwrap_or(5);
        let source = price_feed_cfg.source.clone();
        let asset = price_feed_cfg.asset.clone();
        let price_feed = new_price_feed(&price_feed_cfg);
        let mut last_pause_message_timestamp = 0;
        info!("Price upstream {} started", &key);

        loop {
            let state = state_clone.load(Relaxed);
            if state == STATE_STOPPED {
                info!("Price upstream {} stopped", &key);
                break;
            }

            if state == STATE_PAUSED {
                if current_timestamp() - last_pause_message_timestamp > PAUSE_MESSAGE_INTERVAL_MS {
                    info!("Price upstream {} paused", &key);
                    last_pause_message_timestamp = current_timestamp();
                }
                /*
                TODO: we are burning here too much, so i introduce some sleep,
                    because in our arch we can change config not more frequently than every second, see start_config_poller_task.
                    To improve this we may introduce some wake up call when change state from paused to running or stopped
                */
                sleep(Duration::from_millis(500)).await;

                continue;
            }

            match price_feed.fetch().await {
                Ok(price) => {
                    fail_count = 0;
                    trace!("Price upstream {} fetched {}", key, price);

                    // maybe replace with fan?
                    if let Err(e) = proc_tx.try_send(ProcessorMessage::Price(
                        price,
                        asset.clone(),
                        source.clone(),
                    )) {
                        error!("Error try_send {} to processor: {}", key, e);
                    }

                    if let Err(e) = persister_tx.try_send(PersisterMessage::Price(
                        price,
                        asset.clone(),
                        source.clone(),
                    )) {
                        error!("Error try_send {} to persister: {}", key, e);
                    }
                }
                Err(e) => {
                    debug!(
                        "Error fetching price upstream {}, code: {:?}, message: {}",
                        &source, e.code, e.message
                    );
                    fail_count = fail_count + 1;
                    if fail_count > fail_count_warn {
                        warn!("Error fetching price upstream {}", key);
                        fail_count = 0;
                    }
                }
            };
        }
    });
    state
}

fn new_price_feed(cfg: &PriceFeedConfig) -> Box<dyn PriceFeed> {
    match cfg.source {
        Source::Coinbase => Box::new(CoinbasePriceFeed::new(cfg.url())),
        Source::Kraken => {
            panic!("Kraken price upstream not supported yet");
        }
    }
}

#[async_trait]
pub trait PriceFeed: Send + Sync + 'static {
    async fn fetch(&self) -> Result<f64, FeedErr>;
}

#[derive(Debug)]
pub struct FeedErr {
    pub code: Option<u16>,
    pub message: String,
}

impl FeedErr {
    pub fn new(code: Option<u16>, message: String) -> FeedErr {
        FeedErr { code, message }
    }
}
