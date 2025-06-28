use crate::app_config::app_config::{AppConfig, FeedConfig};
use crate::order_book::order_book_collector::{Asset, Source};
use crate::order_book::processor::ProcessorMessage;
use crate::persistence::persistence_sender::PersisterMessage;
use crate::upstream::binance_order_book_feed::BinanceOrderBookFeed;
use crate::upstream::uniswap_order_book_feed::UniswapFeed;
use async_trait::async_trait;
use crossbeam_channel::Sender;
use log::{debug, error, info};
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
    ConfigChange(AppConfig),
    Stop,
}

pub struct OrderBookFeedManager {
    proc_tx: Sender<ProcessorMessage>,
    persister_tx: Sender<PersisterMessage>,
    tasks: HashMap<Source, Arc<AtomicU8>>
}

impl OrderBookFeedManager {
    pub fn new(proc_tx: Sender<ProcessorMessage>, persister_tx: Sender<PersisterMessage>) -> Self {
        Self {
            proc_tx,
            persister_tx,
            tasks: HashMap::new(),
        }
    }

    pub fn init(&mut self, feeds: Vec<FeedConfig>) {
        let mut map = HashMap::new();
        for price_feed_cfg in feeds {
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

pub fn start_price_feed_man_control_task(
    mut price_feed_man: OrderBookFeedManager,
    mut rcv: UnboundedReceiver<PriceFeedManagerMessage>,
) {
    tokio::spawn(async move {
        loop {
            match rcv.recv().await {
                Some(msg) => match msg {
                    PriceFeedManagerMessage::ConfigChange(conf) => price_feed_man.init(conf.feeds),
                    PriceFeedManagerMessage::Stop => price_feed_man.stop_all(),
                },
                None => {}
            }
        }
    });
}

fn start_fetch_task(
    feed_cfg: FeedConfig,
    proc_tx: Sender<ProcessorMessage>,
    persister_tx: Sender<PersisterMessage>,
) -> Arc<AtomicU8> {
    let state = Arc::new(AtomicU8::new(if feed_cfg.enabled {
        STATE_RUNNING
    } else {
        STATE_PAUSED
    }));
    let state_clone = state.clone();

    tokio::spawn(async move {
        let key = feed_cfg.key();
        let source = feed_cfg.source.clone();
        let mut order_book_feed = new_price_feed(&feed_cfg).await;
        info!("Price upstream {} started", &key);

        loop {
            let state = state_clone.load(Relaxed);
            if state == STATE_STOPPED {
                info!("Price upstream {} stopped", &key);
                break;
            }

            if state == STATE_PAUSED {
                /*
                TODO: we are burning here too much, so i introduce some sleep,
                    because in our arch we can change config not more frequently than every second, see start_config_poller_task.
                    To improve this we may introduce some wake up call when change state from paused to running or stopped
                */
                sleep(Duration::from_millis(500)).await;

                continue;
            }

            /* why i made trait like this?

             loop {
                match price_feed.fetch().await {

                }
             }

             to make trait more generic, what it will be not websocket, but rather polling http
            */
            match order_book_feed.fetch().await {
                Ok(order_book_opt) => {
                    if let Some(order_book) = order_book_opt {
                        debug!("Price upstream {} fetched {:?}", key, order_book);
                        // maybe replace with fan?
                        if let Err(e) = proc_tx.try_send(ProcessorMessage::OrderBook(order_book.clone())) {
                            error!("Error try_send {} to processor: {}", key, e);
                        }

                        if let Err(e) = persister_tx.try_send(PersisterMessage::OrderBook(
                            order_book,
                            source.clone(),
                        )) {
                            error!("Error try_send {} to persister: {}", key, e);
                        }
                    }
                }
                Err(e) => {
                    debug!(
                        "Error fetching price upstream {}, code: {:?}, message: {}",
                        &source, e.code, e.message
                    );
                }
            };
        }
    });
    state
}

async fn new_price_feed(cfg: &FeedConfig) -> Box<dyn OrderBookFeed> {
    match cfg.source {
        Source::Binance => Box::new(BinanceOrderBookFeed::new(cfg.url()).await),
        Source::Uniswap => Box::new(UniswapFeed::new()),
    }
}

#[async_trait]
pub trait OrderBookFeed: Send + Sync + 'static {
    async fn fetch(&mut self) -> Result<Option<OrderBook>, FeedErr>;
}

#[derive(Debug, Clone)]
pub struct OrderBookLevel {
    pub price: f64,
    pub qty: f64,
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub source: Source,
    pub asset: Asset,
    pub timestamp: i64,
    pub bids: Vec<OrderBookLevel>, // sorted descending by price
    pub asks: Vec<OrderBookLevel>, // sorted ascending by price
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
