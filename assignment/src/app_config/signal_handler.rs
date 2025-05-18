use crate::app_config::config_watcher::ConfigPollerMessage;
use crate::arb_bot::processor::ProcessorMessage;
use crate::persistence::persistence_sender::PersisterMessage;
use crate::upstream::price_feed::PriceFeedManagerMessage;
use crossbeam_channel::Sender;
use log::{error, info};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

pub fn start_signal_handler_thread(
    proc_tx: Sender<ProcessorMessage>,
    persister_tx: Sender<PersisterMessage>,
    price_feed_manager_tx: UnboundedSender<PriceFeedManagerMessage>,
    config_poller_tx: Sender<ConfigPollerMessage>,
) {
    let mut signals = Signals::new([SIGINT, SIGTERM]).expect("signal handler created");
    thread::Builder::new()
        .name("signal_handler".to_string())
        .spawn(move || {
            for sig in signals.forever() {
                info!("Received signal {:?}", sig);

                if let Err(e) = config_poller_tx.send(ConfigPollerMessage::Stop) {
                    error!("config poller stop send failed: {}", e);
                }

                if let Err(e) = price_feed_manager_tx.send(PriceFeedManagerMessage::Stop) {
                    error!("upstream stop send failed: {}", e);
                }

                if let Err(e) = proc_tx.send(ProcessorMessage::Stop) {
                    error!("processor stop send failed: {}", e);
                }

                if let Err(e) = persister_tx.send(PersisterMessage::Stop) {
                    error!("persister stop send failed: {}", e);
                }

                thread::sleep(Duration::from_secs(10));
            }
        })
        .expect("Failed to spawn signal handler thread");
}
