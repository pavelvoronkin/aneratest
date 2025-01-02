use crate::app_config::app_config::PriceFeedConfig;
use crate::index_collector::index_collector::{Asset, IndexCollector, Source};
use crossbeam_channel::{Receiver, Sender};
use log::{error, info};
use std::collections::HashMap;
use std::thread::JoinHandle;
use crate::downstream::downstream_sender::DownstreamMessage;

pub enum ProcessorMessage {
    Price(f64, Asset, Source),
    Stop,
}

pub fn start(
    receiver: Receiver<ProcessorMessage>,
    sender: Sender<DownstreamMessage>,
    map: HashMap<Asset, Vec<PriceFeedConfig>>,
) -> JoinHandle<()> {
    let name = String::from("processor");
    let fail_msg = format!("Couldn't start {}", name);
    std::thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            info!("started {}", name);
            let mut stop_flag = false;
            let mut collector = IndexCollector::new(map.clone());
            loop {
                match receiver.try_recv() {
                    Ok(ProcessorMessage::Price(price, asset, source)) => {
                        collector.collect_price(price, &asset, &source);
                        let index_price = collector.get_index_price(&asset);
                        if let Err(e) = sender.try_send(DownstreamMessage::Index(index_price, asset.clone())) {
                            error!("Error sending index to downstream {}", e);
                        }
                    }
                    Ok(ProcessorMessage::Stop) => {
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
        .expect(fail_msg.as_str())
}
