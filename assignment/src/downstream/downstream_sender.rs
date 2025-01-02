use std::collections::HashMap;
use std::thread::JoinHandle;
use crossbeam_channel::Receiver;
use log::info;
use crate::app_config::app_config::PriceFeedConfig;
use crate::index_collector::index_collector::{Asset, IndexCollector};
use crate::index_collector::processor::ProcessorMessage;

pub enum DownstreamMessage {
    Index(f64, Asset),
    Stop,
}

pub fn start(
    receiver: Receiver<DownstreamMessage>,
) -> JoinHandle<()> {
    let name = String::from("downstream");
    let fail_msg = format!("Couldn't start {}", name);
    std::thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            info!("started {}", name);
            let mut stop_flag = false;
            loop {
                match receiver.try_recv() {
                    Ok(DownstreamMessage::Index(index_price, asset)) => {
                        // TODO: implement actual sender, have to rewrite to tokio async task?
                        info!("TODO send index price {} for asset {} to downstream", index_price, asset);
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
        .expect(fail_msg.as_str())
}

