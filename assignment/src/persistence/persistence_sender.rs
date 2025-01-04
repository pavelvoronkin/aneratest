use std::collections::HashMap;
use std::process::exit;
use std::thread::JoinHandle;
use crossbeam_channel::Receiver;
use log::{debug, info};
use crate::app_config::app_config::PriceFeedConfig;
use crate::index_collector::index_collector::{Asset, IndexCollector};
use crate::index_collector::processor::ProcessorMessage;

pub fn start(
    receiver: Receiver<ProcessorMessage>,
) -> JoinHandle<()> {
    let name = String::from("persister");
    let fail_msg = format!("Couldn't start {}", name);
    std::thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            info!("started {}", name);
            let mut stop_flag = false;
            loop {
                match receiver.try_recv() {
                    Ok(ProcessorMessage::Price(price, asset, source)) => {
                        // TODO: implement persister
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
            exit(0);
        })
        .expect(fail_msg.as_str())
}

