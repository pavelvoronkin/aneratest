use crate::app_config::app_config::IndexCollectorAppConfig;
use crate::index_collector::index_collector::{Asset, Source};
use crossbeam_channel::Receiver;
use log::info;
use std::process::exit;
use std::thread::JoinHandle;

pub enum PersisterMessage {
    Price(f64, Asset, Source),
    ConfigChange(IndexCollectorAppConfig),
    Stop,
}

pub fn start(receiver: Receiver<PersisterMessage>) -> JoinHandle<()> {
    let name = String::from("persister");
    let fail_msg = format!("Couldn't start {}", name);
    std::thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            info!("started {}", name);
            let mut stop_flag = false;
            loop {
                match receiver.try_recv() {
                    Ok(PersisterMessage::Price(_price, _asset, _source)) => {
                        // TODO: implement persister
                    }
                    Ok(PersisterMessage::Stop) => {
                        info!("{} received stop", name);
                        stop_flag = true;
                    }
                    Ok(PersisterMessage::ConfigChange(_)) => {
                        // TODO: implement config change
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
