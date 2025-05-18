use crate::app_config::app_config::AppConfig;
use crate::arb_bot::arb_bot::{ArbBot, Asset, Source};
use crate::upstream::price_feed::PriceEvent;
use crossbeam_channel::Receiver;
use log::info;
use std::thread::JoinHandle;

pub enum ProcessorMessage {
    Price(PriceEvent, Asset, Source),
    ConfigChange(AppConfig),
    Stop,
}

pub fn start(receiver: Receiver<ProcessorMessage>, map: AppConfig) -> JoinHandle<()> {
    let name = String::from("processor");
    let fail_msg = format!("Couldn't start {}", name);
    std::thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            info!("started {}", name);
            let mut stop_flag = false;
            let mut bot = ArbBot::new(map.clone());
            loop {
                match receiver.try_recv() {
                    Ok(ProcessorMessage::Price(price, asset, source)) => {
                        bot.collect_price(price, &asset, &source);
                        if let Some(_) = bot.take_opportunities(&asset) {
                            // TODO: send opportunity to downstream
                        }
                    }
                    Ok(ProcessorMessage::Stop) => {
                        info!("{} received stop", name);
                        stop_flag = true;
                    }
                    Ok(ProcessorMessage::ConfigChange(cfg)) => {
                        info!("processor received config change");
                        bot.init(cfg);
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
