use crate::app_config::app_config::AppConfig;
use crate::order_book::order_book_collector::Source;
use crate::upstream::order_book_feed::OrderBook;
use crossbeam_channel::Receiver;
use log::info;
use std::process::exit;
use std::thread::JoinHandle;

pub enum PersisterMessage {
    OrderBook(OrderBook, Source),
    ConfigChange(AppConfig),
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
                    Ok(PersisterMessage::OrderBook(_price, _source)) => {
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
