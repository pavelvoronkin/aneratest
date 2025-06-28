use std::sync::Arc;
use crate::app_config::app_config::AppConfig;
use crate::order_book::order_book_collector::OrderBookCollector;
use crate::upstream::order_book_feed::OrderBook;
use crossbeam_channel::Receiver;
use log::info;
use std::thread::JoinHandle;
use crate::infra::clock::Clock;

pub enum ProcessorMessage {
    OrderBook(OrderBook),
    ConfigChange(AppConfig),
    Stop,
}

pub fn start(receiver: Receiver<ProcessorMessage>, map: AppConfig, clock: Arc<dyn Clock>) -> JoinHandle<()> {
    let name = String::from("processor");
    let fail_msg = format!("Couldn't start {}", name);
    std::thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            info!("started {}", name);
            let mut stop_flag = false;
            let mut collector = OrderBookCollector::new(map.clone(), clock);
            loop {
                match receiver.try_recv() {
                    Ok(ProcessorMessage::OrderBook(order_book)) => {
                        collector.collect_order_book(order_book);
                        // TODO: send to downstream?
                    }
                    Ok(ProcessorMessage::Stop) => {
                        info!("{} received stop", name);
                        stop_flag = true;
                    }
                    Ok(ProcessorMessage::ConfigChange(cfg)) => {
                        info!("processor received config change");
                        collector.init(cfg);
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
