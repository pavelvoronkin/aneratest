use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread;
use std::time::Duration;
use crossbeam_channel::Sender;
use log::{error, info};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use signal_hook::low_level::exit;
use crate::downstream::downstream_sender::DownstreamMessage;
use crate::index_collector::processor::ProcessorMessage;

const RUN: u8 = 0;
const PAUSE: u8 = 1;
const STOP: u8 = 2;

pub fn start_signal_handler(control: Arc<Control>) {
    let mut signals = Signals::new([SIGINT, SIGTERM]).expect("signal handler created");
    thread::spawn(move || {
        for sig in signals.forever() {
            info!("Received signal {:?}", sig);
            control.stop();
            thread::sleep(Duration::from_secs(1));
            exit(0);
        }
    });

}

pub struct Control {
    flag: AtomicU8,
    proc_snd: Sender<ProcessorMessage>,
    persister_snd: Sender<ProcessorMessage>,
    d_snd: Sender<DownstreamMessage>,
}

impl Control {
    pub fn new(proc_snd: Sender<ProcessorMessage>,
               persister_snd: Sender<ProcessorMessage>,
               d_snd: Sender<DownstreamMessage>
    ) -> Control {
        Control {
            flag: AtomicU8::new(0),
            proc_snd,
            persister_snd,
            d_snd
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.flag.load(Ordering::Relaxed) == STOP
    }

    pub fn is_paused(&self) -> bool {
        self.flag.load(Ordering::Relaxed) == PAUSE
    }

    pub fn stop(&self) {
        if let Err(e) = self.proc_snd.send(ProcessorMessage::Stop) {
            error!("processor stop send failed: {}", e);
        }

        if let Err(e) = self.persister_snd.send(ProcessorMessage::Stop) {
            error!("processor stop send failed: {}", e);
        }

        if let Err(e) = self.d_snd.send(DownstreamMessage::Stop) {
            error!("processor stop send failed: {}", e);
        }

        self.flag.store(STOP, Ordering::SeqCst)
    }

    pub fn pause(&self) {
        self.flag.store(PAUSE, Ordering::SeqCst)
    }
}
