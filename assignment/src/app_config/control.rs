use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread;
use std::time::Duration;
use log::info;
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use signal_hook::low_level::exit;

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
}

impl Control {
    pub fn new() -> Control {
        Control {
            flag: AtomicU8::new(0),
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.flag.load(Ordering::Relaxed) == STOP
    }

    pub fn is_paused(&self) -> bool {
        self.flag.load(Ordering::Relaxed) == PAUSE
    }

    pub fn stop(&self) {
        self.flag.store(STOP, Ordering::SeqCst)
    }

    pub fn pause(&self) {
        self.flag.store(PAUSE, Ordering::SeqCst)
    }
}
