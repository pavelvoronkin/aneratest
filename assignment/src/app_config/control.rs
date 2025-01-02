use std::sync::atomic::{AtomicU8, Ordering};

const RUN: u8 = 0;
const PAUSE: u8 = 1;
const STOP: u8 = 2;

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
