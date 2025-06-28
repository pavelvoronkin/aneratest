use std::time::{SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync {
    fn current_timestamp(&self) -> i64;
    fn current_timestamp_us(&self) -> i64;
}

pub struct SystemClock;

impl SystemClock {
    pub fn new() -> Self {
        SystemClock
    }
}

impl Clock for SystemClock {
    // return current timestamp in ms
    fn current_timestamp(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64
    }

    fn current_timestamp_us(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_micros() as i64
    }
}

pub struct FixedClock {
    pub fixed_timestamp: i64,
}

impl FixedClock {
    pub fn new() -> Self {
        FixedClock {
            fixed_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as i64,
        }
    }
}

impl Clock for FixedClock {
    fn current_timestamp(&self) -> i64 {
        self.fixed_timestamp
    }

    fn current_timestamp_us(&self) -> i64 {
        self.fixed_timestamp * 1000
    }
}