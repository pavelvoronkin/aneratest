use crate::app_config::control::Control;
use std::sync::Arc;

pub struct WebAppState {
    pub control: Arc<Control>,
}

impl WebAppState {
    pub fn new(control: Arc<Control>) -> Self {
        Self { control }
    }
}
