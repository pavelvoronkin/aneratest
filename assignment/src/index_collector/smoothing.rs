use ta::indicators::{ExponentialMovingAverage, SimpleMovingAverage};
use ta::Next;

pub trait Smoothing {
    fn smooth(&mut self, price: f64) -> f64;
}

pub struct SMASmoothing {
    sma: SimpleMovingAverage,
}

impl Default for SMASmoothing {
    fn default() -> Self {
        SMASmoothing {
            sma: SimpleMovingAverage::new(20).expect("Failed to create sma"),
        }
    }
}

impl Smoothing for SMASmoothing {
    fn smooth(&mut self, price: f64) -> f64 {
        self.sma.next(price)
    }
}

pub struct EMASmoothing {
    ema: ExponentialMovingAverage,
}

impl Default for EMASmoothing {
    fn default() -> Self {
        EMASmoothing {
            ema: ExponentialMovingAverage::new(20).expect("Failed to create EMA"),
        }
    }
}

impl Smoothing for EMASmoothing {
    fn smooth(&mut self, price: f64) -> f64 {
        self.ema.next(price)
    }
}

#[cfg(test)]
mod tests {
    use crate::index_collector::smoothing::{EMASmoothing, SMASmoothing, Smoothing};

    #[test]
    fn test_sma() {
        // given
        let mut sma = SMASmoothing::default();

        // when
        for i in 1..23 {
            sma.smooth(i as f64);
        }

        // then
        assert_eq!(sma.smooth(24.0), 13.55);
    }

    #[test]
    fn test_ema() {
        // given
        let mut ema = EMASmoothing::default();

        // when
        for i in 1..23 {
            ema.smooth(i as f64);
        }

        // then
        assert_eq!(ema.smooth(24.0), 14.645937151331967);
    }
}
