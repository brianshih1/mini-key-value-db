use super::clock_source::{manual::Manual, ClockSource};

pub struct HLC<S: ClockSource> {
    s: S,
    max_observed: str,
}

impl HLC<Manual> {}
