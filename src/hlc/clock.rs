use super::timestamp::*;
use super::wall_clock::{manual::Manual, WallClock};
use std::cmp::max;

// TODO: How do we make sure this is thread-safe?
pub struct HLC<S: WallClock> {
    wall_clock: S,
    latest_timestamp: Timestamp<S::Time>,
}

impl<S: WallClock> HLC<S> {
    /**
     * This method corresponds to the Receive event of message m part of the paper: https://cse.buffalo.edu/tech-reports/2014-04.pdf.
     * First, set the next latest_timestamp's time to the max of current PT, incoming PT, and latest_timestamp's PT.
     * Next, decide the logical clock based on the following rules:
     *
     * If the incoming message's PT is the greatest, set logical clock to the incoming timestamp's logical clock + 1.
     * If the current PT is the greatest, then use 0 as the logical clock.
     * If the previous latest_timestamp's PT is the greatest, then increment the clock by 1
     */
    fn receive_timestamp(&mut self, incoming_timestamp: Timestamp<S::Time>) {
        let current_pt = self.wall_clock.current_time();
        let incoming_pt = incoming_timestamp.wall_time;
        let latest_pt = self.latest_timestamp.wall_time;
        let max_pt = max(current_pt, max(incoming_pt, latest_pt));
        if current_pt == incoming_pt && incoming_pt == latest_pt {
            self.latest_timestamp = Timestamp {
                wall_time: max_pt,
                logical_time: max(
                    incoming_timestamp.logical_time,
                    self.latest_timestamp.logical_time,
                ) + 1,
            }
        } else if latest_pt == max_pt {
            self.latest_timestamp = Timestamp {
                wall_time: max_pt,
                logical_time: self.latest_timestamp.logical_time + 1,
            }
        } else if max_pt == incoming_pt {
            self.latest_timestamp = Timestamp {
                wall_time: max_pt,
                logical_time: incoming_timestamp.logical_time + 1,
            }
        } else {
            self.latest_timestamp = Timestamp {
                wall_time: max_pt,
                logical_time: 0,
            }
        }
    }
}

impl HLC<Manual> {
    /**
     * This method corresponds to the Send or Local Event part of the paper: https://cse.buffalo.edu/tech-reports/2014-04.pdf
     * If the current PT is bigger than the latest PT, then use the current PT with logical clock of 0.
     * Otherwise, increment the latest_timestamp's logical timestamp by 1.
     */
    fn get_timestamp(&mut self) -> Timestamp<u32> {
        let current_pt = self.wall_clock.current_time();
        let max_pt = max(self.latest_timestamp.wall_time, current_pt);
        if self.latest_timestamp.wall_time == max_pt {
            self.latest_timestamp = Timestamp {
                wall_time: max_pt,
                logical_time: self.latest_timestamp.logical_time + 1,
            };
        } else {
            self.latest_timestamp = Timestamp {
                wall_time: max_pt,
                logical_time: 0,
            };
        }
        self.latest_timestamp
    }
}
