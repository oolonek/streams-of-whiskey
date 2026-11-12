use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime};

use reqwest::header::HeaderMap;

const MAX_FALLBACK_SECS: u64 = 300;
// Release workers gradually after a shared pause instead of sending a burst at its end.
const RELEASE_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Default)]
struct State {
    deadline: Option<Instant>,
    next_release: Option<Instant>,
    waiters: usize,
    stopped: bool,
}

pub struct RateGate {
    state: Mutex<State>,
    changed: Condvar,
}

impl RateGate {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
        }
    }

    /// Wait for the shared cooldown, admitting queued workers one at a time.
    pub fn wait(&self) -> Result<(), &'static str> {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if state.stopped {
            return Err("rate gate stopped");
        }
        if state.deadline.is_none() && state.next_release.is_none() {
            return Ok(());
        }
        state.waiters += 1;
        loop {
            if state.stopped {
                state.waiters -= 1;
                return Err("rate gate stopped");
            }
            let now = Instant::now();
            if let Some(deadline) = state.deadline {
                if deadline > now {
                    let (new_state, _) = self
                        .changed
                        .wait_timeout(state, deadline - now)
                        .unwrap_or_else(|err| err.into_inner());
                    state = new_state;
                    continue;
                }
                state.deadline = None;
            }
            if let Some(next_release) = state.next_release {
                if next_release > now {
                    let (new_state, _) = self
                        .changed
                        .wait_timeout(state, next_release - now)
                        .unwrap_or_else(|err| err.into_inner());
                    state = new_state;
                    continue;
                }
            }
            state.waiters -= 1;
            state.next_release = (state.waiters > 0).then(|| now + RELEASE_INTERVAL);
            self.changed.notify_all();
            return Ok(());
        }
    }

    /// A successful response can announce that the shared quota is exhausted.
    pub fn observe(&self, headers: &HeaderMap) {
        if let Some(delay) = rate_limit_delay(headers) {
            self.pause(delay);
        }
    }

    /// Record a 429 and return the effective delay for this response.
    pub fn throttle(&self, headers: &HeaderMap, attempt: u32) -> Duration {
        let delay = retry_after_delay(headers)
            .or_else(|| rate_limit_delay(headers))
            .unwrap_or_else(|| fallback_delay(attempt));
        self.pause(delay);
        delay
    }

    pub fn stop(&self) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        state.stopped = true;
        self.changed.notify_all();
    }

    pub fn stopped(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .stopped
    }

    pub fn remaining(&self) -> Option<Duration> {
        let state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if state.stopped {
            return None;
        }
        state
            .deadline
            .or(state.next_release)
            .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
    }

    fn pause(&self, delay: Duration) {
        if delay.is_zero() {
            return;
        }
        let now = Instant::now();
        let deadline = now
            .checked_add(delay)
            .unwrap_or_else(|| now + Duration::from_secs(MAX_FALLBACK_SECS));
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if state.stopped {
            return;
        }
        if state.deadline.is_none_or(|previous| deadline > previous) {
            state.deadline = Some(deadline);
            state.next_release = Some(deadline);
            self.changed.notify_all();
        }
    }
}

impl Default for RateGate {
    fn default() -> Self {
        Self::new()
    }
}

fn fallback_delay(attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(5);
    Duration::from_secs((15_u64 << shift).min(MAX_FALLBACK_SECS))
}

fn retry_after_delay(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get_all("retry-after")
        .iter()
        .filter_map(|value| {
            let value = value.to_str().ok()?.trim();
            if let Ok(seconds) = value.parse::<u64>() {
                return Some(Duration::from_secs(seconds));
            }
            httpdate::parse_http_date(value)
                .ok()?
                .duration_since(SystemTime::now())
                .ok()
                .or(Some(Duration::ZERO))
        })
        .max()
}

fn rate_limit_delay(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get_all("ratelimit")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|entry| {
            let mut remaining = None;
            let mut reset = None;
            for parameter in entry.split(';') {
                let Some((key, value)) = parameter.trim().split_once('=') else {
                    continue;
                };
                let value = value.trim().trim_matches('"');
                if key.trim().eq_ignore_ascii_case("r") {
                    remaining = value.parse::<u64>().ok();
                } else if key.trim().eq_ignore_ascii_case("t") {
                    reset = value.parse::<u64>().ok();
                }
            }
            (remaining == Some(0))
                .then_some(reset)
                .flatten()
                .map(Duration::from_secs)
        })
        .max()
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderName, HeaderValue};
    use std::sync::{Arc, Barrier, mpsc};
    use std::thread;

    fn headers(name: &str, value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
        headers
    }

    #[test]
    fn retry_after_takes_precedence_and_fallback_is_bounded() {
        let gate = RateGate::new();
        let mut response = headers("ReTrY-AfTeR", "42");
        response.insert(
            HeaderName::from_bytes(b"RateLimit").unwrap(),
            HeaderValue::from_static("r=0;t=90"),
        );
        assert_eq!(gate.throttle(&response, 100), Duration::from_secs(42));
        assert!(gate.remaining().is_some());
        gate.stop();
        assert_eq!(gate.throttle(&HeaderMap::new(), 1), Duration::from_secs(15));
        assert_eq!(gate.throttle(&HeaderMap::new(), 2), Duration::from_secs(30));
        assert_eq!(
            gate.throttle(&HeaderMap::new(), u32::MAX),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn retry_after_http_date_and_exhausted_quota() {
        let date = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(60));
        let gate = RateGate::new();
        let delay = gate.throttle(&headers("Retry-After", &date), 1);
        assert!(delay >= Duration::from_secs(58) && delay <= Duration::from_secs(60));
        let gate = RateGate::new();
        gate.observe(&headers("Retry-After", "90"));
        assert!(gate.remaining().is_none());
        gate.observe(&headers("RaTeLiMiT", "\"fixed\";r=5;t=90"));
        assert!(gate.remaining().is_none());
        gate.observe(&headers("RaTeLiMiT", "\"fixed\";r=0;t=60"));
        assert!(gate.remaining().is_some());
        assert_eq!(
            gate.throttle(&headers("RateLimit", "r=0;t=25"), 5),
            Duration::from_secs(25)
        );
    }

    #[test]
    fn later_throttles_extend_shared_cooldown_and_stop_wakes_workers() {
        let gate = Arc::new(RateGate::new());
        gate.observe(&headers("RateLimit", "r=0;t=10"));
        let barrier = Arc::new(Barrier::new(3));
        let (sender, receiver) = mpsc::channel();
        let workers: Vec<_> = (0..2)
            .map(|_| {
                let gate = Arc::clone(&gate);
                let barrier = Arc::clone(&barrier);
                let sender = sender.clone();
                thread::spawn(move || {
                    barrier.wait();
                    sender.send(gate.wait()).unwrap();
                })
            })
            .collect();
        barrier.wait();
        assert!(receiver.recv_timeout(Duration::from_millis(30)).is_err());
        gate.throttle(&headers("Retry-After", "20"), 1);
        assert!(gate.remaining().unwrap() > Duration::from_secs(19));
        gate.stop();
        for _ in 0..2 {
            assert_eq!(
                receiver.recv_timeout(Duration::from_millis(500)).unwrap(),
                Err("rate gate stopped")
            );
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(gate.stopped());
        assert_eq!(gate.wait(), Err("rate gate stopped"));
        assert_eq!(gate.remaining(), None);
    }

    #[test]
    fn queued_workers_leave_cooldown_separately() {
        let gate = Arc::new(RateGate::new());
        gate.pause(Duration::from_millis(100));
        let barrier = Arc::new(Barrier::new(3));
        let (sender, receiver) = mpsc::channel();
        let workers: Vec<_> = (0..2)
            .map(|_| {
                let gate = Arc::clone(&gate);
                let barrier = Arc::clone(&barrier);
                let sender = sender.clone();
                thread::spawn(move || {
                    barrier.wait();
                    gate.wait().unwrap();
                    sender.send(Instant::now()).unwrap();
                })
            })
            .collect();
        barrier.wait();
        let first = receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        let second = receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(second.duration_since(first) >= RELEASE_INTERVAL - Duration::from_millis(20));
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(gate.remaining().is_none());
        gate.wait().unwrap();
    }
}
