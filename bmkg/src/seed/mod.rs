use crate::models::{SeismicEvent, ThreatLevel, TsunamiWarning};
use crate::state::AppState;
use chrono::{DateTime, Duration, TimeZone, Utc};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::ops::Range;

const REGIONS: [&str; 10] = [
    "Aceh", "Nias", "Mentawai", "Bengkulu", "Selat Sunda", "Yogyakarta", "Palu", "Ambon", "Maluku Utara", "Papua Barat",
];

const LAT_RANGE: Range<f64> = -11.0..6.0;
const LON_RANGE: Range<f64> = 95.0..141.0;

const SEED_PERIOD_HOURS: i64 = 3;

fn seed_start() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
}

/// Creates seismic events from a deterministic RNG, so the same seed always produces the same history.
pub struct EventGenerator {
    rng: StdRng,
    next_event_id: u64,
    next_warning_id: u64,
}

impl EventGenerator {
    pub fn new(seed: u64) -> Self {
        Self { rng: StdRng::seed_from_u64(seed), next_event_id: 1, next_warning_id: 1 }
    }

    /// Generates one event, and a tsunami warning when it warrants one (shallow and strong enough).
    pub fn next(&mut self, occurred_at: DateTime<Utc>) -> (SeismicEvent, Option<TsunamiWarning>) {
        let region = REGIONS[self.rng.random_range(0..REGIONS.len())];
        let magnitude = (self.rng.random_range(25..76) as f64) / 10.0; // 2.5 - 7.5
        let depth_km = self.rng.random_range(5.0..300.0);
        let epicenter_lat = self.rng.random_range(LAT_RANGE);
        let epicenter_lon = self.rng.random_range(LON_RANGE);
        let potential_tsunami = magnitude >= 6.5 && depth_km <= 60.0;

        let event_id = format!("evt-{:06}", self.next_event_id);
        self.next_event_id += 1;

        let event = SeismicEvent {
            event_id: event_id.clone(),
            magnitude,
            depth_km,
            epicenter_lat,
            epicenter_lon,
            region_name: region.to_string(),
            occurred_at,
            potential_tsunami,
        };

        let warning = if potential_tsunami {
            let issued_at = occurred_at + Duration::minutes(self.rng.random_range(1..5));
            let estimated_arrival = issued_at + Duration::minutes(self.rng.random_range(10..45));

            let mut affected_zones = vec![format!("Pesisir {region}")];
            if magnitude >= 7.0 {
                affected_zones.push(format!("Pesisir {}", REGIONS[self.rng.random_range(0..REGIONS.len())]));
            }
            let threat_level = if magnitude >= 7.5 {
                ThreatLevel::Awas
            } else if magnitude >= 7.0 {
                ThreatLevel::Siaga
            } else {
                ThreatLevel::Waspada
            };

            let warning_id = format!("tsw-{:06}", self.next_warning_id);
            self.next_warning_id += 1;
            Some(TsunamiWarning {
                warning_id,
                related_event_id: event_id,
                threat_level,
                affected_zones,
                estimated_arrival,
                issued_at,
            })
        } else {
            None
        };

        (event, warning)
    }
}

async fn store(state: &AppState, event: SeismicEvent, warning: Option<TsunamiWarning>) {
    state.add_seismic_event(event).await;
    if let Some(warning) = warning {
        state.add_tsunami_warning(warning).await;
    }
}

/// Seeds one event every `SEED_PERIOD_HOURS` from a fixed start date up to `until`.
pub async fn seed_default(state: &AppState, generator: &mut EventGenerator, until: DateTime<Utc>) {
    let period = Duration::hours(SEED_PERIOD_HOURS);
    let mut slot = seed_start();
    while slot <= until {
        let (event, warning) = generator.next(slot);
        store(state, event, warning).await;
        slot += period;
    }
}

/// Seeds exactly `count` events, spaced `SEED_PERIOD_HOURS` apart, ending at or before `until`.
pub async fn seed_count(state: &AppState, generator: &mut EventGenerator, count: usize, until: DateTime<Utc>) {
    let period = Duration::hours(SEED_PERIOD_HOURS);
    let period_secs = period.num_seconds();
    // Truncated to whole seconds first, since `until` (usually `Utc::now()`) carries sub-second precision.
    let now_whole_seconds = DateTime::from_timestamp(until.timestamp(), 0).expect("a valid DateTime always has a representable Unix timestamp");
    let last_slot = now_whole_seconds - Duration::seconds(now_whole_seconds.timestamp().rem_euclid(period_secs));

    for i in 0..count {
        let slots_back = (count - 1 - i) as i32;
        let (event, warning) = generator.next(last_slot - period * slots_back);
        store(state, event, warning).await;
    }
}

/// Runs forever, adding one new event every `interval`, continuing the seed's generator sequence.
pub async fn run_live_generator(state: AppState, mut generator: EventGenerator, interval: std::time::Duration) {
    loop {
        tokio::time::sleep(interval).await;
        let (event, warning) = generator.next(Utc::now());
        store(&state, event, warning).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_seed_produces_the_same_first_event() {
        let mut a = EventGenerator::new(42);
        let mut b = EventGenerator::new(42);
        let t = seed_start();

        assert_eq!(a.next(t).0, b.next(t).0);
    }

    #[test]
    fn different_seeds_produce_different_events() {
        let mut a = EventGenerator::new(1);
        let mut b = EventGenerator::new(2);
        let t = seed_start();

        assert_ne!(a.next(t).0, b.next(t).0);
    }

    #[test]
    fn event_and_warning_ids_are_unique_and_increasing() {
        let mut generator = EventGenerator::new(42);
        let mut event_ids = Vec::new();
        let mut warning_ids = Vec::new();
        for i in 0..200 {
            let (event, warning) = generator.next(seed_start() + Duration::hours(i));
            event_ids.push(event.event_id);
            if let Some(w) = warning {
                warning_ids.push(w.warning_id);
            }
        }
        let mut sorted_events = event_ids.clone();
        sorted_events.sort();
        sorted_events.dedup();
        assert_eq!(sorted_events.len(), event_ids.len(), "event ids must be unique");
        assert!(!warning_ids.is_empty(), "expected at least one tsunami warning over 200 events");
        let mut sorted_warnings = warning_ids.clone();
        sorted_warnings.sort();
        sorted_warnings.dedup();
        assert_eq!(sorted_warnings.len(), warning_ids.len(), "warning ids must be unique");
    }

    #[test]
    fn a_warning_is_only_produced_for_a_shallow_strong_event() {
        let mut generator = EventGenerator::new(42);
        for i in 0..500 {
            let (event, warning) = generator.next(seed_start() + Duration::hours(i));
            let warrants_warning = event.magnitude >= 6.5 && event.depth_km <= 60.0;
            assert_eq!(warning.is_some(), warrants_warning, "event {}: magnitude={} depth={}", event.event_id, event.magnitude, event.depth_km);
        }
    }

    #[test]
    fn a_warning_references_its_own_event_and_arrives_after_it_was_issued() {
        let mut generator = EventGenerator::new(7);
        for i in 0..500 {
            let (event, warning) = generator.next(seed_start() + Duration::hours(i));
            if let Some(warning) = warning {
                assert_eq!(warning.related_event_id, event.event_id);
                assert!(warning.issued_at >= event.occurred_at);
                assert!(warning.estimated_arrival > warning.issued_at);
                assert!(!warning.affected_zones.is_empty());
            }
        }
    }

    #[test]
    fn generated_coordinates_stay_within_indonesias_bounding_box() {
        let mut generator = EventGenerator::new(3);
        for i in 0..200 {
            let (event, _) = generator.next(seed_start() + Duration::hours(i));
            assert!(LAT_RANGE.contains(&event.epicenter_lat));
            assert!(LON_RANGE.contains(&event.epicenter_lon));
            assert!(REGIONS.contains(&event.region_name.as_str()));
        }
    }

    #[tokio::test]
    async fn seed_default_produces_at_least_the_spec_minimum_of_20_records() {
        let state = AppState::new();
        let mut generator = EventGenerator::new(42);
        seed_default(&state, &mut generator, seed_start() + Duration::days(10)).await;

        assert!(state.seismic_event_count().await >= 20);
    }

    #[tokio::test]
    async fn seed_default_reaches_up_to_but_not_past_now() {
        let state = AppState::new();
        let mut generator = EventGenerator::new(42);
        let until = seed_start() + Duration::hours(30);
        seed_default(&state, &mut generator, until).await;

        let events = state.seismic_events_since(None).await;
        assert!(events.last().unwrap().occurred_at <= until);
        assert!(events.iter().all(|e| e.occurred_at >= seed_start()));
    }

    #[tokio::test]
    async fn seed_count_seeds_exactly_the_requested_number() {
        let state = AppState::new();
        let mut generator = EventGenerator::new(42);
        seed_count(&state, &mut generator, 7, Utc::now()).await;

        assert_eq!(state.seismic_event_count().await, 7);
    }

    #[tokio::test]
    async fn seed_count_produces_whole_second_timestamps_even_from_a_sub_second_instant() {
        let state = AppState::new();
        let mut generator = EventGenerator::new(42);
        seed_count(&state, &mut generator, 3, Utc::now()).await;

        let events = state.seismic_events_since(None).await;
        for event in &events {
            assert_eq!(event.occurred_at.timestamp_subsec_nanos(), 0, "{} has a stray sub-second fraction: {}", event.event_id, event.occurred_at);
        }
    }

    #[tokio::test]
    async fn seed_count_of_zero_seeds_nothing() {
        let state = AppState::new();
        let mut generator = EventGenerator::new(42);
        seed_count(&state, &mut generator, 0, Utc::now()).await;

        assert_eq!(state.seismic_event_count().await, 0);
    }

    #[tokio::test]
    async fn seed_count_ends_at_or_before_the_given_instant() {
        let state = AppState::new();
        let mut generator = EventGenerator::new(42);
        let until = seed_start() + Duration::hours(100);
        seed_count(&state, &mut generator, 5, until).await;

        let events = state.seismic_events_since(None).await;
        assert_eq!(events.len(), 5);
        assert!(events.last().unwrap().occurred_at <= until);
        for pair in events.windows(2) {
            assert_eq!((pair[1].occurred_at - pair[0].occurred_at).num_hours(), SEED_PERIOD_HOURS, "slots must be evenly spaced");
        }
    }

    #[tokio::test]
    async fn run_live_generator_keeps_adding_events_while_it_runs() {
        let state = AppState::new();
        let generator = EventGenerator::new(1);
        tokio::spawn(run_live_generator(state.clone(), generator, std::time::Duration::from_millis(5)));

        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        assert!(state.seismic_event_count().await >= 5, "expected several events after 60ms with a 5ms interval");
    }

    #[tokio::test]
    async fn run_live_generator_timestamps_events_at_generation_time_not_a_fixed_slot() {
        let state = AppState::new();
        let before = Utc::now();
        let generator = EventGenerator::new(2);
        tokio::spawn(run_live_generator(state.clone(), generator, std::time::Duration::from_millis(5)));

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let after = Utc::now();

        let events = state.seismic_events_since(None).await;
        assert!(!events.is_empty());
        for event in &events {
            assert!(event.occurred_at >= before && event.occurred_at <= after, "{} is outside [{before}, {after}]", event.occurred_at);
        }
    }
}
