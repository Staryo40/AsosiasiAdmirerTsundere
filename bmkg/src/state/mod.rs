use crate::models::{SeismicEvent, TsunamiWarning};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Default)]
struct Store {
    seismic_events: Vec<SeismicEvent>,
    tsunami_warnings: Vec<TsunamiWarning>,
}

/// In-memory storage shared between HTTP handlers and the background generator task.
#[derive(Clone, Default)]
pub struct AppState {
    store: Arc<RwLock<Store>>,
}

impl AppState {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn add_seismic_event(&self, event: SeismicEvent) {
        self.store.write().await.seismic_events.push(event);
    }

    pub async fn add_tsunami_warning(&self, warning: TsunamiWarning) {
        self.store.write().await.tsunami_warnings.push(warning);
    }

    /// Events strictly after `since` (or all of them if `since` is `None`), oldest first, re-sorted on every read.
    pub async fn seismic_events_since(&self, since: Option<DateTime<Utc>>) -> Vec<SeismicEvent> {
        let store = self.store.read().await;
        let mut events: Vec<SeismicEvent> = store
            .seismic_events
            .iter()
            .filter(|e| since.is_none_or(|s| e.occurred_at > s))
            .cloned()
            .collect();
        events.sort_by_key(|e| e.occurred_at);
        events
    }

    /// Warnings issued strictly after `since` (or all of them if `since` is `None`), oldest first.
    pub async fn tsunami_warnings_since(&self, since: Option<DateTime<Utc>>) -> Vec<TsunamiWarning> {
        let store = self.store.read().await;
        let mut warnings: Vec<TsunamiWarning> = store
            .tsunami_warnings
            .iter()
            .filter(|w| since.is_none_or(|s| w.issued_at > s))
            .cloned()
            .collect();
        warnings.sort_by_key(|w| w.issued_at);
        warnings
    }

    pub async fn seismic_event_count(&self) -> usize {
        self.store.read().await.seismic_events.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ThreatLevel;
    use chrono::TimeZone;

    fn event(id: &str, occurred_at: DateTime<Utc>) -> SeismicEvent {
        SeismicEvent {
            event_id: id.to_string(),
            magnitude: 5.0,
            depth_km: 10.0,
            epicenter_lat: 0.0,
            epicenter_lon: 0.0,
            region_name: "Test".to_string(),
            occurred_at,
            potential_tsunami: false,
        }
    }

    fn warning(id: &str, issued_at: DateTime<Utc>) -> TsunamiWarning {
        TsunamiWarning {
            warning_id: id.to_string(),
            related_event_id: "evt-1".to_string(),
            threat_level: ThreatLevel::Waspada,
            affected_zones: vec!["Zone".to_string()],
            estimated_arrival: issued_at,
            issued_at,
        }
    }

    fn at(minute: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + chrono::Duration::minutes(minute)
    }

    #[tokio::test]
    async fn with_no_since_every_event_comes_back() {
        let state = AppState::new();
        state.add_seismic_event(event("a", at(0))).await;
        state.add_seismic_event(event("b", at(1))).await;

        assert_eq!(state.seismic_events_since(None).await.len(), 2);
    }

    #[tokio::test]
    async fn since_is_strictly_exclusive() {
        let state = AppState::new();
        state.add_seismic_event(event("a", at(5))).await;

        assert_eq!(state.seismic_events_since(Some(at(5))).await.len(), 0, "an event at exactly `since` must not be returned");
        assert_eq!(state.seismic_events_since(Some(at(4))).await.len(), 1);
    }

    #[tokio::test]
    async fn results_are_sorted_oldest_first_regardless_of_insertion_order() {
        let state = AppState::new();
        state.add_seismic_event(event("later", at(10))).await;
        state.add_seismic_event(event("earlier", at(1))).await;

        let events = state.seismic_events_since(None).await;
        assert_eq!(events.iter().map(|e| e.event_id.as_str()).collect::<Vec<_>>(), vec!["earlier", "later"]);
    }

    #[tokio::test]
    async fn tsunami_warnings_are_filtered_by_issued_at_not_estimated_arrival() {
        let state = AppState::new();
        let mut w = warning("a", at(0));
        w.estimated_arrival = at(100);
        state.add_tsunami_warning(w).await;

        assert_eq!(state.tsunami_warnings_since(Some(at(1))).await.len(), 0);
        assert_eq!(state.tsunami_warnings_since(Some(at(0))).await.len(), 0);
        assert_eq!(state.tsunami_warnings_since(None).await.len(), 1);
    }

    #[tokio::test]
    async fn seismic_and_tsunami_storage_are_independent() {
        let state = AppState::new();
        state.add_seismic_event(event("a", at(0))).await;

        assert_eq!(state.seismic_event_count().await, 1);
        assert_eq!(state.tsunami_warnings_since(None).await.len(), 0);
    }

    #[tokio::test]
    async fn cloning_the_state_shares_the_same_storage() {
        let state = AppState::new();
        let clone = state.clone();

        clone.add_seismic_event(event("a", at(0))).await;

        assert_eq!(state.seismic_event_count().await, 1, "a clone must share storage, like an Arc handle");
    }
}
