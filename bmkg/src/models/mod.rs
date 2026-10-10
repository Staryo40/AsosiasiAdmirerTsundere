use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Field names, types and casing here are the exact contract BMKG serves —
/// the Aggregator parses these literally.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeismicEvent {
    pub event_id: String,
    pub magnitude: f64,
    pub depth_km: f64,
    pub epicenter_lat: f64,
    pub epicenter_lon: f64,
    pub region_name: String,
    pub occurred_at: DateTime<Utc>,
    pub potential_tsunami: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThreatLevel {
    Waspada,
    Siaga,
    Awas,
}

/// `issued_at` is included alongside the five originally-expected fields:
/// `since` filtering needs a real creation timestamp, and `estimated_arrival`
/// is a future prediction, not one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TsunamiWarning {
    pub warning_id: String,
    pub related_event_id: String,
    pub threat_level: ThreatLevel,
    pub affected_zones: Vec<String>,
    pub estimated_arrival: DateTime<Utc>,
    pub issued_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn seismic_event_serializes_with_the_spec_field_names() {
        let event = SeismicEvent {
            event_id: "evt-000001".into(),
            magnitude: 6.2,
            depth_km: 12.5,
            epicenter_lat: -7.8,
            epicenter_lon: 110.4,
            region_name: "Yogyakarta".into(),
            occurred_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            potential_tsunami: false,
        };
        let json = serde_json::to_value(&event).unwrap();
        for field in ["event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon", "region_name", "occurred_at", "potential_tsunami"] {
            assert!(json.get(field).is_some(), "missing field: {field}");
        }
        assert_eq!(json.as_object().unwrap().len(), 8, "unexpected extra field in {json}");
    }

    #[test]
    fn threat_level_serializes_as_the_exact_spec_casing() {
        assert_eq!(serde_json::to_value(ThreatLevel::Waspada).unwrap(), "Waspada");
        assert_eq!(serde_json::to_value(ThreatLevel::Siaga).unwrap(), "Siaga");
        assert_eq!(serde_json::to_value(ThreatLevel::Awas).unwrap(), "Awas");
    }

    #[test]
    fn tsunami_warning_serializes_with_the_spec_fields_plus_issued_at() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 5, 0).unwrap();
        let warning = TsunamiWarning {
            warning_id: "tsw-000001".into(),
            related_event_id: "evt-000001".into(),
            threat_level: ThreatLevel::Siaga,
            affected_zones: vec!["Pesisir Yogyakarta".into()],
            estimated_arrival: now,
            issued_at: now,
        };
        let json = serde_json::to_value(&warning).unwrap();
        for field in ["warning_id", "related_event_id", "threat_level", "affected_zones", "estimated_arrival", "issued_at"] {
            assert!(json.get(field).is_some(), "missing field: {field}");
        }
    }
}
