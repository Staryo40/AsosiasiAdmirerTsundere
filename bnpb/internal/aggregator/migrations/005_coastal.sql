-- Coastal sensor (P5): extend canonical model and poll state.
ALTER TABLE hazard_events DROP CONSTRAINT IF EXISTS hazard_events_source_check;
ALTER TABLE hazard_events ADD CONSTRAINT hazard_events_source_check
    CHECK (source IN ('BMKG', 'PVMBG', 'SENSOR_PANTAI'));

ALTER TABLE hazard_events DROP CONSTRAINT IF EXISTS hazard_events_check;
ALTER TABLE hazard_events ADD CONSTRAINT hazard_events_source_type_check CHECK (
    (source = 'BMKG' AND hazard_type = 'SEISMIC') OR
    (source = 'PVMBG' AND hazard_type = 'VOLCANIC') OR
    (source = 'SENSOR_PANTAI' AND hazard_type = 'COASTAL')
);

ALTER TABLE poll_state DROP CONSTRAINT IF EXISTS poll_state_endpoint_check;
ALTER TABLE poll_state ADD CONSTRAINT poll_state_endpoint_check
    CHECK (endpoint IN ('seismic', 'warnings', 'volcanic', 'coastal'));

ALTER TABLE poll_state DROP CONSTRAINT IF EXISTS poll_state_check;
ALTER TABLE poll_state ADD CONSTRAINT poll_state_source_endpoint_check CHECK (
    (source = 'BMKG' AND endpoint IN ('seismic', 'warnings')) OR
    (source = 'PVMBG' AND endpoint = 'volcanic') OR
    (source = 'SENSOR_PANTAI' AND endpoint = 'coastal')
);

ALTER TABLE poll_state ADD COLUMN IF NOT EXISTS resume_token text;

INSERT INTO poll_state (endpoint, source) VALUES ('coastal', 'SENSOR_PANTAI')
    ON CONFLICT (endpoint) DO NOTHING;

INSERT INTO schema_migrations (version) VALUES (5);