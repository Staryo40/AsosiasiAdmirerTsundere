-- Replace only the two enum checks left by 005_coastal.sql, in the startup migration transaction.
-- A normal CHECK validation scans existing rows; it does not rewrite their values.
ALTER TABLE hazard_events
    DROP CONSTRAINT IF EXISTS hazard_events_source_check,
    DROP CONSTRAINT IF EXISTS hazard_events_source_type_check,
    ADD CONSTRAINT hazard_events_source_check
        CHECK (source IN ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI')),
    ADD CONSTRAINT hazard_events_source_type_check CHECK (
        (source = 'BMKG' AND hazard_type = 'SEISMIC') OR
        (source = 'PVMBG' AND hazard_type = 'VOLCANIC') OR
        (source = 'SATELIT' AND hazard_type = 'WILDFIRE') OR
        (source = 'SENSOR_PANTAI' AND hazard_type = 'COASTAL'));

INSERT INTO schema_migrations (version) VALUES (6);
