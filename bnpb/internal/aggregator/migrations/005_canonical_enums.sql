-- AI-assisted implementation; human review and AI-use disclosure are required.
-- Replace only the two inherited enum checks, in the startup migration transaction.
-- A normal CHECK validation scans existing rows; it does not rewrite their values.
ALTER TABLE hazard_events
    DROP CONSTRAINT hazard_events_source_check,
    DROP CONSTRAINT hazard_events_check,
    ADD CONSTRAINT hazard_events_source_check
        CHECK (source IN ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI')),
    ADD CONSTRAINT hazard_events_source_type_check CHECK (
        (source = 'BMKG' AND hazard_type = 'SEISMIC') OR
        (source = 'PVMBG' AND hazard_type = 'VOLCANIC') OR
        (source = 'SATELIT' AND hazard_type = 'WILDFIRE') OR
        (source = 'SENSOR_PANTAI' AND hazard_type = 'COASTAL'));

INSERT INTO schema_migrations (version) VALUES (5);
