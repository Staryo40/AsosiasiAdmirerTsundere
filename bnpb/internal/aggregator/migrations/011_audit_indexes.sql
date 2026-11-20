-- Audit traverses ingestion order rather than the public occurrence-time order.
CREATE INDEX hazard_audit_ingest_page ON hazard_events (ingested_at, hazard_id COLLATE "C");
CREATE INDEX hazard_audit_source_ingest_page ON hazard_events (source, ingested_at, hazard_id COLLATE "C");
CREATE INDEX hazard_audit_reference_bytes ON hazard_events (source_ref_id COLLATE "C");
CREATE INDEX hazard_audit_source_reference_bytes ON hazard_events (source, source_ref_id COLLATE "C");
INSERT INTO schema_migrations (version) VALUES (11);
