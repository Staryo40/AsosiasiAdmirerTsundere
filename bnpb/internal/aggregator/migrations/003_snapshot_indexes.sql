-- Keyset boundaries and ordering use bytewise IDs, independent of database locale.
DROP INDEX hazard_page;
DROP INDEX hazard_source_page;
CREATE INDEX hazard_page ON hazard_events (occurred_at DESC, hazard_id COLLATE "C" DESC);
CREATE INDEX hazard_source_page ON hazard_events (source, occurred_at DESC, hazard_id COLLATE "C" DESC);
INSERT INTO schema_migrations (version) VALUES (3);
