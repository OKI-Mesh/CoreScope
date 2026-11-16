-- freshen-fixture.sql: shift every timestamp in the E2E database forward so the
-- newest one is "now", preserving the order between rows. Run it against a
-- fresh copy of the fixture before each server start.
--   psql -v ON_ERROR_STOP=1 -f tools/freshen-fixture.sql <database>
--
-- Each table shifts by its own offset (now minus its newest timestamp), and a
-- table's related columns shift by the same interval so the target's
-- first_seen <= last_seen style checks keep holding.
BEGIN;

-- Work in UTC. Subtracting two timestamptz values gives an interval counted in
-- 24-hour days, while adding that interval back counts calendar days in the
-- session time zone; across a daylight-saving change the two disagree by an hour.
SET LOCAL TIME ZONE 'UTC';

UPDATE nodes
   SET last_seen = last_seen + (SELECT now() - max(last_seen) FROM nodes)
 WHERE last_seen IS NOT NULL;

UPDATE transmissions
   SET first_seen = first_seen + shift.delta,
       created_at = created_at + shift.delta,
       last_seen  = last_seen  + shift.delta
  FROM (SELECT now() - max(first_seen) AS delta FROM transmissions) AS shift;

-- Per-node reach (/api/nodes/{pk}/reach?days=N) filters on observations.timestamp,
-- so without this the fixture's observations age out of the window and reach
-- returns no links.
UPDATE observations
   SET timestamp = timestamp + (SELECT now() - max(timestamp) FROM observations);

-- Observers must not look stale, or RemoveStaleObservers marks them inactive on
-- startup and /api/observers (and the map's observer markers) drops them.
UPDATE observers
   SET last_seen      = last_seen + shift.delta,
       last_packet_at = last_packet_at + shift.delta,
       inactive       = false
  FROM (SELECT now() - max(last_seen) AS delta FROM observers) AS shift;

UPDATE neighbor_edges
   SET last_seen = last_seen + (SELECT now() - max(last_seen) FROM neighbor_edges);

COMMIT;

SELECT 'nodes: max last_seen ' || max(last_seen) FROM nodes;
SELECT 'observers: count ' || count(*) || ' max last_seen ' || max(last_seen) FROM observers;
