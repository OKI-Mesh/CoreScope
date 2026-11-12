-- e2e-seed.sql: rows the E2E suite needs that the captured fixture lacks. Applied
-- once, after the fixture is loaded and freshened, when the template database is
-- built. Times are relative to the freshened fixture, so the seed stays valid as
-- the calendar moves on.

-- Observer IATA codes. The captured observers carry none, and the map and the
-- observer filters need a spread.
UPDATE observers
   SET iata = CASE id % 5 WHEN 0 THEN 'IND' WHEN 1 THEN 'FWA' WHEN 2 THEN 'EVV' WHEN 3 THEN 'CVG' WHEN 4 THEN 'DAY' END
 WHERE iata IS NULL OR iata = '';

-- #1486 collapse test: a transmission seen by 3 observers. The captured fixture's
-- transmissions each have one observation, so the packets page would render only
-- flat rows and the toggle-select (grouped) row the repro needs would not exist.
--
-- raw_hex 15000102030405060708090a0b0c0d0e0f has header 0x15, route_type 1 and
-- payload_type 5. The row sorts LAST in both default packets views: the flat view
-- orders by transmissions.id DESC (id 0), and the grouped view by the newest
-- observation time, which is two days older than every other observation. That
-- also keeps it out of the default 15-minute window, so the
-- test-e2e-playwright first-10-rows hex-pane test is unaffected; the #1486 test
-- reaches it by hash with ?timeWindow=0.
INSERT INTO transmissions (id, raw_hex, hash, first_seen, last_seen, route_type, payload_type, payload_version,
                           decoded_json, channel_hash, from_pubkey)
OVERRIDING SYSTEM VALUE
VALUES (0, decode('15000102030405060708090a0b0c0d0e0f', 'hex'), 'fae0c9e6d357a814',
        (SELECT min(timestamp) FROM observations) - interval '2 days',
        (SELECT min(timestamp) FROM observations) - interval '2 days', 1, 5, 0,
        '{"type":"CHAN","channel":"#test","text":"#1486 fixture"}', NULL, NULL);

INSERT INTO observations (transmission_id, observer_idx, direction, snr, rssi, score,
                          path_hash, timestamp, hop_prefixes, hop_nodes)
SELECT 0, hop.observer, 'rx', hop.snr, hop.rssi, 0,
       encode(sha256(convert_to(hop.prefix, 'UTF8')), 'hex'),
       (SELECT first_seen FROM transmissions WHERE id = 0),
       ARRAY[hop.prefix], '{}'::integer[]
  FROM (VALUES (1, 'aa', 5.0, -95), (2, 'bb', 5.5, -92), (3, 'cc', 6.0, -90)) AS hop(observer, prefix, snr, rssi);

-- #1791 Group Data filter test: a single GRP_DATA (payload_type 6) packet. Its
-- observation is "now" so it shows in the default window with no override.
--
-- Header byte 0x19 is payload 6 (GRP_DATA) over route 1 (FLOOD). The path length
-- byte 0x00 means zero hops, so the observation carries no hops either, which
-- keeps the rendered hop-row count equal to the hex path's byte count (the
-- "hex strip Path range matches hop row count" test).
--
-- id -1000000 is a deliberate out-of-band sentinel: it cannot collide with a real
-- ingested transmission, whose ids come from the identity sequence and are positive.
INSERT INTO transmissions (id, raw_hex, hash, first_seen, last_seen, route_type, payload_type, payload_version,
                           decoded_json, channel_hash, from_pubkey)
OVERRIDING SYSTEM VALUE
VALUES (-1000000, decode('19000102030405060708090a0b0c0d0e0f', 'hex'), '17910000deadbeef', now(), now(), 1, 6, 0,
        '{"type":"GRP_DATA","channel":"#test","raw":"deadbeef"}', NULL, NULL);

INSERT INTO observations (transmission_id, observer_idx, direction, snr, rssi, score,
                          path_hash, timestamp, hop_prefixes, hop_nodes)
VALUES (-1000000, 1, 'rx', 7.0, -88, 0, NULL, now(), '{}', '{}');
