-- +goose Up
-- #730: marks nodes whose ADVERT GPS lies outside the configured
-- geofilter polygon.
ALTER TABLE nodes ADD COLUMN foreign_advert INTEGER DEFAULT 0;
ALTER TABLE inactive_nodes ADD COLUMN foreign_advert INTEGER DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_nodes_foreign_advert ON nodes(foreign_advert) WHERE foreign_advert = 1;

-- +goose Down
DROP INDEX IF EXISTS idx_nodes_foreign_advert;
-- SQLite DROP COLUMN requires 3.35+; columns left as documented no-op.