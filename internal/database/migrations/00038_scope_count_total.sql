-- +goose Up
CREATE TABLE IF NOT EXISTS scope_match_totals (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    since_unix INTEGER NOT NULL,
    unique_matches INTEGER NOT NULL,
    explicit_over_derived INTEGER NOT NULL,
    ambiguous INTEGER NOT NULL,
    none_matches INTEGER NOT NULL,
    updated_unix INTEGER NOT NULL
);
-- +goose Down
DROP TABLE IF EXISTS scope_match_totals;