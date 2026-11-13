-- 00005_noise_floor_real_v1.sql
-- +goose Up
UPDATE observers SET noise_floor = CAST(noise_floor AS REAL)
	WHERE noise_floor IS NOT NULL AND typeof(noise_floor) = 'integer';

-- +goose Down
-- No meaningful rollback (type-affinity cast, not reversible).