-- name: GetScopeMatchTotals :one
SELECT counting_since, unique_matches, explicit_over_derived, ambiguous, none_matches
FROM scope_match_totals
WHERE id = 1;

-- name: UpsertScopeMatchTotals :exec
INSERT INTO scope_match_totals
    (id, counting_since, unique_matches, explicit_over_derived, ambiguous, none_matches, updated_at)
VALUES (1,
        sqlc.arg(counting_since)::timestamptz,
        sqlc.arg(unique_matches)::bigint,
        sqlc.arg(explicit_over_derived)::bigint,
        sqlc.arg(ambiguous)::bigint,
        sqlc.arg(none_matches)::bigint,
        sqlc.arg(updated_at)::timestamptz)
ON CONFLICT (id) DO UPDATE SET
    counting_since        = EXCLUDED.counting_since,
    unique_matches        = EXCLUDED.unique_matches,
    explicit_over_derived = EXCLUDED.explicit_over_derived,
    ambiguous             = EXCLUDED.ambiguous,
    none_matches          = EXCLUDED.none_matches,
    updated_at            = EXCLUDED.updated_at;