package db

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"arena-crawler/internal/arena"
	"arena-crawler/internal/extract"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed schema.sql
var schemaSQL string

type Store struct {
	db *sql.DB
}

type QueueItem struct {
	Slug     string
	Depth    int
	Attempts int
}

type ProfileCounts struct {
	Followers int
	Following int
	Channels  int
}

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func ApplySchema(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

func (s *Store) UpsertProfile(ctx context.Context, user arena.User, collectedAt time.Time) error {
	bioPlain := ""
	if user.Bio != nil {
		bioPlain = user.Bio.Plain
	}

	_, err := s.db.ExecContext(ctx, `
INSERT INTO crawl_profiles (
  user_id, slug, name, bio_plain, followers_count, following_count, channels_count, profile_collected_at, updated_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
ON CONFLICT (user_id)
DO UPDATE SET
  slug = EXCLUDED.slug,
  name = EXCLUDED.name,
  bio_plain = EXCLUDED.bio_plain,
  followers_count = EXCLUDED.followers_count,
  following_count = EXCLUDED.following_count,
  channels_count = EXCLUDED.channels_count,
  profile_collected_at = EXCLUDED.profile_collected_at,
  updated_at = NOW();
`, user.ID, user.Slug, user.Name, bioPlain, user.Counts.Followers, user.Counts.Following, user.Counts.Channels, collectedAt)
	if err != nil {
		return fmt.Errorf("upsert profile %s: %w", user.Slug, err)
	}
	return nil
}

func (s *Store) HasFreshProfile(ctx context.Context, slug string, staleBefore time.Time) (bool, error) {
	slug = normalizeSlug(slug)
	var exists bool
	err := s.db.QueryRowContext(ctx, `
SELECT EXISTS(
  SELECT 1
  FROM crawl_profiles
  WHERE slug = $1
    AND profile_collected_at >= $2
);
`, slug, staleBefore).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check fresh profile %s: %w", slug, err)
	}
	return exists, nil
}

func (s *Store) UpsertPersonalLink(ctx context.Context, user arena.User, link extract.Link, collectedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO personal_links (user_id, user_slug, url, domain, source, collected_at)
VALUES ($1, $2, $3, $4, 'bio_plain', $5)
ON CONFLICT (user_id, url)
DO UPDATE SET
  domain = EXCLUDED.domain,
  user_slug = EXCLUDED.user_slug,
  collected_at = EXCLUDED.collected_at;
`, user.ID, user.Slug, link.URL, link.Domain, collectedAt)
	if err != nil {
		return fmt.Errorf("upsert personal link %s (%s): %w", user.Slug, link.URL, err)
	}
	return nil
}

func (s *Store) EnqueueSlug(ctx context.Context, slug string, depth int, staleBefore, retryBefore time.Time) error {
	slug = normalizeSlug(slug)
	if slug == "" {
		return nil
	}

	_, err := s.db.ExecContext(ctx, `
INSERT INTO crawl_state (slug, depth, status, last_enqueued_at)
VALUES ($1, $2, 'queued', NOW())
ON CONFLICT (slug)
DO UPDATE SET
  depth = LEAST(crawl_state.depth, EXCLUDED.depth),
  status = CASE
    WHEN crawl_state.status = 'done' AND (crawl_state.last_crawled_at IS NULL OR crawl_state.last_crawled_at < $3)
      THEN 'queued'
    WHEN crawl_state.status = 'failed' AND (crawl_state.next_retry_at IS NULL OR crawl_state.next_retry_at <= $4)
      THEN 'queued'
    ELSE crawl_state.status
  END,
  last_enqueued_at = CASE
    WHEN (crawl_state.status = 'done' AND (crawl_state.last_crawled_at IS NULL OR crawl_state.last_crawled_at < $3))
      OR (crawl_state.status = 'failed' AND (crawl_state.next_retry_at IS NULL OR crawl_state.next_retry_at <= $4))
      OR crawl_state.status = 'queued'
      THEN NOW()
    ELSE crawl_state.last_enqueued_at
  END;
`, slug, depth, staleBefore, retryBefore)
	if err != nil {
		return fmt.Errorf("enqueue slug %s: %w", slug, err)
	}
	return nil
}

func (s *Store) RequeueStuckProcessing(ctx context.Context, before time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
UPDATE crawl_state
SET status = 'queued',
    next_retry_at = NULL,
    last_error = '[auto] recovered stuck processing item',
    last_enqueued_at = NOW()
WHERE status = 'processing' AND (last_attempt_at IS NULL OR last_attempt_at < $1);
`, before)
	if err != nil {
		return 0, fmt.Errorf("requeue stuck processing: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *Store) DequeueNext(ctx context.Context, edgeSource string) (QueueItem, bool, error) {
	if edgeSource != "following" && edgeSource != "followers" && edgeSource != "auto" {
		edgeSource = "followers"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return QueueItem{}, false, err
	}
	defer tx.Rollback()

	var item QueueItem
	err = tx.QueryRowContext(ctx, `
WITH next_item AS (
  SELECT cs.slug, cs.depth
  FROM crawl_state cs
  WHERE cs.status = 'queued'
    AND (cs.next_retry_at IS NULL OR cs.next_retry_at <= NOW())
  ORDER BY
    COALESCE((
      SELECT CASE
        WHEN $1 = 'following' THEN cp.following_count
        WHEN $1 = 'followers' THEN cp.followers_count
        ELSE GREATEST(cp.following_count, cp.followers_count)
      END
      FROM crawl_profiles cp
      WHERE cp.slug = cs.slug
    ), 0) DESC,
    cs.depth ASC,
    cs.last_enqueued_at ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE crawl_state AS cs
SET status = 'processing',
    attempts = cs.attempts + 1,
    last_attempt_at = NOW(),
    last_error = NULL
FROM next_item
WHERE cs.slug = next_item.slug
RETURNING cs.slug, cs.depth, cs.attempts;
	`, edgeSource).Scan(&item.Slug, &item.Depth, &item.Attempts)
	if err == sql.ErrNoRows {
		if commitErr := tx.Commit(); commitErr != nil {
			return QueueItem{}, false, commitErr
		}
		return QueueItem{}, false, nil
	}
	if err != nil {
		return QueueItem{}, false, fmt.Errorf("dequeue next: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return QueueItem{}, false, err
	}
	return item, true, nil
}

func (s *Store) GetProfileCounts(ctx context.Context, slug string) (ProfileCounts, bool, error) {
	slug = normalizeSlug(slug)
	var counts ProfileCounts
	err := s.db.QueryRowContext(ctx, `
SELECT followers_count, following_count, channels_count
FROM crawl_profiles
WHERE slug = $1;
`, slug).Scan(&counts.Followers, &counts.Following, &counts.Channels)
	if err == sql.ErrNoRows {
		return ProfileCounts{}, false, nil
	}
	if err != nil {
		return ProfileCounts{}, false, fmt.Errorf("get profile counts %s: %w", slug, err)
	}
	return counts, true, nil
}

func (s *Store) MarkDone(ctx context.Context, slug string, crawledAt time.Time) error {
	slug = normalizeSlug(slug)
	_, err := s.db.ExecContext(ctx, `
UPDATE crawl_state
SET status = 'done',
    last_crawled_at = $2,
    next_retry_at = NULL,
    last_error = NULL
WHERE slug = $1;
`, slug, crawledAt)
	if err != nil {
		return fmt.Errorf("mark done %s: %w", slug, err)
	}
	return nil
}

func (s *Store) MarkFailed(ctx context.Context, slug, errMsg string, nextRetryAt time.Time) error {
	slug = normalizeSlug(slug)
	errMsg = strings.TrimSpace(errMsg)
	if len(errMsg) > 4000 {
		errMsg = errMsg[:4000]
	}

	_, err := s.db.ExecContext(ctx, `
UPDATE crawl_state
SET status = 'failed',
    next_retry_at = $2,
    last_error = $3
WHERE slug = $1;
`, slug, nextRetryAt, errMsg)
	if err != nil {
		return fmt.Errorf("mark failed %s: %w", slug, err)
	}
	return nil
}

func normalizeSlug(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.TrimPrefix(s, "https://www.are.na/")
	s = strings.TrimPrefix(s, "https://are.na/")
	s = strings.TrimPrefix(s, "www.are.na/")
	s = strings.TrimPrefix(s, "are.na/")
	s = strings.TrimPrefix(s, "users/")
	return strings.Trim(s, "/")
}
