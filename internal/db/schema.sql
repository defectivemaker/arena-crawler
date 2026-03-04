CREATE TABLE IF NOT EXISTS crawl_profiles (
  user_id BIGINT PRIMARY KEY,
  slug TEXT NOT NULL UNIQUE,
  name TEXT,
  bio_plain TEXT,
  followers_count INT NOT NULL DEFAULT 0,
  following_count INT NOT NULL DEFAULT 0,
  channels_count INT NOT NULL DEFAULT 0,
  profile_collected_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS crawl_profiles_slug_idx ON crawl_profiles (slug);

CREATE TABLE IF NOT EXISTS personal_links (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES crawl_profiles(user_id) ON DELETE CASCADE,
  user_slug TEXT NOT NULL,
  url TEXT NOT NULL,
  domain TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'bio_plain',
  collected_at TIMESTAMPTZ NOT NULL,
  UNIQUE (user_id, url)
);

CREATE INDEX IF NOT EXISTS personal_links_domain_idx ON personal_links (domain);
CREATE INDEX IF NOT EXISTS personal_links_user_slug_idx ON personal_links (user_slug);

CREATE TABLE IF NOT EXISTS crawl_state (
  slug TEXT PRIMARY KEY,
  depth INT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('queued', 'processing', 'done', 'failed')),
  attempts INT NOT NULL DEFAULT 0,
  last_enqueued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_attempt_at TIMESTAMPTZ,
  last_crawled_at TIMESTAMPTZ,
  next_retry_at TIMESTAMPTZ,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS crawl_state_status_idx ON crawl_state (status, next_retry_at, depth, last_enqueued_at);

-- Remove legacy graph storage to keep footprint low.
DROP TABLE IF EXISTS follower_edges;
