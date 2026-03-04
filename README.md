# arena-crawler

Recursive Are.na follower crawler in Go, optimized for low storage.

It stores only:
- profile data (`slug`, `name`, `bio.plain`, counts)
- extracted personal website links from `bio.plain`
- crawl state for resume/retry/dedup (`crawl_state`)

It does **not** store follower-edge graph rows.

API optimization:
- follower pages (`/users/{slug}/followers?per=100`) are used as the primary profile source
- follower user payloads are upserted directly (profile + links)
- direct `GET /users/{slug}` is used only when a profile is missing/stale
- this reduces total requests substantially at larger depths

Built-in rate limiting:
- global request limiter in code (applies to all API calls)
- automatic 429 handling via `Retry-After` / `X-RateLimit-Reset`
- defaults target guest tier safely (`30 req/min`)

## 1) Start Postgres (optional via Docker)

```bash
cd /Users/jfat/dev/many-sites/arena-crawler
docker compose up -d
```

## 2) Create database + schema

Set an admin DSN that can create databases (usually points at `postgres` DB):

```bash
export PG_ADMIN_DSN='postgres://USER:PASSWORD@localhost:5432/postgres?sslmode=disable'
```

Create DB and tables:

```bash
go run ./cmd/initdb -db-name arena_personal_projects
```

Or explicitly pass admin DSN:

```bash
go run ./cmd/initdb \
  -admin-dsn 'postgres://arena:arena@localhost:5432/postgres?sslmode=disable' \
  -db-name arena_personal_projects
```

## 3) Crawl recursively with resume support

```bash
export DATABASE_URL='postgres://USER:PASSWORD@localhost:5432/arena_personal_projects?sslmode=disable'
# optional for authenticated/private access
export ARENA_TOKEN='your_arena_bearer_token'

go run ./cmd/crawl \
  -seed leticia-de-cassia \
  -max-depth -1 \
  -max-users -1 \
  -request-limit-per-minute 30 \
  -per-page 100
```

## Queue behavior

`crawl_state` tracks visited users and avoids recompute:
- `queued` -> waiting to process
- `processing` -> currently being processed
- `done` -> already crawled
- `failed` -> will retry after `next_retry_at`
- dequeue priority: lower depth first, then higher `followers_count`, then older queue time

Useful flags:
- `-max-depth` (default `-1`): follower recursion depth, unlimited when `-1`.
- `-max-users` (default `-1`): process cap per run, unlimited when `-1`.
- `-recrawl-after-hours` (default `720`): if `done` is older than this, it can be re-queued.
- `-retry-delay-minutes` (default `30`): backoff for failures.
- `-recover-processing-after-minutes` (default `30`): recovers stuck `processing` rows on startup.
- `-request-limit-per-minute` (default `30`): hard global request budget.
- `-rate-limit-jitter-ms` (default `200`): jitter added to request pacing/backoff.
- `-rate-limit-429-max-backoff-sec` (default `70`): max wait per 429 response.

You can run without `-seed` to continue from existing queued work.

## Useful queries

Queue status:

```sql
SELECT status, count(*)
FROM crawl_state
GROUP BY status
ORDER BY status;
```

Latest links:

```sql
SELECT user_slug, url, collected_at
FROM personal_links
ORDER BY collected_at DESC
LIMIT 200;
```

Top domains:

```sql
SELECT domain, count(*) AS n
FROM personal_links
GROUP BY domain
ORDER BY n DESC
LIMIT 100;
```
