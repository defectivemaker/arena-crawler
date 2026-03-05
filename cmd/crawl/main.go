package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"arena-crawler/internal/arena"
	"arena-crawler/internal/db"
	"arena-crawler/internal/extract"
)

var (
	defaultRequestLimitPerMinute      = 30
	defaultRateLimitJitterMs          = 200
	defaultRateLimit429MaxBackoffSecs = 70
)

func main() {
	seed := flag.String("seed", "", "Comma-separated seed user slugs (optional if queue already populated)")
	maxDepth := flag.Int("max-depth", -1, "Follower recursion depth (-1 = unlimited)")
	maxUsers := flag.Int("max-users", -1, "Maximum users to process in this run (-1 = unlimited)")
	perPage := flag.Int("per-page", 100, "Connections page size")
	maxPagesPerUser := flag.Int("max-pages-per-user", -1, "Max relation pages to fetch per user (-1 = unlimited)")
	edgeSource := flag.String("edge-source", "auto", "Traversal relation: auto, following, or followers")
	minRelationCount := flag.Int("min-relation-count", 20, "Skip traversal for users below this relation count (0 disables)")
	dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "Target Postgres DSN")
	token := flag.String("token", os.Getenv("ARENA_TOKEN"), "Are.na bearer token (optional)")
	requestTimeoutSec := flag.Int("request-timeout-sec", 20, "HTTP timeout per request")
	requestLimitPerMinute := flag.Int("request-limit-per-minute", defaultRequestLimitPerMinute, "Global API request cap")
	rateLimitJitterMs := flag.Int("rate-limit-jitter-ms", defaultRateLimitJitterMs, "Random jitter added to pacing/backoff")
	rateLimit429MaxBackoffSec := flag.Int("rate-limit-429-max-backoff-sec", defaultRateLimit429MaxBackoffSecs, "Max backoff on 429")
	recrawlAfterHours := flag.Int("recrawl-after-hours", 24*30, "Requeue profiles older than this")
	retryDelayMinutes := flag.Int("retry-delay-minutes", 30, "Delay before retrying failed slugs")
	recoverProcessingAfterMinutes := flag.Int("recover-processing-after-minutes", 30, "Requeue stale processing items older than this")
	logFilePath := flag.String("log-file", "crawl.log", "Log file path (append mode)")
	flag.Parse()

	if *dsn == "" {
		log.Fatal("missing -dsn (or DATABASE_URL)")
	}
	if *edgeSource != "following" && *edgeSource != "followers" && *edgeSource != "auto" {
		log.Fatal("invalid -edge-source; expected auto, following, or followers")
	}
	logFile, err := os.OpenFile(*logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("open log file %s: %v", *logFilePath, err)
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.Printf("starting crawl log_file=%s edge_source=%s per_page=%d max_pages_per_user=%d min_relation_count=%d", *logFilePath, *edgeSource, *perPage, *maxPagesPerUser, *minRelationCount)

	ctx := context.Background()
	store, err := db.Open(*dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer store.Close()

	if err := store.Ping(ctx); err != nil {
		log.Fatalf("ping db: %v", err)
	}

	now := time.Now().UTC()
	staleBefore := now.Add(-time.Duration(*recrawlAfterHours) * time.Hour)
	retryBefore := now
	retryDelay := time.Duration(*retryDelayMinutes) * time.Minute

	recovered, err := store.RequeueStuckProcessing(ctx, now.Add(-time.Duration(*recoverProcessingAfterMinutes)*time.Minute))
	if err != nil {
		log.Fatalf("recover processing queue items: %v", err)
	}
	if recovered > 0 {
		log.Printf("recovered_stuck_processing=%d", recovered)
	}

	seeded := 0
	for _, raw := range strings.Split(*seed, ",") {
		slug := normalizeSlug(raw)
		if slug == "" {
			continue
		}
		if err := store.EnqueueSlug(ctx, slug, 0, staleBefore, retryBefore); err != nil {
			log.Printf("seed enqueue failed slug=%s err=%v", slug, err)
			continue
		}
		seeded++
	}
	if seeded > 0 {
		log.Printf("seeded=%d", seeded)
	}

	client := arena.NewClient(
		strings.TrimSpace(*token),
		time.Duration(*requestTimeoutSec)*time.Second,
		*requestLimitPerMinute,
		time.Duration(*rateLimitJitterMs)*time.Millisecond,
		time.Duration(*rateLimit429MaxBackoffSec)*time.Second,
	)

	processed := 0
	linksSaved := 0
	failed := 0
	profileFetches := 0

	for *maxUsers < 0 || processed < *maxUsers {
		item, ok, err := store.DequeueNext(ctx, *edgeSource)
		if err != nil {
			log.Fatalf("dequeue: %v", err)
		}
		if !ok {
			break
		}

		collectedAt := time.Now().UTC()
		counts, hasCounts, err := store.GetProfileCounts(ctx, item.Slug)
		if err != nil {
			failed++
			retryAt := time.Now().UTC().Add(retryDelay)
			_ = store.MarkFailed(ctx, item.Slug, err.Error(), retryAt)
			log.Printf("counts fetch failed user=%s depth=%d attempts=%d err=%v", item.Slug, item.Depth, item.Attempts, err)
			continue
		}
		hasFresh, err := store.HasFreshProfile(ctx, item.Slug, staleBefore)
		if err != nil {
			failed++
			retryAt := time.Now().UTC().Add(retryDelay)
			_ = store.MarkFailed(ctx, item.Slug, err.Error(), retryAt)
			log.Printf("freshness check failed user=%s depth=%d attempts=%d err=%v", item.Slug, item.Depth, item.Attempts, err)
			continue
		}
		if !hasFresh {
			user, err := client.GetUser(ctx, item.Slug)
			if err != nil {
				var apiErr *arena.APIError
				if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
					_ = store.MarkDone(ctx, item.Slug, collectedAt)
					log.Printf("skip missing/non-user slug=%s depth=%d attempts=%d", item.Slug, item.Depth, item.Attempts)
					processed++
					continue
				}
				failed++
				retryAt := time.Now().UTC().Add(retryDelay)
				_ = store.MarkFailed(ctx, item.Slug, err.Error(), retryAt)
				log.Printf("failed user=%s depth=%d attempts=%d err=%v", item.Slug, item.Depth, item.Attempts, err)
				continue
			}
			profileFetches++

			if err := store.UpsertProfile(ctx, user, collectedAt); err != nil {
				failed++
				retryAt := time.Now().UTC().Add(retryDelay)
				_ = store.MarkFailed(ctx, item.Slug, err.Error(), retryAt)
				log.Printf("profile upsert failed user=%s depth=%d attempts=%d err=%v", item.Slug, item.Depth, item.Attempts, err)
				continue
			}
			counts = db.ProfileCounts{
				Followers: user.Counts.Followers,
				Following: user.Counts.Following,
				Channels:  user.Counts.Channels,
			}
			hasCounts = true

			for _, link := range linksFromBio(user.Bio) {
				if err := store.UpsertPersonalLink(ctx, user, link, collectedAt); err != nil {
					log.Printf("link upsert failed user=%s link=%s err=%v", user.Slug, link.URL, err)
					continue
				}
				linksSaved++
			}
		}

		traversalRelation, relationCount := pickRelation(*edgeSource, counts, hasCounts)
		if (*maxDepth < 0 || item.Depth < *maxDepth) && *minRelationCount > 0 && relationCount < *minRelationCount {
			if err := store.MarkDone(ctx, item.Slug, collectedAt); err != nil {
				log.Printf("mark done failed user=%s err=%v", item.Slug, err)
			}
			processed++
			log.Printf("skip low-relation user=%s depth=%d attempts=%d relation=%s relation_count=%d threshold=%d total_users=%d total_links=%d", item.Slug, item.Depth, item.Attempts, traversalRelation, relationCount, *minRelationCount, processed, linksSaved)
			continue
		}

		if *maxDepth < 0 || item.Depth < *maxDepth {
			var connections []arena.User
			if traversalRelation == "following" {
				connections, err = client.GetFollowing(ctx, item.Slug, *perPage, *maxPagesPerUser)
			} else {
				connections, err = client.GetFollowers(ctx, item.Slug, *perPage, *maxPagesPerUser)
			}
			if err != nil {
				var apiErr *arena.APIError
				if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
					_ = store.MarkDone(ctx, item.Slug, collectedAt)
					log.Printf("skip missing/non-user relation source slug=%s depth=%d attempts=%d edge_source=%s traversal_relation=%s", item.Slug, item.Depth, item.Attempts, *edgeSource, traversalRelation)
					processed++
					continue
				}
				failed++
				retryAt := time.Now().UTC().Add(retryDelay)
				_ = store.MarkFailed(ctx, item.Slug, err.Error(), retryAt)
				log.Printf("connections fetch failed user=%s depth=%d attempts=%d edge_source=%s traversal_relation=%s err=%v", item.Slug, item.Depth, item.Attempts, *edgeSource, traversalRelation, err)
				continue
			}

			for _, connection := range connections {
				if connection.Type != "" && !strings.EqualFold(connection.Type, "User") {
					continue
				}
				nextSlug := normalizeSlug(connection.Slug)
				if nextSlug == "" {
					continue
				}
				if err := store.UpsertProfile(ctx, connection, collectedAt); err != nil {
					log.Printf("profile upsert failed user=%s err=%v", connection.Slug, err)
				} else {
					for _, link := range linksFromBio(connection.Bio) {
						if err := store.UpsertPersonalLink(ctx, connection, link, collectedAt); err != nil {
							log.Printf("link upsert failed user=%s link=%s err=%v", connection.Slug, link.URL, err)
							continue
						}
						linksSaved++
					}
				}
				if err := store.EnqueueSlug(ctx, nextSlug, item.Depth+1, staleBefore, retryBefore); err != nil {
					log.Printf("enqueue connection failed source=%s connection=%s edge_source=%s traversal_relation=%s err=%v", item.Slug, nextSlug, *edgeSource, traversalRelation, err)
				}
			}
		}

		if err := store.MarkDone(ctx, item.Slug, collectedAt); err != nil {
			log.Printf("mark done failed user=%s err=%v", item.Slug, err)
		}
		processed++
		log.Printf("processed user=%s depth=%d attempts=%d edge_source=%s traversal_relation=%s relation_count=%d total_users=%d total_links=%d profile_fetches=%d failed=%d", item.Slug, item.Depth, item.Attempts, *edgeSource, traversalRelation, relationCount, processed, linksSaved, profileFetches, failed)
	}

	fmt.Printf("done users=%d links=%d profile_fetches=%d failed=%d\n", processed, linksSaved, profileFetches, failed)
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

func linksFromBio(bio *arena.Bio) []extract.Link {
	if bio == nil {
		return nil
	}
	return extract.ExtractPersonalLinks(bio.Plain)
}

func pickRelation(mode string, counts db.ProfileCounts, hasCounts bool) (string, int) {
	switch mode {
	case "following":
		return "following", counts.Following
	case "followers":
		return "followers", counts.Followers
	default:
		if !hasCounts {
			return "following", 0
		}
		if counts.Following >= counts.Followers {
			return "following", counts.Following
		}
		return "followers", counts.Followers
	}
}
