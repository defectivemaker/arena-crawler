package arena

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const baseURL = "https://api.are.na/v3"

type UserCounts struct {
	Channels  int `json:"channels"`
	Followers int `json:"followers"`
	Following int `json:"following"`
}

type Bio struct {
	Plain string `json:"plain"`
}

type User struct {
	ID     int64      `json:"id"`
	Slug   string     `json:"slug"`
	Name   string     `json:"name"`
	Bio    *Bio       `json:"bio"`
	Counts UserCounts `json:"counts"`
}

type followersResponse struct {
	Data []User `json:"data"`
}

type Client struct {
	httpClient    *http.Client
	token         string
	minInterval   time.Duration
	jitter        time.Duration
	max429Backoff time.Duration
	mu            sync.Mutex
	nextRequestAt time.Time
	rng           *rand.Rand
}

func NewClient(token string, timeout time.Duration, requestsPerMinute int, jitter, max429Backoff time.Duration) *Client {
	minInterval := time.Duration(0)
	if requestsPerMinute > 0 {
		minInterval = time.Minute / time.Duration(requestsPerMinute)
	}
	return &Client{
		httpClient:    &http.Client{Timeout: timeout},
		token:         strings.TrimSpace(token),
		minInterval:   minInterval,
		jitter:        jitter,
		max429Backoff: max429Backoff,
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (c *Client) GetUser(ctx context.Context, slug string) (User, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return User{}, err
	}
	u.Path = path.Join(u.Path, "users", slug)

	var out User
	if err := c.getJSON(ctx, u.String(), &out); err != nil {
		return User{}, err
	}
	return out, nil
}

func (c *Client) GetFollowers(ctx context.Context, slug string, perPage int) ([]User, error) {
	if perPage <= 0 {
		perPage = 100
	}
	followers := make([]User, 0, perPage)

	page := 1
	for {
		u, err := url.Parse(baseURL)
		if err != nil {
			return nil, err
		}
		u.Path = path.Join(u.Path, "users", slug, "followers")
		q := u.Query()
		q.Set("page", fmt.Sprintf("%d", page))
		q.Set("per", fmt.Sprintf("%d", perPage))
		u.RawQuery = q.Encode()

		var payload followersResponse
		if err := c.getJSON(ctx, u.String(), &payload); err != nil {
			return nil, err
		}
		if len(payload.Data) == 0 {
			break
		}

		followers = append(followers, payload.Data...)
		if len(payload.Data) < perPage {
			break
		}
		page++
	}

	return followers, nil
}

func (c *Client) getJSON(ctx context.Context, requestURL string, out any) error {
	const maxAttempts = 6

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := c.waitForTurn(ctx); err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := c.backoffFor429(resp)
			resp.Body.Close()
			if attempt == maxAttempts {
				return fmt.Errorf("arena api %s returned 429 after %d attempts", requestURL, maxAttempts)
			}
			if err := sleepWithContext(ctx, wait); err != nil {
				return err
			}
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			resp.Body.Close()
			return fmt.Errorf("arena api %s returned %d: %s", requestURL, resp.StatusCode, strings.TrimSpace(string(body)))
		}

		err = json.NewDecoder(resp.Body).Decode(out)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("decode response for %s: %w", requestURL, err)
		}
		return nil
	}
	return fmt.Errorf("arena api %s exhausted retries", requestURL)
}

func (c *Client) waitForTurn(ctx context.Context) error {
	if c.minInterval <= 0 {
		return nil
	}

	c.mu.Lock()
	now := time.Now()
	when := now
	if c.nextRequestAt.After(now) {
		when = c.nextRequestAt
	}
	jitter := time.Duration(0)
	if c.jitter > 0 {
		jitter = time.Duration(c.rng.Int63n(int64(c.jitter) + 1))
	}
	next := when.Add(c.minInterval).Add(jitter)
	c.nextRequestAt = next
	c.mu.Unlock()

	if delay := time.Until(when); delay > 0 {
		return sleepWithContext(ctx, delay)
	}
	return nil
}

func (c *Client) randomJitter() time.Duration {
	if c.jitter <= 0 {
		return 0
	}
	c.mu.Lock()
	n := c.rng.Int63n(int64(c.jitter) + 1)
	c.mu.Unlock()
	return time.Duration(n)
}

func (c *Client) backoffFor429(resp *http.Response) time.Duration {
	if retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After")); retryAfter != "" {
		if secs, err := strconv.Atoi(retryAfter); err == nil && secs > 0 {
			d := time.Duration(secs) * time.Second
			if c.max429Backoff > 0 && d > c.max429Backoff {
				return c.max429Backoff
			}
			return d + c.randomJitter()
		}
	}

	if reset := strings.TrimSpace(resp.Header.Get("X-RateLimit-Reset")); reset != "" {
		if ts, err := strconv.ParseInt(reset, 10, 64); err == nil && ts > 0 {
			wait := time.Until(time.Unix(ts, 0)) + c.randomJitter()
			if wait < 0 {
				wait = time.Second
			}
			if c.max429Backoff > 0 && wait > c.max429Backoff {
				return c.max429Backoff
			}
			return wait
		}
	}

	if c.max429Backoff > 0 {
		return c.max429Backoff
	}
	return 30 * time.Second
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
