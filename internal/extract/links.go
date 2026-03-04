package extract

import (
	"net/url"
	"regexp"
	"sort"
	"strings"
)

type Link struct {
	URL    string
	Domain string
}

var urlPattern = regexp.MustCompile(`(?i)(https?://[^\s<>")\]]+|(?:[a-z0-9-]+\.)+[a-z]{2,}(?:/[^\s<>")\]]*)?)`)

var blockedDomains = []string{
	"are.na",
	"instagram.com",
	"facebook.com",
	"fb.com",
	"m.facebook.com",
	"x.com",
	"twitter.com",
	"t.co",
	"threads.net",
	"tiktok.com",
	"youtube.com",
	"youtu.be",
	"linkedin.com",
	"pinterest.com",
	"snapchat.com",
	"discord.gg",
	"discord.com",
	"telegram.me",
	"t.me",
	"linktr.ee",
	"linktree.com",
	"beacons.ai",
	"campsite.bio",
	"wixsite.com",
}

func ExtractPersonalLinks(text string) []Link {
	if strings.TrimSpace(text) == "" {
		return nil
	}

	matches := urlPattern.FindAllString(text, -1)
	if len(matches) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(matches))
	out := make([]Link, 0, len(matches))
	for _, raw := range matches {
		normalized := normalizeCandidate(raw)
		if normalized == "" {
			continue
		}

		u, err := url.Parse(normalized)
		if err != nil || u.Host == "" {
			continue
		}
		host := normalizeHost(u.Host)
		if host == "" || isBlockedDomain(host) || !strings.Contains(host, ".") {
			continue
		}

		cleanURL := u.Scheme + "://" + host + strings.TrimRight(u.EscapedPath(), "/")
		if u.RawQuery != "" {
			cleanURL += "?" + u.RawQuery
		}

		if _, ok := seen[cleanURL]; ok {
			continue
		}
		seen[cleanURL] = struct{}{}
		out = append(out, Link{URL: cleanURL, Domain: host})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].URL < out[j].URL })
	return out
}

func normalizeCandidate(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimRight(s, ".,;:!?)\"]")
	if s == "" {
		return ""
	}
	if !strings.HasPrefix(strings.ToLower(s), "http://") && !strings.HasPrefix(strings.ToLower(s), "https://") {
		s = "https://" + s
	}
	return s
}

func normalizeHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.TrimPrefix(host, "www.")
	return host
}

func isBlockedDomain(host string) bool {
	for _, blocked := range blockedDomains {
		if host == blocked || strings.HasSuffix(host, "."+blocked) {
			return true
		}
	}
	return false
}
