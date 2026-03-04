package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	copilotTokenExchangeURL = "https://api.github.com/copilot_internal/v2/token"
	copilotUserAgent        = "GitHubCopilotChat/0.35.0"
	copilotEditorVersion    = "vscode/1.107.0"
	copilotPluginVersion    = "copilot-chat/0.35.0"
	copilotAPIVersion       = "2025-04-01"
	tokenRefreshBuffer      = 5 * time.Minute
	copilotTokenHTTPTimeout = 30 * time.Second
)

// cachedCopilotToken holds a Copilot session token with its expiry.
type cachedCopilotToken struct {
	token     string
	expiresAt time.Time
}

// copilotTokenResponse is the JSON response from the token exchange endpoint.
type copilotTokenResponse struct {
	Token     string `json:"token"`
	ExpiresAt int64  `json:"expires_at"`
}

// CopilotTokenProvider exchanges GitHub OAuth tokens for Copilot session tokens
// and caches them per account ID with automatic TTL-based refresh.
type CopilotTokenProvider struct {
	mu         sync.RWMutex
	cache      map[int64]*cachedCopilotToken
	httpClient *http.Client
	sf         singleflight.Group
}

func NewCopilotTokenProvider() *CopilotTokenProvider {
	return &CopilotTokenProvider{
		cache:      make(map[int64]*cachedCopilotToken),
		httpClient: &http.Client{Timeout: copilotTokenHTTPTimeout},
	}
}

// GetAccessToken returns a valid Copilot session token for the given account.
// Uses singleflight to prevent thundering herd on cache miss.
func (p *CopilotTokenProvider) GetAccessToken(ctx context.Context, account *Account) (string, error) {
	if account == nil {
		return "", errors.New("account is nil")
	}
	if account.Platform != PlatformCopilot {
		return "", errors.New("not a copilot account")
	}

	// Check cache (read lock)
	if token := p.getCached(account.ID); token != "" {
		return token, nil
	}

	githubToken := account.GetCredential("access_token")
	if githubToken == "" {
		return "", errors.New("access_token not found in credentials")
	}

	// Singleflight: coalesce concurrent exchanges for the same account
	key := fmt.Sprintf("exchange:%d", account.ID)
	val, err, _ := p.sf.Do(key, func() (any, error) {
		// Double-check cache after winning the singleflight race
		if token := p.getCached(account.ID); token != "" {
			return token, nil
		}
		sessionToken, expiresAt, err := p.exchangeToken(ctx, githubToken)
		if err != nil {
			return "", err
		}
		p.setCache(account.ID, sessionToken, expiresAt.Add(-tokenRefreshBuffer))
		return sessionToken, nil
	})
	if err != nil {
		return "", fmt.Errorf("copilot token exchange failed: %w", err)
	}
	return val.(string), nil
}

// InvalidateCache removes the cached token for the given account ID.
// Used when upstream returns 401 to force a re-exchange.
func (p *CopilotTokenProvider) InvalidateCache(accountID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.cache, accountID)
}

// getCached returns the cached token if it exists and hasn't expired.
func (p *CopilotTokenProvider) getCached(accountID int64) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if cached, ok := p.cache[accountID]; ok && time.Now().Before(cached.expiresAt) {
		return cached.token
	}
	return ""
}

// setCache stores a token in the cache with the given expiry time.
func (p *CopilotTokenProvider) setCache(accountID int64, token string, expiresAt time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cache[accountID] = &cachedCopilotToken{
		token:     token,
		expiresAt: expiresAt,
	}
}

// exchangeToken exchanges a GitHub OAuth token for a Copilot session token.
// GET https://api.github.com/copilot_internal/v2/token
// Auth: "token {github_access_token}" (NOT "Bearer")
func (p *CopilotTokenProvider) exchangeToken(ctx context.Context, githubToken string) (string, time.Time, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, copilotTokenExchangeURL, nil)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("create exchange request: %w", err)
	}
	req.Header.Set("Authorization", "token "+githubToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", copilotUserAgent)
	req.Header.Set("Editor-Version", copilotEditorVersion)
	req.Header.Set("Editor-Plugin-Version", copilotPluginVersion)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("token exchange request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB limit
	if err != nil {
		return "", time.Time{}, fmt.Errorf("read exchange response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("token exchange returned %d: %s", resp.StatusCode, string(body))
	}

	var result copilotTokenResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", time.Time{}, fmt.Errorf("decode exchange response: %w", err)
	}
	if result.Token == "" {
		return "", time.Time{}, errors.New("token exchange returned empty token")
	}
	if result.ExpiresAt == 0 {
		return "", time.Time{}, errors.New("token exchange returned zero expires_at")
	}

	expiresAt := time.Unix(result.ExpiresAt, 0)
	if expiresAt.Before(time.Now()) {
		return "", time.Time{}, fmt.Errorf("token exchange returned past expiry: %v", expiresAt)
	}
	return result.Token, expiresAt, nil
}
