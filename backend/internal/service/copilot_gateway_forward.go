package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

// setCopilotBaseHeaders sets the Copilot upstream request headers.
// Uses CLIProxyAPIPlus-style headers for Copilot API compatibility.
func (s *CopilotGatewayService) setCopilotBaseHeaders(req *http.Request, token string) {
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", copilotUserAgent)
	req.Header.Set("Editor-Version", copilotEditorVersion)
	req.Header.Set("Editor-Plugin-Version", copilotPluginVersion)
	req.Header.Set("X-Request-Id", uuid.New().String())
	req.Header.Set("Copilot-Integration-Id", copilotIntegrationID)
	req.Header.Set("Openai-Intent", copilotOpenAIIntent)
}

// setCopilotMessagesHeaders sets headers specific to the /v1/messages endpoint.
//
// Headers set:
//   - Content-Type: application/json
//   - X-Initiator: user/agent (from last message role)
//   - Copilot-Vision-Request: true (if request contains image blocks)
//   - Anthropic-Beta: transparently forwarded from client, filtered for claude-code;
//     if absent but body contains thinking.budget_tokens, adds interleaved-thinking beta
//   - Anthropic-Version: transparently forwarded from client (if present)
func (s *CopilotGatewayService) setCopilotMessagesHeaders(req *http.Request, c *gin.Context, body []byte) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-initiator", detectInitiatorMessages(body))
	if detectVisionMessages(body) {
		req.Header.Set("Copilot-Vision-Request", "true")
	}

	// Anthropic-Beta: forward from client, filtering out claude-code beta
	if beta := c.Request.Header.Get("Anthropic-Beta"); beta != "" {
		req.Header.Set("Anthropic-Beta", filterAnthropicBeta(beta))
	} else if gjson.GetBytes(body, "thinking.budget_tokens").Exists() {
		// Body requests thinking but client didn't send Anthropic-Beta — add it
		req.Header.Set("Anthropic-Beta", "interleaved-thinking-2025-05-14")
	}

	// Anthropic-Version: forward as-is
	if version := c.Request.Header.Get("Anthropic-Version"); version != "" {
		req.Header.Set("Anthropic-Version", version)
	}
}

// Forward sends a chat/completions request to GitHub Copilot.
func (s *CopilotGatewayService) Forward(ctx context.Context, c *gin.Context, account *Account, body []byte) (*CopilotForwardResult, error) {
	return s.forward(ctx, c, account, body, copilotForwardConfig{
		upstreamURL:  copilotUpstreamURL,
		extractUsage: chatCompletionsUsage,
		detectInit:   detectInitiator,
		detectVision: detectVision,
	})
}

// ForwardResponses sends a Responses API request to GitHub Copilot.
func (s *CopilotGatewayService) ForwardResponses(ctx context.Context, c *gin.Context, account *Account, body []byte) (*CopilotForwardResult, error) {
	// Ensure "reasoning.encrypted_content" is in the include array so upstream
	// returns plaintext reasoning instead of encrypted content.
	body = ensureResponsesInclude(body)
	return s.forward(ctx, c, account, body, copilotForwardConfig{
		upstreamURL:  copilotUpstreamResponsesURL,
		extractUsage: responsesUsage,
		detectInit:   detectInitiatorResponses,
		detectVision: detectVisionResponses,
	})
}

// ForwardResponsesRaw sends a Responses API request to GitHub Copilot and
// returns the raw *http.Response without processing the body. The caller is
// responsible for closing the response body and handling the response stream.
//
// This method applies model mapping, token auth, required headers, and the
// "reasoning.encrypted_content" include injection, but delegates response
// processing to the caller (e.g., for Stream ID synchronization via
// ProcessResponsesStream).
//
// Returns:
//   - *http.Response: the raw upstream response (caller must close Body)
//   - string: the original (pre-mapping) model name
//   - error: any error building or executing the upstream request
func (s *CopilotGatewayService) ForwardResponsesRaw(ctx context.Context, account *Account, body []byte) (*http.Response, string, error) {
	// Ensure "reasoning.encrypted_content" is in the include array
	body = ensureResponsesInclude(body)

	model := gjson.GetBytes(body, "model").String()
	mappedModel := account.GetMappedModel(model)
	logger.L().Debug("copilot model mapping applied (ForwardResponsesRaw)",
		zap.Int64("account_id", account.ID),
		zap.String("requested_model", model),
		zap.String("mapped_model", mappedModel),
		zap.Bool("was_mapped", mappedModel != model),
	)
	if mappedModel != model {
		var err error
		body, err = sjson.SetBytes(body, "model", mappedModel)
		if err != nil {
			return nil, "", fmt.Errorf("replace model in body: %w", err)
		}
	}

	token, err := s.copilotTokenProvider.GetAccessToken(ctx, account)
	if err != nil {
		return nil, "", fmt.Errorf("get copilot access token: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, copilotUpstreamResponsesURL, bytes.NewReader(body))
	if err != nil {
		return nil, "", fmt.Errorf("create upstream request: %w", err)
	}
	s.setCopilotBaseHeaders(req, token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-initiator", detectInitiatorResponses(body))
	if detectVisionResponses(body) {
		req.Header.Set("Copilot-Vision-Request", "true")
	}

	proxyURL := ""
	if account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		return nil, "", fmt.Errorf("copilot upstream request: %w", err)
	}

	return resp, model, nil
}

// convertedResponseHandler handles the upstream response after a format-converting forward.
type convertedResponseHandler struct {
	handleStream    func(c *gin.Context, resp *http.Response, model string, start time.Time) (*CopilotForwardResult, error)
	handleNonStream func(c *gin.Context, resp *http.Response, model string, start time.Time) (*CopilotForwardResult, error)
}

// forwardConverted is the shared pipeline for format-converting forwards.
// It takes an already-converted request body, sends it upstream, and dispatches
// the response to the appropriate handler.
func (s *CopilotGatewayService) forwardConverted(
	ctx context.Context, c *gin.Context, account *Account,
	convertedBody []byte, originalModel string, isStream bool,
	cfg copilotForwardConfig, handler convertedResponseHandler,
) (*CopilotForwardResult, error) {
	start := time.Now()

	// Apply model mapping
	model := gjson.GetBytes(convertedBody, "model").String()
	mappedModel := account.GetMappedModel(model)
	logger.L().Info("copilot model mapping applied",
		zap.Int64("account_id", account.ID),
		zap.String("requested_model", model),
		zap.String("mapped_model", mappedModel),
		zap.Bool("was_mapped", mappedModel != model),
	)
	if mappedModel != model {
		var err error
		convertedBody, err = sjson.SetBytes(convertedBody, "model", mappedModel)
		if err != nil {
			return nil, fmt.Errorf("replace model in body: %w", err)
		}
	}

	// Build upstream request
	token, err := s.copilotTokenProvider.GetAccessToken(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("get copilot access token: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.upstreamURL, bytes.NewReader(convertedBody))
	if err != nil {
		return nil, fmt.Errorf("create upstream request: %w", err)
	}
	s.setCopilotBaseHeaders(req, token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-initiator", cfg.detectInit(convertedBody))
	if cfg.detectVision(convertedBody) {
		req.Header.Set("Copilot-Vision-Request", "true")
	}

	proxyURL := ""
	if account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		return nil, fmt.Errorf("copilot upstream request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode >= 400 {
		return s.handleErrorResponse(c, resp, account, mappedModel, isStream, start)
	}

	if isStream {
		return handler.handleStream(c, resp, originalModel, start)
	}
	return handler.handleNonStream(c, resp, originalModel, start)
}

// ForwardMessages sends an Anthropic Messages request directly to GitHub
// Copilot's native /v1/messages endpoint without any format conversion.
// This preserves cache_control, thinking, metadata and other Anthropic-specific
// fields that would be lost in a Chat Completions round-trip, enabling prompt
// caching to work correctly.
func (s *CopilotGatewayService) ForwardMessages(ctx context.Context, c *gin.Context, account *Account, body []byte) (*CopilotForwardResult, error) {
	start := time.Now()

	model := gjson.GetBytes(body, "model").String()
	isStream := gjson.GetBytes(body, "stream").Bool()

	mappedModel := account.GetMappedModel(model)
	logger.L().Info("copilot messages model mapping applied",
		zap.Int64("account_id", account.ID),
		zap.String("requested_model", model),
		zap.String("mapped_model", mappedModel),
		zap.Bool("was_mapped", mappedModel != model),
	)
	if mappedModel != model {
		var err error
		body, err = sjson.SetBytes(body, "model", mappedModel)
		if err != nil {
			return nil, fmt.Errorf("replace model in body: %w", err)
		}
	}

	// Strip cache_control fields not supported by Copilot API (e.g. "scope")
	body = stripCacheControlScope(body)

	token, err := s.copilotTokenProvider.GetAccessToken(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("get copilot access token: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, copilotUpstreamMessagesURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create upstream request: %w", err)
	}
	s.setCopilotBaseHeaders(req, token)
	s.setCopilotMessagesHeaders(req, c, body)

	proxyURL := ""
	if account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		return nil, fmt.Errorf("copilot upstream request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode >= 400 {
		return s.handleErrorResponse(c, resp, account, mappedModel, isStream, start)
	}

	if isStream {
		return s.handleMessagesStreamResponse(c, resp, model, start)
	}
	return s.handleMessagesNonStreamResponse(c, resp, model, start)
}

// ForwardChatAsAnthropic accepts an Anthropic Messages request body, converts
// it to Chat Completions format, forwards to Copilot /chat/completions, and
// converts the response back to Anthropic Messages format.
func (s *CopilotGatewayService) ForwardChatAsAnthropic(ctx context.Context, c *gin.Context, account *Account, body []byte) (*CopilotForwardResult, error) {
	var anthropicReq apicompat.AnthropicRequest
	if err := json.Unmarshal(body, &anthropicReq); err != nil {
		return nil, fmt.Errorf("parse anthropic request: %w", err)
	}

	chatReq, err := apicompat.AnthropicToChat(&anthropicReq)
	if err != nil {
		return nil, fmt.Errorf("convert anthropic to chat: %w", err)
	}

	chatBody, err := json.Marshal(chatReq)
	if err != nil {
		return nil, fmt.Errorf("marshal chat request: %w", err)
	}

	return s.forwardConverted(ctx, c, account, chatBody, anthropicReq.Model, anthropicReq.Stream,
		copilotForwardConfig{
			upstreamURL:  copilotUpstreamURL,
			extractUsage: chatCompletionsUsage,
			detectInit:   detectInitiator,
			detectVision: detectVision,
		},
		convertedResponseHandler{
			handleStream:    s.handleAnthropicStreamResponse,
			handleNonStream: s.handleAnthropicNonStreamResponse,
		},
	)
}

// ForwardChatAsResponses accepts a Chat Completions request body, converts it
// to Responses API format, forwards to Copilot /responses, and converts the
// response back to Chat Completions format. Used for codex models that only
// support the /responses endpoint.
func (s *CopilotGatewayService) ForwardChatAsResponses(ctx context.Context, c *gin.Context, account *Account, body []byte) (*CopilotForwardResult, error) {
	var chatReq apicompat.ChatRequest
	if err := json.Unmarshal(body, &chatReq); err != nil {
		return nil, fmt.Errorf("parse chat request: %w", err)
	}

	responsesReq, err := apicompat.ChatToResponses(&chatReq)
	if err != nil {
		return nil, fmt.Errorf("convert chat to responses: %w", err)
	}

	responsesBody, err := json.Marshal(responsesReq)
	if err != nil {
		return nil, fmt.Errorf("marshal responses request: %w", err)
	}

	return s.forwardConverted(ctx, c, account, responsesBody, chatReq.Model, chatReq.Stream,
		copilotForwardConfig{
			upstreamURL:  copilotUpstreamResponsesURL,
			extractUsage: responsesUsage,
			detectInit:   detectInitiatorResponses,
			detectVision: detectVisionResponses,
		},
		convertedResponseHandler{
			handleStream:    s.handleResponsesToChatStream,
			handleNonStream: s.handleResponsesToChatNonStream,
		},
	)
}

// forward is the shared implementation for both endpoints.
func (s *CopilotGatewayService) forward(ctx context.Context, c *gin.Context, account *Account, body []byte, cfg copilotForwardConfig) (*CopilotForwardResult, error) {
	start := time.Now()

	model := gjson.GetBytes(body, "model").String()
	isStream := gjson.GetBytes(body, "stream").Bool()

	mappedModel := account.GetMappedModel(model)
	logger.L().Info("copilot model mapping applied",
		zap.Int64("account_id", account.ID),
		zap.String("requested_model", model),
		zap.String("mapped_model", mappedModel),
		zap.Bool("was_mapped", mappedModel != model),
	)
	if mappedModel != model {
		var err error
		body, err = sjson.SetBytes(body, "model", mappedModel)
		if err != nil {
			return nil, fmt.Errorf("replace model in body: %w", err)
		}
	}

	token, err := s.copilotTokenProvider.GetAccessToken(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("get copilot access token: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.upstreamURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create upstream request: %w", err)
	}
	s.setCopilotBaseHeaders(req, token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-initiator", cfg.detectInit(body))
	if cfg.detectVision(body) {
		req.Header.Set("Copilot-Vision-Request", "true")
	}

	proxyURL := ""
	if account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		return nil, fmt.Errorf("copilot upstream request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode >= 400 {
		return s.handleErrorResponse(c, resp, account, mappedModel, isStream, start)
	}

	if isStream {
		return s.handleStreamResponse(c, resp, mappedModel, start, cfg.extractUsage)
	}
	return s.handleNonStreamResponse(c, resp, mappedModel, start, cfg.extractUsage)
}

// FetchModelsFromUpstream fetches the model list from GitHub Copilot upstream
// using any available schedulable copilot account.
func (s *CopilotGatewayService) FetchModelsFromUpstream(ctx context.Context) ([]byte, error) {
	accounts, err := s.accountRepo.ListSchedulableByPlatform(ctx, PlatformCopilot)
	if err != nil {
		return nil, fmt.Errorf("list copilot accounts: %w", err)
	}
	if len(accounts) == 0 {
		return nil, fmt.Errorf("no schedulable copilot accounts")
	}
	return s.FetchModels(ctx, &accounts[0])
}

// FetchModels fetches the model list from GitHub Copilot upstream.
// GET https://api.githubcopilot.com/models with Copilot-standard headers.
func (s *CopilotGatewayService) FetchModels(ctx context.Context, account *Account) ([]byte, error) {
	token, err := s.copilotTokenProvider.GetAccessToken(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("get copilot access token: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, copilotUpstreamModelsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create models request: %w", err)
	}
	s.setCopilotBaseHeaders(req, token)
	req.Header.Set("x-initiator", "agent")

	proxyURL := ""
	if account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		return nil, fmt.Errorf("copilot models request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, fmt.Errorf("read models response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("copilot models returned %d: %s", resp.StatusCode, truncate(string(body), 512))
	}
	return body, nil
}
