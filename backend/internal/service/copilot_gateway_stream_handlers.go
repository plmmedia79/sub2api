package service

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

// streamKeepaliveInterval returns the configured keepalive interval for SSE streams.
// Returns 0 if disabled. Default is 10s (configured via gateway.stream_keepalive_interval).
func (s *CopilotGatewayService) streamKeepaliveInterval() time.Duration {
	if s.cfg != nil && s.cfg.Gateway.StreamKeepaliveInterval > 0 {
		return time.Duration(s.cfg.Gateway.StreamKeepaliveInterval) * time.Second
	}
	return 0
}

// newSSEScanner creates a bufio.Scanner configured for SSE streaming responses.
// Buffer starts at 64KB and grows up to 1MB per line.
func newSSEScanner(r io.Reader) *bufio.Scanner {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	return s
}

// shouldFailoverCopilot returns true for status codes that should trigger account failover.
func shouldFailoverCopilot(statusCode int) bool {
	switch statusCode {
	case 401, 402, 403, 429:
		return true
	default:
		return statusCode >= 500
	}
}

// HandleResponsesError processes an error response from the upstream /responses
// endpoint. It checks error passthrough rules, determines whether to failover,
// and writes the appropriate error response to the client. This is the exported
// counterpart of handleErrorResponse, used by the ResponsesHandler when handling
// raw upstream responses.
func (s *CopilotGatewayService) HandleResponsesError(c *gin.Context, resp *http.Response, account *Account, model string, stream bool, start time.Time) (*CopilotForwardResult, error) {
	return s.handleErrorResponse(c, resp, account, model, stream, start)
}

// handleErrorResponse checks error passthrough rules, then either returns
// UpstreamFailoverError for retriable codes or writes the error to the client.
func (s *CopilotGatewayService) handleErrorResponse(c *gin.Context, resp *http.Response, account *Account, model string, stream bool, start time.Time) (*CopilotForwardResult, error) {
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, fmt.Errorf("read error response: %w", err)
	}

	requestID := resp.Header.Get("x-request-id")
	logger.L().Warn("copilot upstream error",
		zap.Int("status", resp.StatusCode),
		zap.String("model", model),
		zap.String("request_id", requestID),
		zap.Int64("account_id", account.ID),
		zap.String("body", truncate(string(respBody), 200)),
	)

	// Check error passthrough rules — if matched, write response and return non-failover error
	if status, errType, errMsg, matched := applyErrorPassthroughRule(
		c, PlatformCopilot, resp.StatusCode, respBody,
		http.StatusBadGateway, "upstream_error", "Upstream request failed",
	); matched {
		c.JSON(status, gin.H{
			"error": gin.H{"type": errType, "message": errMsg},
		})
		return nil, fmt.Errorf("upstream error: %d (passthrough rule matched)", resp.StatusCode)
	}

	// Invalidate cached session token on 401 so next attempt re-exchanges
	if resp.StatusCode == http.StatusUnauthorized {
		s.copilotTokenProvider.InvalidateCache(account.ID)
	}

	// Retriable codes → failover
	if shouldFailoverCopilot(resp.StatusCode) {
		return nil, &UpstreamFailoverError{
			StatusCode:   resp.StatusCode,
			ResponseBody: respBody,
		}
	}

	// Non-retriable error → write directly to client
	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), respBody)

	return &CopilotForwardResult{
		RequestID: requestID,
		Model:     model,
		Stream:    stream,
		Duration:  time.Since(start),
	}, nil
}

// handleStreamResponse reads SSE events from upstream and forwards them to the client,
// extracting usage via the provided extractor.
func (s *CopilotGatewayService) handleStreamResponse(c *gin.Context, resp *http.Response, model string, start time.Time, extract usageExtractor) (*CopilotForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no")
	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	c.Writer.WriteHeader(http.StatusOK)

	// Keepalive ticker to prevent proxy idle timeout (Cloudflare 100s)
	keepaliveInterval := s.streamKeepaliveInterval()
	var keepaliveTicker *time.Ticker
	if keepaliveInterval > 0 {
		keepaliveTicker = time.NewTicker(keepaliveInterval)
		defer keepaliveTicker.Stop()
	}
	lastDataAt := time.Now()

	var usage CopilotUsage
	var firstTokenMs *int
	firstChunk := true

	scanner := newSSEScanner(resp.Body)

	// Use a channel to read lines so we can select on keepalive
	lineCh := make(chan string)
	errCh := make(chan error, 1)
	go func() {
		for scanner.Scan() {
			lineCh <- scanner.Text()
		}
		errCh <- scanner.Err()
		close(lineCh)
	}()

	scanDone := false
	for !scanDone {
		var keepaliveCh <-chan time.Time
		if keepaliveTicker != nil {
			keepaliveCh = keepaliveTicker.C
		}
		select {
		case line, ok := <-lineCh:
			if !ok {
				scanDone = true
				break
			}
			lastDataAt = time.Now()
			if keepaliveTicker != nil {
				keepaliveTicker.Reset(keepaliveInterval)
			}

			if firstChunk && strings.HasPrefix(line, "data: ") && line != "data: [DONE]" {
				firstChunk = false
				ms := int(time.Since(start).Milliseconds())
				firstTokenMs = &ms
			}

			if strings.HasPrefix(line, "data: ") && line != "data: [DONE]" {
				payload := line[6:]
				// Try Chat Completions format (usage at top level)
				u := gjson.Get(payload, "usage")
				if u.Exists() {
					usage = extract(u)
				} else {
					// Try Responses API format (usage nested in response.usage)
					u = gjson.Get(payload, "response.usage")
					if u.Exists() {
						// Convert Responses API format to Chat Completions format
						pt := int(u.Get("input_tokens").Int())
						ct := int(u.Get("output_tokens").Int())
						usage = CopilotUsage{PromptTokens: pt, CompletionTokens: ct, TotalTokens: pt + ct}
					}
				}
			}

			fmt.Fprintf(c.Writer, "%s\n", line)
			c.Writer.Flush()
		case <-keepaliveCh:
			if time.Since(lastDataAt) >= keepaliveInterval {
				fmt.Fprintf(c.Writer, ": keepalive\n\n")
				c.Writer.Flush()
			}
		}
	}

	var scanErr error
	select {
	case scanErr = <-errCh:
	default:
	}

	if scanErr != nil {
		logger.L().Warn("copilot stream read error",
			zap.Error(scanErr),
			zap.String("request_id", requestID),
		)
	}

	// Debug: log final usage before returning
	logger.L().Debug("copilot chat completions stream: final usage",
		zap.String("request_id", requestID),
		zap.Int("prompt_tokens", usage.PromptTokens),
		zap.Int("completion_tokens", usage.CompletionTokens),
		zap.Int("total_tokens", usage.TotalTokens),
		zap.Bool("is_zero_usage", usage.PromptTokens == 0 && usage.CompletionTokens == 0),
	)

	return &CopilotForwardResult{
		RequestID:    requestID,
		Usage:        usage,
		Model:        model,
		Stream:       true,
		Duration:     time.Since(start),
		FirstTokenMs: firstTokenMs,
	}, nil
}

// handleNonStreamResponse reads the full upstream response, extracts usage, and writes it back.
func (s *CopilotGatewayService) handleNonStreamResponse(c *gin.Context, resp *http.Response, model string, start time.Time, extract usageExtractor) (*CopilotForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, fmt.Errorf("read upstream response: %w", err)
	}

	var usage CopilotUsage
	u := gjson.GetBytes(respBody, "usage")
	if u.Exists() {
		usage = extract(u)
		logger.L().Debug("copilot non-stream: usage extracted from response",
			zap.String("request_id", requestID),
			zap.Int("prompt_tokens", usage.PromptTokens),
			zap.Int("completion_tokens", usage.CompletionTokens),
			zap.Int("total_tokens", usage.TotalTokens),
		)
	} else {
		logger.L().Debug("copilot non-stream: no usage field in response",
			zap.String("request_id", requestID),
			zap.String("body_preview", truncate(string(respBody), 512)),
		)
	}

	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), respBody)

	return &CopilotForwardResult{
		RequestID: requestID,
		Usage:     usage,
		Model:     model,
		Stream:    false,
		Duration:  time.Since(start),
	}, nil
}

// handleMessagesNonStreamResponse reads the full Anthropic Messages response
// from upstream, extracts usage, and writes it back to the client unchanged.
func (s *CopilotGatewayService) handleMessagesNonStreamResponse(c *gin.Context, resp *http.Response, model string, start time.Time) (*CopilotForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, fmt.Errorf("read upstream response: %w", err)
	}

	usage := extractAnthropicUsage(respBody)

	logger.L().Debug("copilot messages non-stream: usage extracted",
		zap.String("request_id", requestID),
		zap.Int("prompt_tokens", usage.PromptTokens),
		zap.Int("completion_tokens", usage.CompletionTokens),
		zap.Int("total_tokens", usage.TotalTokens),
	)

	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), respBody)

	return &CopilotForwardResult{
		RequestID: requestID,
		Usage:     usage,
		Model:     model,
		Stream:    false,
		Duration:  time.Since(start),
	}, nil
}

// handleMessagesStreamResponse forwards Anthropic SSE events from upstream to
// the client unchanged, extracting usage from message_start and message_delta
// events along the way.
func (s *CopilotGatewayService) handleMessagesStreamResponse(c *gin.Context, resp *http.Response, model string, start time.Time) (*CopilotForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no")
	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	c.Writer.WriteHeader(http.StatusOK)

	// Keepalive ticker to prevent proxy idle timeout (Cloudflare 100s)
	keepaliveInterval := s.streamKeepaliveInterval()
	var keepaliveTicker *time.Ticker
	if keepaliveInterval > 0 {
		keepaliveTicker = time.NewTicker(keepaliveInterval)
		defer keepaliveTicker.Stop()
	}
	lastDataAt := time.Now()

	var usage CopilotUsage
	var firstTokenMs *int
	firstChunk := true

	scanner := newSSEScanner(resp.Body)

	// Use a channel to read lines so we can select on keepalive
	lineCh := make(chan string)
	errCh := make(chan error, 1)
	go func() {
		for scanner.Scan() {
			lineCh <- scanner.Text()
		}
		errCh <- scanner.Err()
		close(lineCh)
	}()

	scanDone := false
	for !scanDone {
		var keepaliveCh <-chan time.Time
		if keepaliveTicker != nil {
			keepaliveCh = keepaliveTicker.C
		}
		select {
		case line, ok := <-lineCh:
			if !ok {
				scanDone = true
				break
			}
			lastDataAt = time.Now()
			if keepaliveTicker != nil {
				keepaliveTicker.Reset(keepaliveInterval)
			}

			// Track first data line for TTFT
			if firstChunk && strings.HasPrefix(line, "data: ") {
				firstChunk = false
				ms := int(time.Since(start).Milliseconds())
				firstTokenMs = &ms
			}

			// Extract usage from Anthropic SSE events
			if strings.HasPrefix(line, "data: ") {
				payload := line[6:]
				eventType := gjson.Get(payload, "type").String()
				switch eventType {
				case "message_start":
					// message_start → message.usage contains input_tokens + cache fields
					u := gjson.Get(payload, "message.usage")
					if u.Exists() {
						inputTokens := int(u.Get("input_tokens").Int())
						cacheCreation := int(u.Get("cache_creation_input_tokens").Int())
						cacheRead := int(u.Get("cache_read_input_tokens").Int())
						usage.PromptTokens = inputTokens + cacheCreation + cacheRead
					}
				case "message_delta":
					// message_delta → usage.output_tokens
					u := gjson.Get(payload, "usage")
					if u.Exists() {
						usage.CompletionTokens = int(u.Get("output_tokens").Int())
					}
				}
			}

			// Transparently forward every line (event:, data:, empty lines)
			fmt.Fprintf(c.Writer, "%s\n", line)
			c.Writer.Flush()
		case <-keepaliveCh:
			if time.Since(lastDataAt) >= keepaliveInterval {
				fmt.Fprintf(c.Writer, ": keepalive\n\n")
				c.Writer.Flush()
			}
		}
	}

	var scanErr error
	select {
	case scanErr = <-errCh:
	default:
	}

	if scanErr != nil {
		logger.L().Warn("copilot messages stream read error",
			zap.Error(scanErr),
			zap.String("request_id", requestID),
		)
	}

	usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens

	if usage.PromptTokens == 0 && usage.CompletionTokens == 0 {
		logger.L().Warn("copilot messages stream: final usage is ZERO",
			zap.String("request_id", requestID),
			zap.String("model", model),
		)
	} else {
		logger.L().Info("copilot messages stream: final usage",
			zap.String("request_id", requestID),
			zap.String("model", model),
			zap.Int("prompt_tokens", usage.PromptTokens),
			zap.Int("completion_tokens", usage.CompletionTokens),
			zap.Int("total_tokens", usage.TotalTokens),
		)
	}

	return &CopilotForwardResult{
		RequestID:    requestID,
		Usage:        usage,
		Model:        model,
		Stream:       true,
		Duration:     time.Since(start),
		FirstTokenMs: firstTokenMs,
	}, nil
}

// handleAnthropicNonStreamResponse reads a Chat Completions response from
// upstream, converts it to Anthropic Messages format, and writes it to the client.
func (s *CopilotGatewayService) handleAnthropicNonStreamResponse(c *gin.Context, resp *http.Response, model string, start time.Time) (*CopilotForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, fmt.Errorf("read upstream response: %w", err)
	}

	var chatResp apicompat.ChatResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		return nil, fmt.Errorf("parse chat response: %w", err)
	}

	anthropicResp := apicompat.ChatToAnthropic(&chatResp, model)

	var usage CopilotUsage
	if chatResp.Usage != nil {
		usage = CopilotUsage{
			PromptTokens:     chatResp.Usage.PromptTokens,
			CompletionTokens: chatResp.Usage.CompletionTokens,
			TotalTokens:      chatResp.Usage.TotalTokens,
		}
	}

	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	c.JSON(http.StatusOK, anthropicResp)

	return &CopilotForwardResult{
		RequestID: requestID,
		Usage:     usage,
		Model:     model,
		Stream:    false,
		Duration:  time.Since(start),
	}, nil
}

// handleAnthropicStreamResponse reads Chat Completions SSE chunks from upstream,
// converts each to Anthropic SSE events, and writes them to the client.
func (s *CopilotGatewayService) handleAnthropicStreamResponse(c *gin.Context, resp *http.Response, model string, start time.Time) (*CopilotForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	c.Writer.WriteHeader(http.StatusOK)

	state := apicompat.NewChatToAnthropicStreamState()
	var usage CopilotUsage
	var firstTokenMs *int
	firstChunk := true

	scanner := newSSEScanner(resp.Body)

	for scanner.Scan() {
		line := scanner.Text()

		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		payload := line[6:]

		// Track first token timing
		if firstChunk {
			firstChunk = false
			ms := int(time.Since(start).Milliseconds())
			firstTokenMs = &ms
		}

		// Extract usage from chunk
		u := gjson.Get(payload, "usage")
		if u.Exists() {
			usage = chatCompletionsUsage(u)
		}

		// Parse the Chat chunk
		var chunk apicompat.ChatStreamChunk
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			logger.L().Warn("copilot anthropic stream: failed to parse chunk",
				zap.Error(err),
				zap.String("request_id", requestID),
			)
			continue
		}

		// Override model to return the original Anthropic model name
		chunk.Model = model

		// Convert to Anthropic events
		events := apicompat.ChatChunkToAnthropicEvents(&chunk, state)
		for _, evt := range events {
			sse, err := apicompat.ChatStreamEventToSSE(evt)
			if err != nil {
				logger.L().Warn("copilot anthropic stream: failed to marshal event",
					zap.Error(err),
					zap.String("request_id", requestID),
				)
				continue
			}
			fmt.Fprint(c.Writer, sse)
		}
		c.Writer.Flush()
	}

	if err := scanner.Err(); err != nil {
		logger.L().Warn("copilot anthropic stream read error",
			zap.Error(err),
			zap.String("request_id", requestID),
		)
	}

	// Ensure the Anthropic stream is properly terminated even if upstream
	// disconnected without sending a finish_reason chunk.
	if finalEvents := apicompat.FinalizeAnthropicStream(state); len(finalEvents) > 0 {
		for _, evt := range finalEvents {
			sse, err := apicompat.ChatStreamEventToSSE(evt)
			if err != nil {
				continue
			}
			fmt.Fprint(c.Writer, sse)
		}
		c.Writer.Flush()
	}

	// Debug: log final usage before returning
	logger.L().Debug("copilot anthropic stream: final usage",
		zap.String("request_id", requestID),
		zap.Int("prompt_tokens", usage.PromptTokens),
		zap.Int("completion_tokens", usage.CompletionTokens),
		zap.Int("total_tokens", usage.TotalTokens),
		zap.Bool("is_zero_usage", usage.PromptTokens == 0 && usage.CompletionTokens == 0),
	)

	return &CopilotForwardResult{
		RequestID:    requestID,
		Usage:        usage,
		Model:        model,
		Stream:       true,
		Duration:     time.Since(start),
		FirstTokenMs: firstTokenMs,
	}, nil
}

// handleResponsesToChatNonStream reads a Responses API response from upstream,
// converts it to Chat Completions format, and writes it to the client.
func (s *CopilotGatewayService) handleResponsesToChatNonStream(c *gin.Context, resp *http.Response, model string, start time.Time) (*CopilotForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, fmt.Errorf("read upstream response: %w", err)
	}

	var responsesResp apicompat.ResponsesResponse
	if err := json.Unmarshal(respBody, &responsesResp); err != nil {
		return nil, fmt.Errorf("parse responses response: %w", err)
	}

	chatResp := apicompat.ResponsesToChat(&responsesResp)
	chatResp.Model = model

	var usage CopilotUsage
	if responsesResp.Usage != nil {
		usage = CopilotUsage{
			PromptTokens:     responsesResp.Usage.InputTokens,
			CompletionTokens: responsesResp.Usage.OutputTokens,
			TotalTokens:      responsesResp.Usage.TotalTokens,
		}
	}

	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	c.JSON(http.StatusOK, chatResp)

	return &CopilotForwardResult{
		RequestID: requestID,
		Usage:     usage,
		Model:     model,
		Stream:    false,
		Duration:  time.Since(start),
	}, nil
}

// handleResponsesToChatStream reads Responses SSE events from upstream,
// converts each to Chat Completions SSE chunks, and writes them to the client.
func (s *CopilotGatewayService) handleResponsesToChatStream(c *gin.Context, resp *http.Response, model string, start time.Time) (*CopilotForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	c.Writer.WriteHeader(http.StatusOK)

	state := apicompat.NewResponsesToChatStreamState()
	var usage CopilotUsage
	var firstTokenMs *int
	firstChunk := true
	finishSent := false
	completionEventReceived := false // Track if we received response.completed/incomplete event

	scanner := newSSEScanner(resp.Body)

	logger.L().Info("copilot responses-to-chat stream: started",
		zap.String("request_id", requestID),
		zap.String("model", model),
	)

	for scanner.Scan() {
		line := scanner.Text()

		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		payload := line[6:]

		if firstChunk {
			firstChunk = false
			ms := int(time.Since(start).Milliseconds())
			firstTokenMs = &ms
		}

		// Parse the Responses SSE event
		var event apicompat.ResponsesStreamEvent
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			logger.L().Warn("copilot responses-to-chat stream: failed to parse event",
				zap.Error(err),
				zap.String("request_id", requestID),
				zap.String("payload_preview", truncate(string(payload), 256)),
			)
			continue
		}

		// Enhanced logging: Log all event types to diagnose missing usage
		if event.Type == "response.completed" || event.Type == "response.incomplete" {
			completionEventReceived = true
			hasUsage := event.Response != nil && event.Response.Usage != nil
			logger.L().Info("copilot responses-to-chat stream: completion event",
				zap.String("request_id", requestID),
				zap.String("event_type", event.Type),
				zap.Bool("has_response", event.Response != nil),
				zap.Bool("has_usage", hasUsage),
				zap.String("payload_preview", truncate(string(payload), 512)),
			)
			if hasUsage {
				logger.L().Info("copilot responses-to-chat stream: usage extracted",
					zap.String("request_id", requestID),
					zap.Int("input_tokens", event.Response.Usage.InputTokens),
					zap.Int("output_tokens", event.Response.Usage.OutputTokens),
					zap.Int("total_tokens", event.Response.Usage.TotalTokens),
				)
			} else {
				// CRITICAL: Log when completion event has no usage - this is the bug!
				logger.L().Warn("copilot responses-to-chat stream: completion event WITHOUT usage",
					zap.String("request_id", requestID),
					zap.String("event_type", event.Type),
				)
				logger.L().Debug("copilot responses-to-chat stream: completion event payload",
					zap.String("request_id", requestID),
					zap.String("payload", truncate(string(payload), 200)),
				)
			}
		} else if event.Type != "" {
			// Log other event types for diagnosis (at debug level to avoid spam)
			logger.L().Debug("copilot responses-to-chat stream: event received",
				zap.String("request_id", requestID),
				zap.String("event_type", event.Type),
			)
		}

		// Convert to Chat Completions chunks
		chunks := apicompat.ResponsesEventToChatChunks(&event, state)
		for _, chunk := range chunks {
			chunk.Model = model
			// Extract usage from the final chunk if present
			if chunk.Usage != nil {
				usage = CopilotUsage{
					PromptTokens:     chunk.Usage.PromptTokens,
					CompletionTokens: chunk.Usage.CompletionTokens,
					TotalTokens:      chunk.Usage.TotalTokens,
				}
			}
			// Track if a finish_reason was sent
			for _, ch := range chunk.Choices {
				if ch.FinishReason != nil {
					finishSent = true
				}
			}
			sse, err := apicompat.ResponsesStreamEventToSSE(chunk)
			if err != nil {
				logger.L().Warn("copilot responses-to-chat stream: failed to marshal chunk",
					zap.Error(err),
					zap.String("request_id", requestID),
				)
				continue
			}
			fmt.Fprint(c.Writer, sse)
		}
		c.Writer.Flush()
	}

	if err := scanner.Err(); err != nil {
		logger.L().Warn("copilot responses-to-chat stream read error",
			zap.Error(err),
			zap.String("request_id", requestID),
		)
	}

	// CRITICAL: Log if stream ended without completion event.
	// For Codex models, this is expected behavior (they don't send response.completed).
	// For other models, this indicates a potential upstream issue.
	if !completionEventReceived {
		isCodexModel := strings.Contains(strings.ToLower(model), "codex")
		if isCodexModel {
			// Codex models don't send response.completed, usage must be fetched from Usage API
			logger.L().Info("copilot responses-to-chat stream: ended without completion event (expected for Codex models)",
				zap.String("request_id", requestID),
				zap.String("model", model),
				zap.Bool("first_chunk_received", !firstChunk),
				zap.Bool("finish_reason_sent", finishSent),
				zap.Int("final_prompt_tokens", usage.PromptTokens),
				zap.Int("final_completion_tokens", usage.CompletionTokens),
				zap.String("note", "usage not available in stream, use Usage API"),
			)
		} else {
			logger.L().Error("copilot responses-to-chat stream: ended WITHOUT completion event",
				zap.String("request_id", requestID),
				zap.String("model", model),
				zap.Bool("first_chunk_received", !firstChunk),
				zap.Bool("finish_reason_sent", finishSent),
				zap.Int("final_prompt_tokens", usage.PromptTokens),
				zap.Int("final_completion_tokens", usage.CompletionTokens),
			)
		}
	}

	// If upstream disconnected without a finish_reason, synthesize one.
	if !finishSent && !firstChunk {
		fr := "stop"
		finalChunk := apicompat.ChatStreamChunk{
			ID:      state.ResponseID,
			Object:  "chat.completion.chunk",
			Created: state.Created,
			Model:   model,
			Choices: []apicompat.ChatStreamChoice{{
				Index:        0,
				FinishReason: &fr,
			}},
		}
		if sse, err := apicompat.ResponsesStreamEventToSSE(finalChunk); err == nil {
			fmt.Fprint(c.Writer, sse)
		}
	}

	// Send [DONE] sentinel
	fmt.Fprint(c.Writer, "data: [DONE]\n\n")
	c.Writer.Flush()

	// Log final usage (Info level for production visibility)
	if usage.PromptTokens == 0 && usage.CompletionTokens == 0 {
		logger.L().Warn("copilot responses-to-chat stream: final usage is ZERO",
			zap.String("request_id", requestID),
			zap.String("model", model),
			zap.Int("prompt_tokens", usage.PromptTokens),
			zap.Int("completion_tokens", usage.CompletionTokens),
			zap.Bool("completion_event_received", completionEventReceived),
			zap.Bool("finish_reason_sent", finishSent),
		)
	} else {
		logger.L().Info("copilot responses-to-chat stream: final usage",
			zap.String("request_id", requestID),
			zap.String("model", model),
			zap.Int("prompt_tokens", usage.PromptTokens),
			zap.Int("completion_tokens", usage.CompletionTokens),
			zap.Int("total_tokens", usage.TotalTokens),
		)
	}

	return &CopilotForwardResult{
		RequestID:    requestID,
		Usage:        usage,
		Model:        model,
		Stream:       true,
		Duration:     time.Since(start),
		FirstTokenMs: firstTokenMs,
	}, nil
}
