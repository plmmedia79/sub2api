package handler

import (
	"fmt"
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// forwardResponses sends the request to upstream and handles the response.
// For streaming, it uses ProcessResponsesStream for Stream ID synchronization.
// For non-streaming, it delegates entirely to the copilot gateway service.
func (h *ResponsesHandler) forwardResponses(c *gin.Context, account *service.Account, body []byte, isStream bool, start time.Time) (*service.CopilotForwardResult, error) {
	if !isStream {
		// Non-streaming: delegate entirely to the service which handles model
		// mapping, auth, upstream request, and response writing.
		return h.copilotGatewayService.ForwardResponses(c.Request.Context(), c, account, body)
	}

	// Streaming: get the raw HTTP response so we can process it with
	// ProcessResponsesStream for Stream ID synchronization.
	resp, model, err := h.copilotGatewayService.ForwardResponsesRaw(c.Request.Context(), account, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck

	// Handle upstream errors via the service's error handling pipeline which
	// checks passthrough rules and determines failover.
	if resp.StatusCode >= 400 {
		return h.copilotGatewayService.HandleResponsesError(c, resp, account, model, true, start)
	}

	return h.processResponsesStream(c, resp, model, start)
}

// processResponsesStream reads SSE events from the upstream response, applies
// Stream ID synchronization via ProcessResponsesStream, and forwards the
// processed events to the client.
//
// ProcessResponsesStream internally creates a StreamIdTracker that:
//   - Records canonical item IDs from response.output_item.added events
//   - Replaces mismatched IDs in response.output_item.done events
//   - Applies canonical IDs to item_id fields in delta events
//
// This fixes the known GitHub Copilot bug where added/done events return
// different item IDs, which breaks @ai-sdk/openai clients.
func (h *ResponsesHandler) processResponsesStream(c *gin.Context, resp *http.Response, model string, start time.Time) (*service.CopilotForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	// Set SSE response headers
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	if h.cfg != nil {
		responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, h.responseHeaderFilter)
	}
	c.Writer.WriteHeader(http.StatusOK)

	var firstTokenMs *int
	firstEvent := true

	// Process the upstream stream with Stream ID synchronization.
	usage, err := apicompat.ProcessResponsesStream(resp.Body, func(eventType string, data string) error {
		if firstEvent && eventType != "" {
			firstEvent = false
			ms := int(time.Since(start).Milliseconds())
			firstTokenMs = &ms
		}

		// Write the processed (ID-fixed) event to the client
		if eventType != "" {
			fmt.Fprint(c.Writer, apicompat.FormatSSEEvent(eventType, data))
		} else if data != "" {
			// Comments or non-typed events
			fmt.Fprint(c.Writer, apicompat.FormatSSEData(data))
		}
		c.Writer.Flush()
		return nil
	})

	if err != nil {
		logger.L().Warn("responses.stream_processing_error",
			zap.Error(err),
			zap.String("request_id", requestID),
		)
	}

	// Send [DONE] sentinel
	fmt.Fprint(c.Writer, apicompat.FormatSSEDone())
	c.Writer.Flush()

	// Build usage result
	var copilotUsage service.CopilotUsage
	if usage != nil {
		copilotUsage = service.CopilotUsage{
			PromptTokens:     usage.InputTokens,
			CompletionTokens: usage.OutputTokens,
			TotalTokens:      usage.TotalTokens,
		}
	}

	logger.L().Debug("responses.stream_completed",
		zap.String("request_id", requestID),
		zap.Int("input_tokens", copilotUsage.PromptTokens),
		zap.Int("output_tokens", copilotUsage.CompletionTokens),
	)

	return &service.CopilotForwardResult{
		RequestID:    requestID,
		Usage:        copilotUsage,
		Model:        model,
		Stream:       true,
		Duration:     time.Since(start),
		FirstTokenMs: firstTokenMs,
	}, nil
}
