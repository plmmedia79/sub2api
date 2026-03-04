package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// submitUsageRecordTask submits an async task to the worker pool or executes
// it inline with a timeout if no pool is available.
func (h *ResponsesHandler) submitUsageRecordTask(task service.UsageRecordTask) {
	if task == nil {
		return
	}
	if h.usageRecordWorkerPool != nil {
		h.usageRecordWorkerPool.Submit(task)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	task(ctx)
}

// handleConcurrencyError writes a rate limit error response for concurrency failures.
func (h *ResponsesHandler) handleConcurrencyError(c *gin.Context, err error, slotType string, streamStarted bool) {
	h.handleStreamingAwareError(c, http.StatusTooManyRequests, "rate_limit_error",
		fmt.Sprintf("Concurrency limit exceeded for %s, please retry later", slotType), streamStarted)
}

// handleFailoverExhausted writes the appropriate error after all upstream failovers
// have been exhausted, applying passthrough rules if configured.
func (h *ResponsesHandler) handleFailoverExhausted(c *gin.Context, failoverErr *service.UpstreamFailoverError, streamStarted bool) {
	if status, errType, errMsg, matched := applyFailoverPassthroughRule(c, h.errorPassthroughService, "copilot", failoverErr); matched {
		h.handleStreamingAwareError(c, status, errType, errMsg, streamStarted)
		return
	}
	status, errType, errMsg := mapCopilotUpstreamError(failoverErr.StatusCode)
	h.handleStreamingAwareError(c, status, errType, errMsg, streamStarted)
}

// handleStreamingAwareError writes an error as an SSE event if streaming has
// already started, otherwise writes a regular JSON error response.
func (h *ResponsesHandler) handleStreamingAwareError(c *gin.Context, status int, errType, message string, streamStarted bool) {
	if streamStarted {
		if flusher, ok := c.Writer.(http.Flusher); ok {
			errorData := map[string]any{
				"error": map[string]string{
					"type":    errType,
					"message": message,
				},
			}
			jsonBytes, err := json.Marshal(errorData)
			if err != nil {
				_ = c.Error(err)
				return
			}
			errorEvent := fmt.Sprintf("event: error\ndata: %s\n\n", string(jsonBytes))
			if _, err := fmt.Fprint(c.Writer, errorEvent); err != nil {
				_ = c.Error(err)
			}
			flusher.Flush()
		}
		return
	}
	h.errorResponse(c, status, errType, message)
}

// ensureForwardErrorResponse writes a fallback error response if the writer
// has not already been written to. Returns true if a response was written.
func (h *ResponsesHandler) ensureForwardErrorResponse(c *gin.Context, streamStarted bool) bool {
	if c == nil || c.Writer == nil || c.Writer.Written() {
		return false
	}
	h.handleStreamingAwareError(c, http.StatusBadGateway, "upstream_error", "Upstream request failed", streamStarted)
	return true
}

// errorResponse writes a standard JSON error response.
func (h *ResponsesHandler) errorResponse(c *gin.Context, status int, errType, message string) {
	c.JSON(status, gin.H{
		"error": gin.H{
			"type":    errType,
			"message": message,
		},
	})
}
