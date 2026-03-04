package handler

import (
	"context"
	"errors"
	"net/http"
	"time"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// runAccountLoop executes the account scheduling loop with failover.
// It selects accounts, acquires account slots, forwards requests, and records usage.
func (h *ResponsesHandler) runAccountLoop(
	c *gin.Context,
	apiKey *service.APIKey,
	subscription *service.UserSubscription,
	subject middleware2.AuthSubject,
	body []byte,
	reqModel string,
	reqStream bool,
	streamStarted *bool,
	reqLog *zap.Logger,
	routingStart time.Time,
) {
	maxAccountSwitches := h.maxAccountSwitches
	switchCount := 0
	failedAccountIDs := make(map[int64]struct{})
	var lastFailoverErr *service.UpstreamFailoverError

	for {
		reqLog.Debug("responses.account_selecting", zap.Int("excluded_account_count", len(failedAccountIDs)))
		selection, err := h.gatewayService.SelectAccountWithLoadAwareness(c.Request.Context(), apiKey.GroupID, "", reqModel, failedAccountIDs, "")
		if err != nil {
			reqLog.Warn("responses.account_select_failed",
				zap.Error(err),
				zap.Int("excluded_account_count", len(failedAccountIDs)),
			)
			if len(failedAccountIDs) == 0 {
				h.handleStreamingAwareError(c, http.StatusServiceUnavailable, "api_error", "Service temporarily unavailable", *streamStarted)
				return
			}
			if lastFailoverErr != nil {
				h.handleFailoverExhausted(c, lastFailoverErr, *streamStarted)
			} else {
				h.handleStreamingAwareError(c, http.StatusBadGateway, "upstream_error", "All upstream accounts exhausted", *streamStarted)
			}
			return
		}
		account := selection.Account
		reqLog.Debug("responses.account_selected", zap.Int64("account_id", account.ID), zap.String("account_name", account.Name))
		setOpsSelectedAccount(c, account.ID, account.Platform)

		// Acquire account concurrency slot
		accountReleaseFunc, err := h.acquireAccountConcurrencySlot(c, selection, reqStream, streamStarted, reqLog)
		if err != nil {
			return // error already written
		}
		accountReleaseFunc = wrapReleaseOnDone(c.Request.Context(), accountReleaseFunc)

		// Forward request to upstream
		service.SetOpsLatencyMs(c, service.OpsRoutingLatencyMsKey, time.Since(routingStart).Milliseconds())
		forwardStart := time.Now()
		result, err := h.forwardResponses(c, account, body, reqStream, forwardStart)
		forwardDurationMs := time.Since(forwardStart).Milliseconds()
		if accountReleaseFunc != nil {
			accountReleaseFunc()
		}
		service.SetOpsLatencyMs(c, service.OpsResponseLatencyMsKey, forwardDurationMs)
		if err == nil && result != nil && result.FirstTokenMs != nil {
			service.SetOpsLatencyMs(c, service.OpsTimeToFirstTokenMsKey, int64(*result.FirstTokenMs))
		}
		if err != nil {
			var failoverErr *service.UpstreamFailoverError
			if errors.As(err, &failoverErr) {
				failedAccountIDs[account.ID] = struct{}{}
				lastFailoverErr = failoverErr
				if switchCount >= maxAccountSwitches {
					h.handleFailoverExhausted(c, failoverErr, *streamStarted)
					return
				}
				switchCount++
				reqLog.Warn("responses.upstream_failover_switching",
					zap.Int64("account_id", account.ID),
					zap.Int("upstream_status", failoverErr.StatusCode),
					zap.Int("switch_count", switchCount),
					zap.Int("max_switches", maxAccountSwitches),
				)
				continue
			}
			wroteFallback := h.ensureForwardErrorResponse(c, *streamStarted)
			reqLog.Error("responses.forward_failed",
				zap.Int64("account_id", account.ID),
				zap.Bool("fallback_error_response_written", wroteFallback),
				zap.Error(err),
			)
			return
		}

		// Record usage asynchronously
		userAgent := c.GetHeader("User-Agent")
		clientIP := ip.GetClientIP(c)

		h.submitUsageRecordTask(func(ctx context.Context) {
			forwardResult := copilotToForwardResult(result)
			if err := h.gatewayService.RecordUsage(ctx, &service.RecordUsageInput{
				Result:        forwardResult,
				APIKey:        apiKey,
				User:          apiKey.User,
				Account:       account,
				Subscription:  subscription,
				UserAgent:     userAgent,
				IPAddress:     clientIP,
				APIKeyService: h.apiKeyService,
			}); err != nil {
				logger.L().With(
					zap.String("component", "handler.responses"),
					zap.Int64("user_id", subject.UserID),
					zap.Int64("api_key_id", apiKey.ID),
					zap.Any("group_id", apiKey.GroupID),
					zap.String("model", reqModel),
					zap.Int64("account_id", account.ID),
				).Error("responses.record_usage_failed", zap.Error(err))
			}
		})

		reqLog.Info("responses.request_completed",
			zap.Int64("account_id", account.ID),
			zap.String("result_model", result.Model),
			zap.Int("prompt_tokens", result.Usage.PromptTokens),
			zap.Int("completion_tokens", result.Usage.CompletionTokens),
			zap.Duration("duration", result.Duration),
		)
		return
	}
}
