package handler

import (
	"context"
	"net/http"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// acquireUserConcurrencySlot acquires the user-level concurrency slot, handling
// both fast-path (immediate) and slow-path (wait queue) scenarios.
// Returns the release function or an error; error response is written to c on failure.
func (h *ResponsesHandler) acquireUserConcurrencySlot(
	c *gin.Context,
	subject middleware2.AuthSubject,
	reqStream bool,
	streamStarted *bool,
	reqLog *zap.Logger,
) (releaseFunc func(), err error) {
	releaseFunc, userAcquired, err := h.concurrencyHelper.TryAcquireUserSlot(c.Request.Context(), subject.UserID, subject.Concurrency)
	if err != nil {
		reqLog.Warn("responses.user_slot_acquire_failed", zap.Error(err))
		h.handleConcurrencyError(c, err, "user", *streamStarted)
		return nil, err
	}

	waitCounted := false
	if !userAcquired {
		maxWait := service.CalculateMaxWait(subject.Concurrency)
		canWait, waitErr := h.concurrencyHelper.IncrementWaitCount(c.Request.Context(), subject.UserID, maxWait)
		if waitErr != nil {
			reqLog.Warn("responses.user_wait_counter_increment_failed", zap.Error(waitErr))
		} else if !canWait {
			reqLog.Info("responses.user_wait_queue_full", zap.Int("max_wait", maxWait))
			h.errorResponse(c, http.StatusTooManyRequests, "rate_limit_error", "Too many pending requests, please retry later")
			return nil, context.DeadlineExceeded // sentinel to signal handled error
		}
		if waitErr == nil && canWait {
			waitCounted = true
		}
		defer func() {
			if waitCounted {
				h.concurrencyHelper.DecrementWaitCount(c.Request.Context(), subject.UserID)
			}
		}()

		releaseFunc, err = h.concurrencyHelper.AcquireUserSlotWithWait(c, subject.UserID, subject.Concurrency, reqStream, streamStarted)
		if err != nil {
			reqLog.Warn("responses.user_slot_acquire_failed_after_wait", zap.Error(err))
			h.handleConcurrencyError(c, err, "user", *streamStarted)
			return nil, err
		}
	}

	if waitCounted {
		h.concurrencyHelper.DecrementWaitCount(c.Request.Context(), subject.UserID)
	}
	return releaseFunc, nil
}

// acquireAccountConcurrencySlot acquires the account-level concurrency slot from
// a gateway selection, handling both pre-acquired and wait-queue scenarios.
// Returns the release function or an error; error response is written to c on failure.
func (h *ResponsesHandler) acquireAccountConcurrencySlot(
	c *gin.Context,
	selection *service.AccountSelectionResult,
	reqStream bool,
	streamStarted *bool,
	reqLog *zap.Logger,
) (releaseFunc func(), err error) {
	if selection.Acquired {
		return selection.ReleaseFunc, nil
	}
	account := selection.Account
	if selection.WaitPlan == nil {
		h.handleStreamingAwareError(c, http.StatusServiceUnavailable, "api_error", "No available accounts", *streamStarted)
		return nil, context.DeadlineExceeded // sentinel
	}

	fastReleaseFunc, fastAcquired, err := h.concurrencyHelper.TryAcquireAccountSlot(
		c.Request.Context(),
		account.ID,
		selection.WaitPlan.MaxConcurrency,
	)
	if err != nil {
		reqLog.Warn("responses.account_slot_quick_acquire_failed", zap.Int64("account_id", account.ID), zap.Error(err))
		h.handleConcurrencyError(c, err, "account", *streamStarted)
		return nil, err
	}
	if fastAcquired {
		return fastReleaseFunc, nil
	}

	// Slow path: wait queue
	accountWaitCounted := false
	canWait, err := h.concurrencyHelper.IncrementAccountWaitCount(c.Request.Context(), account.ID, selection.WaitPlan.MaxWaiting)
	if err != nil {
		reqLog.Warn("responses.account_wait_counter_increment_failed", zap.Int64("account_id", account.ID), zap.Error(err))
	} else if !canWait {
		reqLog.Info("responses.account_wait_queue_full",
			zap.Int64("account_id", account.ID),
			zap.Int("max_waiting", selection.WaitPlan.MaxWaiting),
		)
		h.handleStreamingAwareError(c, http.StatusTooManyRequests, "rate_limit_error", "Too many pending requests, please retry later", *streamStarted)
		return nil, context.DeadlineExceeded // sentinel
	}
	if err == nil && canWait {
		accountWaitCounted = true
	}
	releaseWait := func() {
		if accountWaitCounted {
			h.concurrencyHelper.DecrementAccountWaitCount(c.Request.Context(), account.ID)
			accountWaitCounted = false
		}
	}

	releaseFunc, err = h.concurrencyHelper.AcquireAccountSlotWithWaitTimeout(
		c,
		account.ID,
		selection.WaitPlan.MaxConcurrency,
		selection.WaitPlan.Timeout,
		reqStream,
		streamStarted,
	)
	if err != nil {
		reqLog.Warn("responses.account_slot_acquire_failed", zap.Int64("account_id", account.ID), zap.Error(err))
		releaseWait()
		h.handleConcurrencyError(c, err, "account", *streamStarted)
		return nil, err
	}
	releaseWait()
	return releaseFunc, nil
}
