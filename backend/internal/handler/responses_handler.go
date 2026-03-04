package handler

import (
	"io"
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

// ResponsesHandler handles Copilot /responses endpoint requests with full
// Stream ID synchronization support. Unlike the basic passthrough in
// CopilotGatewayHandler.Responses, this handler uses ProcessResponsesStream
// to ensure consistent item IDs across the stream lifecycle, which is required
// by @ai-sdk/openai clients (e.g., OpenCode with Codex models).
type ResponsesHandler struct {
	gatewayService          *service.GatewayService
	copilotGatewayService   *service.CopilotGatewayService
	billingCacheService     *service.BillingCacheService
	apiKeyService           *service.APIKeyService
	usageRecordWorkerPool   *service.UsageRecordWorkerPool
	errorPassthroughService *service.ErrorPassthroughService
	concurrencyHelper       *ConcurrencyHelper
	maxAccountSwitches      int
	cfg                     *config.Config
	responseHeaderFilter    *responseheaders.CompiledHeaderFilter
}

// NewResponsesHandler creates a new ResponsesHandler.
func NewResponsesHandler(
	gatewayService *service.GatewayService,
	copilotGatewayService *service.CopilotGatewayService,
	concurrencyService *service.ConcurrencyService,
	billingCacheService *service.BillingCacheService,
	apiKeyService *service.APIKeyService,
	usageRecordWorkerPool *service.UsageRecordWorkerPool,
	errorPassthroughService *service.ErrorPassthroughService,
	cfg *config.Config,
) *ResponsesHandler {
	pingInterval := time.Duration(0)
	maxAccountSwitches := 3
	if cfg != nil {
		pingInterval = time.Duration(cfg.Concurrency.PingInterval) * time.Second
		if cfg.Gateway.MaxAccountSwitches > 0 {
			maxAccountSwitches = cfg.Gateway.MaxAccountSwitches
		}
	}
	var headerFilter *responseheaders.CompiledHeaderFilter
	if cfg != nil {
		headerFilter = responseheaders.CompileHeaderFilter(cfg.Security.ResponseHeaders)
	}
	return &ResponsesHandler{
		gatewayService:          gatewayService,
		copilotGatewayService:   copilotGatewayService,
		billingCacheService:     billingCacheService,
		apiKeyService:           apiKeyService,
		usageRecordWorkerPool:   usageRecordWorkerPool,
		errorPassthroughService: errorPassthroughService,
		concurrencyHelper:       NewConcurrencyHelper(concurrencyService, SSEPingFormatComment, pingInterval),
		maxAccountSwitches:      maxAccountSwitches,
		cfg:                     cfg,
		responseHeaderFilter:    headerFilter,
	}
}

// HandleResponses handles a Responses API request by forwarding to the upstream
// Copilot /responses endpoint with Stream ID synchronization.
//
// For streaming responses, it uses ProcessResponsesStream to apply ID tracking
// that fixes the known GitHub Copilot bug where added/done events return
// different item IDs. For non-streaming responses, it delegates entirely to
// the copilot gateway service.
//
// POST /copilot/v1/responses
func (h *ResponsesHandler) HandleResponses(c *gin.Context) {
	requestStart := time.Now()

	// Get apiKey and user from context (set by ApiKeyAuth middleware)
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok {
		h.errorResponse(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}

	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		h.errorResponse(c, http.StatusInternalServerError, "api_error", "User context not found")
		return
	}
	reqLog := requestLogger(
		c,
		"handler.responses",
		zap.Int64("user_id", subject.UserID),
		zap.Int64("api_key_id", apiKey.ID),
		zap.Any("group_id", apiKey.GroupID),
	)

	// Read and validate request body
	body, reqModel, reqStream, ok := h.readAndValidateBody(c)
	if !ok {
		return
	}
	reqLog = reqLog.With(zap.String("model", reqModel), zap.Bool("stream", reqStream))
	setOpsRequestContext(c, reqModel, reqStream, body)

	// Track if we've started streaming (for error handling)
	streamStarted := false

	// Bind error passthrough service for service-layer rule matching
	if h.errorPassthroughService != nil {
		service.BindErrorPassthroughService(c, h.errorPassthroughService)
	}

	// Get subscription info (may be nil)
	subscription, _ := middleware2.GetSubscriptionFromContext(c)

	service.SetOpsLatencyMs(c, service.OpsAuthLatencyMsKey, time.Since(requestStart).Milliseconds())
	routingStart := time.Now()

	// 1. Acquire user concurrency slot
	userReleaseFunc, err := h.acquireUserConcurrencySlot(c, subject, reqStream, &streamStarted, reqLog)
	if err != nil {
		return // error already written
	}
	userReleaseFunc = wrapReleaseOnDone(c.Request.Context(), userReleaseFunc)
	if userReleaseFunc != nil {
		defer userReleaseFunc()
	}

	// 2. Re-check billing eligibility after wait
	if err := h.billingCacheService.CheckBillingEligibility(c.Request.Context(), apiKey.User, apiKey, apiKey.Group, subscription); err != nil {
		reqLog.Info("responses.billing_eligibility_check_failed", zap.Error(err))
		status, code, message := billingErrorDetails(err)
		h.handleStreamingAwareError(c, status, code, message, streamStarted)
		return
	}

	// 3. Account scheduling loop with failover
	h.runAccountLoop(c, apiKey, subscription, subject, body, reqModel, reqStream, &streamStarted, reqLog, routingStart)
}

// readAndValidateBody reads and validates the request body, returns body, model, stream flag, and ok.
func (h *ResponsesHandler) readAndValidateBody(c *gin.Context) (body []byte, model string, stream bool, ok bool) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		if maxErr, readOk := extractMaxBytesError(err); readOk {
			h.errorResponse(c, http.StatusRequestEntityTooLarge, "invalid_request_error", buildBodyTooLargeMessage(maxErr.Limit))
			return nil, "", false, false
		}
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to read request body")
		return nil, "", false, false
	}
	if len(body) == 0 {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Request body is empty")
		return nil, "", false, false
	}
	if !gjson.ValidBytes(body) {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		return nil, "", false, false
	}
	modelResult := gjson.GetBytes(body, "model")
	if !modelResult.Exists() || modelResult.Type != gjson.String || modelResult.String() == "" {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "model is required")
		return nil, "", false, false
	}
	streamResult := gjson.GetBytes(body, "stream")
	return body, modelResult.String(), streamResult.Bool(), true
}
