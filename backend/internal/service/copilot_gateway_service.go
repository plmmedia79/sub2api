package service

import (
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/tidwall/gjson"
)

const (
	copilotUpstreamURL          = "https://api.githubcopilot.com/chat/completions"
	copilotUpstreamResponsesURL = "https://api.githubcopilot.com/responses"
	copilotUpstreamMessagesURL  = "https://api.githubcopilot.com/v1/messages"
	copilotUpstreamModelsURL    = "https://api.githubcopilot.com/models"
)

// CopilotUsage represents token usage from a Copilot chat/completions response.
type CopilotUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// CopilotForwardResult holds the outcome of a forwarded Copilot request.
type CopilotForwardResult struct {
	RequestID    string
	Usage        CopilotUsage
	Model        string
	Stream       bool
	Duration     time.Duration
	FirstTokenMs *int
}

// usageExtractor extracts CopilotUsage from a gjson usage object.
type usageExtractor func(u gjson.Result) CopilotUsage

// copilotForwardConfig holds per-endpoint differences for the shared forward path.
type copilotForwardConfig struct {
	upstreamURL  string
	extractUsage usageExtractor
	detectInit   func([]byte) string
	detectVision func([]byte) bool
}

// CopilotGatewayService forwards chat/completions requests to GitHub Copilot,
// precisely mimicking opencode's request headers.
type CopilotGatewayService struct {
	accountRepo          AccountRepository
	cfg                  *config.Config
	httpUpstream         HTTPUpstream
	copilotTokenProvider *CopilotTokenProvider
	versionService       *OpenCodeVersionService
	responseHeaderFilter *responseheaders.CompiledHeaderFilter
}

// NewCopilotGatewayService creates a new CopilotGatewayService.
func NewCopilotGatewayService(
	accountRepo AccountRepository,
	cfg *config.Config,
	httpUpstream HTTPUpstream,
	copilotTokenProvider *CopilotTokenProvider,
	versionService *OpenCodeVersionService,
) *CopilotGatewayService {
	return &CopilotGatewayService{
		accountRepo:          accountRepo,
		cfg:                  cfg,
		httpUpstream:         httpUpstream,
		copilotTokenProvider: copilotTokenProvider,
		responseHeaderFilter: compileResponseHeaderFilter(cfg),
		versionService:       versionService,
	}
}
