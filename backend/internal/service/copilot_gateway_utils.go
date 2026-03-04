package service

import (
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

// truncate returns s truncated to maxLen characters.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

// chatCompletionsUsage extracts usage with prompt_tokens/completion_tokens field names.
func chatCompletionsUsage(u gjson.Result) CopilotUsage {
	return CopilotUsage{
		PromptTokens:     int(u.Get("prompt_tokens").Int()),
		CompletionTokens: int(u.Get("completion_tokens").Int()),
		TotalTokens:      int(u.Get("total_tokens").Int()),
	}
}

// responsesUsage extracts usage with input_tokens/output_tokens field names.
func responsesUsage(u gjson.Result) CopilotUsage {
	pt := int(u.Get("input_tokens").Int())
	ct := int(u.Get("output_tokens").Int())
	return CopilotUsage{PromptTokens: pt, CompletionTokens: ct, TotalTokens: pt + ct}
}

// detectInitiator returns "agent" for all requests.
// Copilot handles "user" detection internally.
func detectInitiator(_ []byte) string {
	return "agent"
}

// detectVision checks if any message contains an image_url content part.
// Used for /chat/completions requests.
func detectVision(body []byte) bool {
	messages := gjson.GetBytes(body, "messages")
	if !messages.Exists() {
		return false
	}
	for _, msg := range messages.Array() {
		content := msg.Get("content")
		if !content.IsArray() {
			continue
		}
		for _, part := range content.Array() {
			if part.Get("type").String() == "image_url" {
				return true
			}
		}
	}
	return false
}

// detectInitiatorResponses returns "agent" for all Responses API requests.
// Copilot handles "user" detection internally.
func detectInitiatorResponses(_ []byte) string {
	return "agent"
}

// detectVisionResponses checks if any input item contains an input_image content part.
// Used for Responses API requests.
func detectVisionResponses(body []byte) bool {
	input := gjson.GetBytes(body, "input")
	if !input.Exists() {
		return false
	}
	for _, item := range input.Array() {
		content := item.Get("content")
		if !content.IsArray() {
			continue
		}
		for _, part := range content.Array() {
			if part.Get("type").String() == "input_image" {
				return true
			}
		}
	}
	return false
}

// detectInitiatorMessages returns "agent" for all requests.
// Copilot handles "user" detection internally.
func detectInitiatorMessages(_ []byte) string {
	return "agent"
}

// detectVisionMessages checks if any message in an Anthropic Messages request
// contains an image content block (type == "image").
func detectVisionMessages(body []byte) bool {
	messages := gjson.GetBytes(body, "messages")
	if !messages.Exists() {
		return false
	}
	for _, msg := range messages.Array() {
		content := msg.Get("content")
		if !content.IsArray() {
			continue
		}
		for _, part := range content.Array() {
			if part.Get("type").String() == "image" {
				return true
			}
		}
	}
	return false
}

// ensureResponsesInclude adds "reasoning.encrypted_content" to the include array if not present.
func ensureResponsesInclude(body []byte) []byte {
	const target = "reasoning.encrypted_content"
	includes := gjson.GetBytes(body, "include")
	if includes.Exists() && includes.IsArray() {
		for _, v := range includes.Array() {
			if v.String() == target {
				return body // already present
			}
		}
		// Append to existing array
		body, _ = sjson.SetBytes(body, "include.-1", target)
		return body
	}
	// No include field — create it
	body, _ = sjson.SetBytes(body, "include", []string{target})
	return body
}

// stripCacheControlScope removes "scope" from cache_control objects in system
// and messages arrays. Copilot API rejects this field with 400.
func stripCacheControlScope(body []byte) []byte {
	// Strip from system array
	sysArr := gjson.GetBytes(body, "system")
	if sysArr.IsArray() {
		for i, item := range sysArr.Array() {
			if item.Get("cache_control.scope").Exists() {
				updated, err := sjson.DeleteBytes(body, fmt.Sprintf("system.%d.cache_control.scope", i))
				if err != nil {
					logger.L().Debug("stripCacheControlScope: failed to delete system cache_control.scope", zap.Int("index", i), zap.Error(err))
				} else {
					body = updated
				}
			}
		}
	}
	// Strip from messages and their content arrays
	msgs := gjson.GetBytes(body, "messages")
	if msgs.IsArray() {
		for i, msg := range msgs.Array() {
			if msg.Get("cache_control.scope").Exists() {
				updated, err := sjson.DeleteBytes(body, fmt.Sprintf("messages.%d.cache_control.scope", i))
				if err != nil {
					logger.L().Debug("stripCacheControlScope: failed to delete message cache_control.scope", zap.Int("index", i), zap.Error(err))
				} else {
					body = updated
				}
			}
			if msg.Get("content").IsArray() {
				for j, block := range msg.Get("content").Array() {
					if block.Get("cache_control.scope").Exists() {
						updated, err := sjson.DeleteBytes(body, fmt.Sprintf("messages.%d.content.%d.cache_control.scope", i, j))
						if err != nil {
							logger.L().Debug("stripCacheControlScope: failed to delete content cache_control.scope", zap.Int("msg_index", i), zap.Int("content_index", j), zap.Error(err))
						} else {
							body = updated
						}
					}
				}
			}
		}
	}
	return body
}

// extractAnthropicUsage extracts token usage from an Anthropic Messages
// response body (non-stream). It accounts for cache_creation_input_tokens and
// cache_read_input_tokens which are Anthropic-specific fields.
func extractAnthropicUsage(body []byte) CopilotUsage {
	u := gjson.GetBytes(body, "usage")
	if !u.Exists() {
		return CopilotUsage{}
	}
	inputTokens := int(u.Get("input_tokens").Int())
	outputTokens := int(u.Get("output_tokens").Int())
	cacheCreation := int(u.Get("cache_creation_input_tokens").Int())
	cacheRead := int(u.Get("cache_read_input_tokens").Int())
	promptTokens := inputTokens + cacheCreation + cacheRead
	return CopilotUsage{
		PromptTokens:     promptTokens,
		CompletionTokens: outputTokens,
		TotalTokens:      promptTokens + outputTokens,
	}
}

// filterAnthropicBeta removes the "claude-code-20250219" token from an
// Anthropic-Beta header value since Copilot's /v1/messages endpoint doesn't
// recognise it.
func filterAnthropicBeta(beta string) string {
	const dropToken = "claude-code-20250219"
	parts := strings.Split(beta, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || p == dropToken {
			continue
		}
		out = append(out, p)
	}
	return strings.Join(out, ",")
}
