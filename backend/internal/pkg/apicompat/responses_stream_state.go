// Package apicompat provides state management for Responses API stream processing.
// This module tracks all stateful data during SSE streaming: response metadata,
// tool calls, reasoning blocks, and token usage.
package apicompat

import (
	"encoding/json"
	"time"
)

// NewResponsesStreamState creates a new stream state tracker with safe defaults.
// currentToolCallIndex and currentReasoningIndex use -1 to indicate "none active".
func NewResponsesStreamState() *ResponsesStreamState {
	return &ResponsesStreamState{
		Created:                time.Now().Unix(),
		ToolCalls:              make([]ResponsesToolCallState, 0),
		ReasoningBlocks:        make([]ResponsesReasoningState, 0),
		Status:                 "in_progress",
		currentToolCallIndex:   -1,
		currentReasoningIndex:  -1,
	}
}

// finalUsage returns the usage summary from the stream state.
func (s *ResponsesStreamState) finalUsage() *ResponsesUsageDetail {
	return &ResponsesUsageDetail{
		InputTokens:  s.InputTokens,
		OutputTokens: s.OutputTokens,
		TotalTokens:  s.InputTokens + s.OutputTokens,
	}
}

// handleResponseCreated processes the response.created event.
func (s *ResponsesStreamState) handleResponseCreated(parsed map[string]json.RawMessage) {
	if responseBytes, ok := parsed["response"]; ok {
		var response struct {
			ID        string `json:"id"`
			Model     string `json:"model"`
			CreatedAt int64  `json:"created_at"`
		}
		if err := json.Unmarshal(responseBytes, &response); err == nil {
			s.ID = response.ID
			s.Model = response.Model
			if response.CreatedAt > 0 {
				s.Created = response.CreatedAt
			}
		}
	}
	s.Status = "in_progress"
}

// handleOutputTextDelta processes the response.output_text.delta event.
func (s *ResponsesStreamState) handleOutputTextDelta(parsed map[string]json.RawMessage) {
	if deltaBytes, ok := parsed["delta"]; ok {
		var delta string
		if err := json.Unmarshal(deltaBytes, &delta); err == nil {
			s.CurrentText += delta
		}
	}
}

// handleOutputTextDone processes the response.output_text.done event.
func (s *ResponsesStreamState) handleOutputTextDone(_ map[string]json.RawMessage) {
	s.CurrentText = ""
}

// handleResponseCompleted processes the response.completed or response.incomplete event.
func (s *ResponsesStreamState) handleResponseCompleted(parsed map[string]json.RawMessage) {
	if responseBytes, ok := parsed["response"]; ok {
		var response struct {
			Status string                `json:"status"`
			Usage  *ResponsesUsageDetail `json:"usage"`
		}
		if err := json.Unmarshal(responseBytes, &response); err == nil {
			s.Status = response.Status
			if response.Usage != nil {
				s.InputTokens = response.Usage.InputTokens
				s.OutputTokens = response.Usage.OutputTokens
			}
		}
	}
}

// refreshCurrentToolCall updates the CurrentToolCall pointer from the index.
// Must be called after any append to ToolCalls to prevent dangling pointers.
func (s *ResponsesStreamState) refreshCurrentToolCall() {
	if s.currentToolCallIndex >= 0 && s.currentToolCallIndex < len(s.ToolCalls) {
		s.CurrentToolCall = &s.ToolCalls[s.currentToolCallIndex]
	} else {
		s.CurrentToolCall = nil
	}
}

// refreshCurrentReasoning updates the CurrentReasoning pointer from the index.
// Must be called after any append to ReasoningBlocks to prevent dangling pointers.
func (s *ResponsesStreamState) refreshCurrentReasoning() {
	if s.currentReasoningIndex >= 0 && s.currentReasoningIndex < len(s.ReasoningBlocks) {
		s.CurrentReasoning = &s.ReasoningBlocks[s.currentReasoningIndex]
	} else {
		s.CurrentReasoning = nil
	}
}
