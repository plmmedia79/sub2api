// Package apicompat provides stream processing for the OpenAI Responses API.
// This module handles SSE event streaming from GitHub Copilot's /responses endpoint,
// including Stream ID synchronization and reasoning event handling.
package apicompat

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

// StreamEventHandler is a callback that receives processed SSE events.
// The eventType is the SSE event type (e.g., "response.output_text.delta").
// The data is the JSON payload for the event.
type StreamEventHandler func(eventType string, data string) error

// ProcessResponsesStream reads SSE events from an upstream Responses API stream,
// applies Stream ID synchronization, handles reasoning events, and forwards
// all events to the provided handler callback.
//
// This is the core streaming processor for the /responses endpoint. It:
// - Tracks response metadata (id, model, created_at)
// - Tracks active reasoning blocks with encrypted_content
// - Handles text delta/done events
// - Handles function_call delta/done events
// - Properly flushes remaining events on stream completion
//
// Parameters:
//   - reader: The upstream response body to read SSE events from
//   - handler: Callback to receive each processed event
//
// Returns:
//   - *ResponsesUsageDetail: Token usage from the response (if available)
//   - error: Any error encountered during stream processing
func ProcessResponsesStream(reader io.Reader, handler StreamEventHandler) (*ResponsesUsageDetail, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	state := NewResponsesStreamState()
	idTracker := NewStreamIdTracker()

	for scanner.Scan() {
		line := scanner.Text()

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, ":") {
			// Forward comments and empty lines as-is
			if line != "" {
				if err := handler("", line); err != nil {
					return nil, err
				}
			}
			continue
		}

		// Parse SSE format: "event: type" or "data: json"
		if strings.HasPrefix(line, "event: ") {
			eventType := strings.TrimSpace(line[7:])
			state.currentEventType = eventType
			continue
		}

		if strings.HasPrefix(line, "data: ") {
			data := line[6:]

			// Handle stream termination
			if data == "[DONE]" {
				if err := flushRemainingEvents(state, handler); err != nil {
					return nil, err
				}
				return state.finalUsage(), nil
			}

			// Process the SSE data event
			processedData, err := processSSEEvent(state, idTracker, state.currentEventType, data)
			if err != nil {
				// Log parse errors instead of silently swallowing them
				logger.L().Warn("responses_stream.parse_error",
					zap.String("event_type", state.currentEventType),
					zap.Error(err))
				continue
			}

			// Forward the processed event to the handler
			if state.currentEventType != "" {
				if err := handler(state.currentEventType, processedData); err != nil {
					return nil, err
				}
			} else {
				// No explicit event type, infer from data
				if err := handler("", processedData); err != nil {
					return nil, err
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("stream read error: %w", err)
	}

	// Flush any remaining events on clean exit
	if err := flushRemainingEvents(state, handler); err != nil {
		return nil, err
	}

	return state.finalUsage(), nil
}

// flushRemainingEvents ensures all pending events are flushed before stream completion.
// This handles cases where the upstream may have sent events without corresponding
// done events.
func flushRemainingEvents(state *ResponsesStreamState, handler StreamEventHandler) error {
	// Flush any pending text
	if state.CurrentText != "" {
		doneEvent := map[string]any{
			"type":         "response.output_text.done",
			"item_id":      state.CurrentItemID,
			"output_index": state.ContentIndex,
			"text":         state.CurrentText,
		}
		if data, err := json.Marshal(doneEvent); err == nil {
			if err := handler("response.output_text.done", string(data)); err != nil {
				return err
			}
		}
		state.CurrentText = ""
	}

	// Mark all incomplete tool calls as done
	for i := range state.ToolCalls {
		if !state.ToolCalls[i].IsComplete && state.ToolCalls[i].Status == "in_progress" {
			doneEvent := map[string]any{
				"type":    "response.function_call_arguments.done",
				"call_id": state.ToolCalls[i].CallID,
				"name":    state.ToolCalls[i].Name,
			}
			if state.ToolCalls[i].Arguments != "" {
				doneEvent["arguments"] = state.ToolCalls[i].Arguments
			}
			if data, err := json.Marshal(doneEvent); err == nil {
				if err := handler("response.function_call_arguments.done", string(data)); err != nil {
					return err
				}
			}
		}
	}

	// Mark all incomplete reasoning blocks as done
	for i := range state.ReasoningBlocks {
		if !state.ReasoningBlocks[i].IsComplete && state.ReasoningBlocks[i].Status == "in_progress" {
			doneEvent := map[string]any{
				"type":          "response.reasoning_summary_text.done",
				"item_id":       state.ReasoningBlocks[i].ItemID,
				"summary_index": state.ReasoningBlocks[i].SummaryIndex,
			}
			if data, err := json.Marshal(doneEvent); err == nil {
				if err := handler("response.reasoning_summary_text.done", string(data)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// FormatSSEEvent formats an event type and data as an SSE line.
func FormatSSEEvent(eventType, data string) string {
	if eventType != "" {
		return fmt.Sprintf("event: %s\ndata: %s\n\n", eventType, data)
	}
	return fmt.Sprintf("data: %s\n\n", data)
}

// FormatSSEData formats data as an SSE data line (no event type).
func FormatSSEData(data string) string {
	return fmt.Sprintf("data: %s\n", data)
}

// FormatSSEDone returns the SSE [DONE] sentinel.
func FormatSSEDone() string {
	return "data: [DONE]\n\n"
}
