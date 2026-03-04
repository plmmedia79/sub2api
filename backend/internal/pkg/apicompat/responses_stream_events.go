// Package apicompat provides SSE event routing for Responses API stream processing.
// This module parses individual SSE data payloads, applies Stream ID fixes,
// and dispatches each event type to the appropriate state handler.
package apicompat

import (
	"encoding/json"
	"fmt"
)

// processSSEEvent processes a single SSE event, updating state and applying
// Stream ID fixes. Returns the processed JSON data string.
func processSSEEvent(state *ResponsesStreamState, idTracker *StreamIdTracker, eventType, data string) (string, error) {
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal([]byte(data), &parsed); err != nil {
		return "", fmt.Errorf("failed to parse event JSON: %w", err)
	}

	// Extract the type field from the JSON payload
	var typeStr string
	if typeBytes, ok := parsed["type"]; ok {
		_ = json.Unmarshal(typeBytes, &typeStr)
	}

	// Use JSON type if SSE event type is not set
	if eventType == "" && typeStr != "" {
		eventType = typeStr
	}

	// Apply Stream ID synchronization
	fixedData, err := idTracker.FixStreamIds(data, eventType)
	if err != nil {
		// If ID fix fails, use original data
		fixedData = data
	}

	// Update state based on event type
	updateStreamState(state, eventType, parsed)

	return fixedData, nil
}

// updateStreamState dispatches the parsed event to the correct state handler
// based on the event type string.
func updateStreamState(state *ResponsesStreamState, eventType string, parsed map[string]json.RawMessage) {
	switch eventType {
	case "response.created":
		state.handleResponseCreated(parsed)

	case "response.output_item.added":
		state.handleOutputItemAdded(parsed)

	case "response.output_item.done":
		state.handleOutputItemDone(parsed)

	case "response.output_text.delta":
		state.handleOutputTextDelta(parsed)

	case "response.output_text.done":
		state.handleOutputTextDone(parsed)

	case "response.function_call_arguments.delta":
		state.handleFunctionCallDelta(parsed)

	case "response.function_call_arguments.done":
		state.handleFunctionCallDone(parsed)

	case "response.reasoning_summary_part.added":
		state.handleReasoningSummaryPartAdded(parsed)

	case "response.reasoning_summary_text.delta":
		state.handleReasoningSummaryTextDelta(parsed)

	case "response.reasoning_summary_text.done":
		state.handleReasoningSummaryTextDone(parsed)

	case "response.completed", "response.incomplete":
		state.handleResponseCompleted(parsed)
	}
}
