// Package apicompat provides item/tool/reasoning state handlers for Responses stream processing.
package apicompat

import "encoding/json"

// handleOutputItemAdded processes response.output_item.added (M1: uses index tracking).
func (s *ResponsesStreamState) handleOutputItemAdded(parsed map[string]json.RawMessage) {
	var outputIndex int
	if idxBytes, ok := parsed["output_index"]; ok {
		_ = json.Unmarshal(idxBytes, &outputIndex)
	}

	if itemBytes, ok := parsed["item"]; ok {
		var item struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		}
		if err := json.Unmarshal(itemBytes, &item); err == nil {
			s.CurrentItemID = item.ID
			s.ContentIndex = outputIndex

			switch item.Type {
			case "message":
				s.CurrentText = ""
				s.PendingText = nil

			case "function_call":
				newToolCall := ResponsesToolCallState{
					Index:  len(s.ToolCalls),
					ItemID: item.ID,
					Status: "in_progress",
				}
				s.ToolCalls = append(s.ToolCalls, newToolCall)
				s.currentToolCallIndex = len(s.ToolCalls) - 1 // M1: index survives future appends
				s.refreshCurrentToolCall()

			case "reasoning":
				newReasoning := ResponsesReasoningState{
					ItemID:       item.ID,
					SummaryIndex: 0,
					Status:       "in_progress",
				}
				s.ReasoningBlocks = append(s.ReasoningBlocks, newReasoning)
				s.currentReasoningIndex = len(s.ReasoningBlocks) - 1 // M1: index survives future appends
				s.refreshCurrentReasoning()

				// Extract encrypted_content if present
				var reasoningItem struct {
					EncryptedContent string `json:"encrypted_content"`
				}
				if err := json.Unmarshal(itemBytes, &reasoningItem); err == nil && reasoningItem.EncryptedContent != "" {
					s.ReasoningBlocks[s.currentReasoningIndex].SummaryText = reasoningItem.EncryptedContent
					s.refreshCurrentReasoning()
				}
			}
		}
	}
}

// handleOutputItemDone processes the response.output_item.done event.
func (s *ResponsesStreamState) handleOutputItemDone(parsed map[string]json.RawMessage) {
	var outputIndex int
	if idxBytes, ok := parsed["output_index"]; ok {
		_ = json.Unmarshal(idxBytes, &outputIndex)
	}

	if itemBytes, ok := parsed["item"]; ok {
		var item struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		}
		if err := json.Unmarshal(itemBytes, &item); err == nil {
			switch item.Type {
			case "message":
				s.CurrentText = ""

			case "function_call":
				for i := range s.ToolCalls {
					if s.ToolCalls[i].ItemID == item.ID || s.ToolCalls[i].Index == outputIndex {
						s.ToolCalls[i].Status = "completed"
						s.ToolCalls[i].IsComplete = true
						break
					}
				}

			case "reasoning":
				for i := range s.ReasoningBlocks {
					if s.ReasoningBlocks[i].ItemID == item.ID {
						s.ReasoningBlocks[i].Status = "completed"
						s.ReasoningBlocks[i].IsComplete = true
						break
					}
				}
			}
		}
	}
}

// handleFunctionCallDelta processes the response.function_call_arguments.delta event.
func (s *ResponsesStreamState) handleFunctionCallDelta(parsed map[string]json.RawMessage) {
	var outputIndex int
	if idxBytes, ok := parsed["output_index"]; ok {
		_ = json.Unmarshal(idxBytes, &outputIndex)
	}

	if deltaBytes, ok := parsed["delta"]; ok {
		var delta string
		if err := json.Unmarshal(deltaBytes, &delta); err == nil {
			for i := range s.ToolCalls {
				if s.ToolCalls[i].Index == outputIndex {
					s.ToolCalls[i].Arguments += delta
					break
				}
			}
		}
	}
}

// handleFunctionCallDone processes the response.function_call_arguments.done event.
func (s *ResponsesStreamState) handleFunctionCallDone(parsed map[string]json.RawMessage) {
	var callID string
	if idBytes, ok := parsed["call_id"]; ok {
		_ = json.Unmarshal(idBytes, &callID)
	}

	if argumentsBytes, ok := parsed["arguments"]; ok {
		var arguments string
		if err := json.Unmarshal(argumentsBytes, &arguments); err == nil {
			for i := range s.ToolCalls {
				if s.ToolCalls[i].CallID == callID || s.ToolCalls[i].ItemID == callID {
					s.ToolCalls[i].Arguments = arguments
					break
				}
			}
		}
	}
}

// handleReasoningSummaryPartAdded processes the response.reasoning_summary_part.added event.
func (s *ResponsesStreamState) handleReasoningSummaryPartAdded(parsed map[string]json.RawMessage) {
	var summaryIndex int
	if idxBytes, ok := parsed["summary_index"]; ok {
		_ = json.Unmarshal(idxBytes, &summaryIndex)
	}

	if s.CurrentReasoning != nil {
		s.CurrentReasoning.SummaryIndex = summaryIndex
		s.CurrentReasoning.SummaryText = ""
	}
}

// handleReasoningSummaryTextDelta processes response.reasoning_summary_text.delta.
// Matches on both ItemID and summaryIndex for correct block identification (M7 fix).
func (s *ResponsesStreamState) handleReasoningSummaryTextDelta(parsed map[string]json.RawMessage) {
	var summaryIndex int
	if idxBytes, ok := parsed["summary_index"]; ok {
		_ = json.Unmarshal(idxBytes, &summaryIndex)
	}

	var itemID string
	if idBytes, ok := parsed["item_id"]; ok {
		_ = json.Unmarshal(idBytes, &itemID)
	}

	if deltaBytes, ok := parsed["delta"]; ok {
		var delta string
		if err := json.Unmarshal(deltaBytes, &delta); err == nil {
			for i := range s.ReasoningBlocks {
				// Match on both ItemID (when present) and summaryIndex for precision (M7 fix)
				itemMatch := itemID == "" || s.ReasoningBlocks[i].ItemID == itemID
				if itemMatch && s.ReasoningBlocks[i].SummaryIndex == summaryIndex {
					s.ReasoningBlocks[i].SummaryText += delta
					break
				}
			}
		}
	}
}

// handleReasoningSummaryTextDone processes response.reasoning_summary_text.done.
// Matches on both ItemID and summaryIndex (M7 fix).
func (s *ResponsesStreamState) handleReasoningSummaryTextDone(parsed map[string]json.RawMessage) {
	var summaryIndex int
	if idxBytes, ok := parsed["summary_index"]; ok {
		_ = json.Unmarshal(idxBytes, &summaryIndex)
	}

	var itemID string
	if idBytes, ok := parsed["item_id"]; ok {
		_ = json.Unmarshal(idBytes, &itemID)
	}

	for i := range s.ReasoningBlocks {
		itemMatch := itemID == "" || s.ReasoningBlocks[i].ItemID == itemID
		if itemMatch && s.ReasoningBlocks[i].SummaryIndex == summaryIndex {
			s.ReasoningBlocks[i].IsComplete = true
			break
		}
	}
}
