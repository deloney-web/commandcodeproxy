package proxy

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/deloney-web/commandcodeproxy/internal/api"
	"github.com/google/uuid"
)

// writeResponsesSSE writes a Responses API Server-Sent Event (event: + data: format).
func writeResponsesSSE(w io.Writer, flusher http.Flusher, eventName string, data any) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("[ERROR] Failed to marshal SSE event %s: %v", eventName, err)
		return
	}
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventName, jsonData)
	flusher.Flush()
}

// buildOutputItem constructs a single message ResponseItem for the response output.
func buildOutputItem(id, status, text string) []api.ResponseItem {
	if text == "" && status == "in_progress" {
		return []api.ResponseItem{}
	}
	return []api.ResponseItem{
		{
			ID:     id,
			Type:   "message",
			Role:   "assistant",
			Status: status,
			Content: []api.ResponseContentPart{
				{Type: "output_text", Text: text, Annotations: []any{}},
			},
		},
	}
}

// streamResponses handles streaming response from CommandCode to OpenAI Responses API SSE.
// Emits the required lifecycle events:
//   response.created → response.in_progress → response.output_item.added →
//   response.content_part.added → response.output_text.delta* →
//   response.output_text.done → response.completed
// On error: response.failed. On premature close: response.incomplete.
func (p *Proxy) streamResponses(w http.ResponseWriter, r *http.Request, ccResp *http.Response, requestID, model string, created int64) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		p.writeOpenAIError(w, http.StatusInternalServerError, "Streaming not supported", "server_error")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	scanner := bufio.NewScanner(ccResp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	// Response object skeleton — updated as events arrive
	respObj := api.OpenAIResponseObject{
		ID:     requestID,
		Object: "response",
		Status: "in_progress",
		Model:  model,
		Output: []api.ResponseItem{},
	}

	itemID := "item_" + uuid.New().String()[:28]
	contentIndex := 0
	accumulatedText := ""
	hasItem := false

	// --- Lifecycle event 1: response.created ---
	writeResponsesSSE(w, flusher, "response.created", api.ResponseCreatedEvent{
		Type:     "response.created",
		Response: respObj,
	})

	// --- Lifecycle event 2: response.in_progress ---
	writeResponsesSSE(w, flusher, "response.in_progress", api.ResponseInProgressEvent{
		Type:     "response.in_progress",
		Response: respObj,
	})

	for scanner.Scan() {
		select {
		case <-r.Context().Done():
			respObj.Status = "incomplete"
			respObj.Output = buildOutputItem(itemID, "incomplete", accumulatedText)
			writeResponsesSSE(w, flusher, "response.incomplete", api.ResponseIncompleteEvent{
				Type:     "response.incomplete",
				Response: respObj,
			})
			return
		default:
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		p.debugf("[DEBUG] CommandCode stream line (responses): %s", truncateLog(line))

		var event api.CCStreamEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}

		switch event.Type {
		case "text-delta":
			if !hasItem {
				// --- Lifecycle event 3: response.output_item.added ---
				item := api.ResponseItem{
					ID:     itemID,
					Type:   "message",
					Role:   "assistant",
					Status: "in_progress",
					Content: []api.ResponseContentPart{
						{Type: "output_text", Text: "", Annotations: []any{}},
					},
				}
				writeResponsesSSE(w, flusher, "response.output_item.added", api.ResponseOutputItemAddedEvent{
					Type: "response.output_item.added",
					Item: item,
				})

				// --- Lifecycle event 4: response.content_part.added ---
				writeResponsesSSE(w, flusher, "response.content_part.added", api.ResponseContentPartAddedEvent{
					Type:         "response.content_part.added",
					Part:         api.ResponseContentPart{Type: "output_text", Text: "", Annotations: []any{}},
					ItemID:       itemID,
					ContentIndex: contentIndex,
				})
				hasItem = true
			}

			// --- Lifecycle event 5: response.output_text.delta ---
			writeResponsesSSE(w, flusher, "response.output_text.delta", api.ResponseOutputTextDeltaEvent{
				Type:         "output_text.delta",
				Delta:        event.Text,
				ItemID:       itemID,
				ContentIndex: contentIndex,
			})
			accumulatedText += event.Text

		case "tool-use", "tool-delta", "tool-input-start", "tool-input-delta", "tool-call":
			// Tool call events are consumed but not yet translated to Responses events.
			// Text before tool calls is already emitted as output_text events.

		case "finish":
			// --- Lifecycle event 6: response.output_text.done ---
			writeResponsesSSE(w, flusher, "response.output_text.done", api.ResponseOutputTextDoneEvent{
				Type:         "output_text.done",
				Text:         accumulatedText,
				ItemID:       itemID,
				ContentIndex: contentIndex,
			})

			respObj.Status = "completed"
			if hasItem {
				respObj.Output = buildOutputItem(itemID, "completed", accumulatedText)
			}

			if event.TotalUsage != nil {
				respObj.Usage = &api.OpenAIUsage{
					PromptTokens:     event.TotalUsage.InputTokens,
					CompletionTokens: event.TotalUsage.OutputTokens,
					TotalTokens:      event.TotalUsage.InputTokens + event.TotalUsage.OutputTokens,
				}
			}

			// --- Lifecycle event 7: response.completed ---
			writeResponsesSSE(w, flusher, "response.completed", api.ResponseCompletedEvent{
				Type:     "response.completed",
				Response: respObj,
			})
			return

		case "error":
			respObj.Status = "failed"
			if hasItem {
				respObj.Output = buildOutputItem(itemID, "failed", accumulatedText)
			}
			if event.Error != nil {
				respObj.Error = &api.OpenAIError{
					Message: event.Error.Message,
					Type:    "api_error",
				}
			}
			writeResponsesSSE(w, flusher, "response.failed", api.ResponseFailedEvent{
				Type:     "response.failed",
				Response: respObj,
			})
			return
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		log.Printf("[ERROR] Scanner error in Responses stream: %v", err)
	}

	// Stream ended without any terminal event — mark incomplete
	if respObj.Status == "in_progress" {
		respObj.Status = "incomplete"
		if hasItem {
			respObj.Output = buildOutputItem(itemID, "incomplete", accumulatedText)
		}
		writeResponsesSSE(w, flusher, "response.incomplete", api.ResponseIncompleteEvent{
			Type:     "response.incomplete",
			Response: respObj,
		})
	}
}

// nonStreamResponses handles non-streaming Responses API requests by accumulating
// the CommandCode stream and returning a single JSON response object.
func (p *Proxy) nonStreamResponses(w http.ResponseWriter, ccResp *http.Response, requestID, model string, created int64) {
	scanner := bufio.NewScanner(ccResp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var accumulatedText strings.Builder
	var inputTokens, outputTokens int
	var streamError string

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		p.debugf("[DEBUG] CommandCode stream line (responses non-stream): %s", truncateLog(line))

		var event api.CCStreamEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}

		switch event.Type {
		case "text-delta":
			accumulatedText.WriteString(event.Text)
		case "finish":
			if event.TotalUsage != nil {
				inputTokens = event.TotalUsage.InputTokens
				outputTokens = event.TotalUsage.OutputTokens
			}
		case "error":
			if event.Error != nil {
				streamError = event.Error.Message
			}
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		log.Printf("[ERROR] Scanner error in Responses non-stream: %v", err)
	}

	respObj := api.OpenAIResponseObject{
		ID:     requestID,
		Object: "response",
		Model:  model,
		Output: []api.ResponseItem{},
	}

	if streamError != "" {
		respObj.Status = "failed"
		respObj.Error = &api.OpenAIError{
			Message: streamError,
			Type:    "api_error",
		}
	} else {
		respObj.Status = "completed"
		if accumulatedText.Len() > 0 {
			respObj.Output = []api.ResponseItem{
				{
					ID:     "item_" + uuid.New().String()[:28],
					Type:   "message",
					Role:   "assistant",
					Status: "completed",
					Content: []api.ResponseContentPart{
						{Type: "output_text", Text: accumulatedText.String(), Annotations: []any{}},
					},
				},
			}
		}
	}

	if inputTokens > 0 || outputTokens > 0 {
		respObj.Usage = &api.OpenAIUsage{
			PromptTokens:     inputTokens,
			CompletionTokens: outputTokens,
			TotalTokens:      inputTokens + outputTokens,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(respObj)
}
