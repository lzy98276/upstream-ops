package protocol

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

func GeminiModelFromPath(path string) string {
	path = strings.TrimSpace(path)
	marker := "/models/"
	idx := strings.Index(path, marker)
	if idx < 0 {
		marker = "models/"
		idx = strings.Index(path, marker)
		if idx < 0 {
			return ""
		}
	}
	value := path[idx+len(marker):]
	if colon := strings.IndexByte(value, ':'); colon >= 0 {
		value = value[:colon]
	}
	value, err := url.PathUnescape(strings.Trim(value, "/"))
	if err != nil {
		return ""
	}
	return strings.TrimPrefix(strings.TrimSpace(value), "models/")
}

func GeminiActionFromPath(path string) string {
	path = strings.TrimSpace(path)
	colon := strings.LastIndex(path, ":")
	if colon < 0 {
		return ""
	}
	action := path[colon+1:]
	if query := strings.IndexByte(action, '?'); query >= 0 {
		action = action[:query]
	}
	return strings.TrimSpace(action)
}

func GeminiPath(model, inboundPath string, stream bool) string {
	action := GeminiActionFromPath(inboundPath)
	if action == "" {
		action = "generateContent"
	}
	if stream && action == "generateContent" {
		action = "streamGenerateContent"
	}
	model = strings.TrimPrefix(strings.TrimSpace(model), "models/")
	path := "/v1beta/models/" + url.PathEscape(model) + ":" + action
	if action == "streamGenerateContent" {
		return path + "?alt=sse"
	}
	return path
}

func GeminiStreamAction(path string) bool {
	return strings.EqualFold(GeminiActionFromPath(path), "streamGenerateContent")
}

func OpenAIToGeminiRequest(body []byte, model string, stream bool) ([]byte, error) {
	var in map[string]any
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid openai request: %w", err)
	}
	out := map[string]any{}
	var systemParts []any
	var contents []any
	if rawMessages, ok := in["messages"].([]any); ok {
		for _, raw := range rawMessages {
			message, _ := raw.(map[string]any)
			if message == nil {
				continue
			}
			role, _ := message["role"].(string)
			parts := openAIContentToGeminiParts(message["content"])
			switch role {
			case "system", "developer":
				systemParts = append(systemParts, parts...)
			case "assistant":
				parts = append(parts, openAIToolCallsToGeminiParts(message["tool_calls"])...)
				contents = append(contents, geminiContent("model", parts))
			case "tool":
				name, _ := message["name"].(string)
				if name == "" {
					name, _ = message["tool_call_id"].(string)
				}
				contents = append(contents, geminiContent("user", []any{map[string]any{
					"functionResponse": map[string]any{
						"name":     name,
						"response": map[string]any{"result": contentToPlainText(message["content"])},
					},
				}}))
			default:
				contents = append(contents, geminiContent("user", parts))
			}
		}
	}
	if len(systemParts) > 0 {
		out["systemInstruction"] = map[string]any{"parts": systemParts}
	}
	if len(contents) == 0 {
		contents = []any{geminiContent("user", []any{map[string]any{"text": ""}})}
	}
	out["contents"] = contents

	config := map[string]any{}
	if value, ok := asFloat(in["temperature"]); ok {
		config["temperature"] = value
	}
	if value, ok := asFloat(in["top_p"]); ok {
		config["topP"] = value
	}
	if value, ok := asInt(in["max_tokens"]); ok && value > 0 {
		config["maxOutputTokens"] = value
	} else if value, ok := asInt(in["max_completion_tokens"]); ok && value > 0 {
		config["maxOutputTokens"] = value
	}
	if stop, ok := in["stop"]; ok {
		config["stopSequences"] = normalizeStop(stop)
	}
	if len(config) > 0 {
		out["generationConfig"] = config
	}
	if declarations := openAIToolsToGemini(in["tools"]); len(declarations) > 0 {
		out["tools"] = []any{map[string]any{"functionDeclarations": declarations}}
	}
	_ = model
	_ = stream
	return json.Marshal(out)
}

func GeminiToOpenAIRequest(body []byte, model string, stream bool) ([]byte, error) {
	var in map[string]any
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid gemini request: %w", err)
	}
	out := map[string]any{"model": model, "stream": stream}
	messages := make([]any, 0)
	if instruction, ok := in["systemInstruction"].(map[string]any); ok {
		if content := geminiPartsToOpenAIContent(instruction["parts"]); content != nil {
			messages = append(messages, map[string]any{"role": "system", "content": content})
		}
	}
	if contents, ok := in["contents"].([]any); ok {
		for _, raw := range contents {
			content, _ := raw.(map[string]any)
			if content == nil {
				continue
			}
			role, _ := content["role"].(string)
			if role == "model" {
				role = "assistant"
			}
			if role == "" {
				role = "user"
			}
			text, toolCalls, toolResponses := geminiPartsToOpenAI(content["parts"])
			if len(toolResponses) > 0 {
				messages = append(messages, toolResponses...)
				if text == nil && len(toolCalls) == 0 {
					continue
				}
			}
			message := map[string]any{"role": role, "content": text}
			if len(toolCalls) > 0 {
				message["tool_calls"] = toolCalls
				if text == nil {
					message["content"] = nil
				}
			}
			messages = append(messages, message)
		}
	}
	if len(messages) == 0 {
		messages = append(messages, map[string]any{"role": "user", "content": ""})
	}
	out["messages"] = messages
	if config, ok := in["generationConfig"].(map[string]any); ok {
		if value, ok := asFloat(config["temperature"]); ok {
			out["temperature"] = value
		}
		if value, ok := asFloat(config["topP"]); ok {
			out["top_p"] = value
		}
		if value, ok := asInt(config["maxOutputTokens"]); ok && value > 0 {
			out["max_tokens"] = value
		}
		if stop, ok := config["stopSequences"]; ok {
			out["stop"] = stop
		}
	}
	if tools := geminiToolsToOpenAI(in["tools"]); len(tools) > 0 {
		out["tools"] = tools
	}
	return json.Marshal(out)
}

func GeminiToOpenAIResponse(body []byte, model string) ([]byte, error) {
	var in map[string]any
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, err
	}
	if errorObj, ok := in["error"].(map[string]any); ok {
		return json.Marshal(map[string]any{"error": errorObj})
	}
	message := map[string]any{"role": "assistant", "content": ""}
	finishReason := "stop"
	if candidates, ok := in["candidates"].([]any); ok && len(candidates) > 0 {
		candidate, _ := candidates[0].(map[string]any)
		if candidate != nil {
			if content, ok := candidate["content"].(map[string]any); ok {
				text, toolCalls, _ := geminiPartsToOpenAI(content["parts"])
				message["content"] = text
				if len(toolCalls) > 0 {
					message["tool_calls"] = toolCalls
					if text == nil || text == "" {
						message["content"] = nil
					}
				}
			}
			finishReason = geminiFinishToOpenAI(stringValue(candidate["finishReason"]))
		}
	}
	usage := geminiUsageToOpenAI(in["usageMetadata"])
	out := map[string]any{
		"id":      "chatcmpl-gemini",
		"object":  "chat.completion",
		"created": 0,
		"model":   model,
		"choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finishReason}},
		"usage":   usage,
	}
	return json.Marshal(out)
}

func OpenAIToGeminiResponse(body []byte, model string) ([]byte, error) {
	var in map[string]any
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, err
	}
	if errorObj, ok := in["error"].(map[string]any); ok {
		return json.Marshal(map[string]any{"error": errorObj})
	}
	parts := []any{}
	finishReason := "STOP"
	if choices, ok := in["choices"].([]any); ok && len(choices) > 0 {
		choice, _ := choices[0].(map[string]any)
		if choice != nil {
			if message, ok := choice["message"].(map[string]any); ok {
				parts = append(parts, openAIContentToGeminiParts(message["content"])...)
				parts = append(parts, openAIToolCallsToGeminiParts(message["tool_calls"])...)
			}
			finishReason = openAIFinishToGemini(stringValue(choice["finish_reason"]))
		}
	}
	if len(parts) == 0 {
		parts = append(parts, map[string]any{"text": ""})
	}
	out := map[string]any{
		"candidates": []any{map[string]any{
			"content":      map[string]any{"role": "model", "parts": parts},
			"finishReason": finishReason,
		}},
		"modelVersion": model,
	}
	if usage, ok := in["usage"].(map[string]any); ok {
		out["usageMetadata"] = map[string]any{
			"promptTokenCount":     geminiMapInt(usage, "prompt_tokens", "input_tokens"),
			"candidatesTokenCount": geminiMapInt(usage, "completion_tokens", "output_tokens"),
			"totalTokenCount":      geminiMapInt(usage, "total_tokens"),
		}
	}
	return json.Marshal(out)
}

func GeminiSSEToOpenAISSE(body []byte, model string) []byte {
	frames := make([][]byte, 0)
	roleSent := false
	finished := false
	for _, event := range parseSSEEvents(string(body)) {
		data := strings.TrimSpace(event.Data)
		if data == "" || data == "[DONE]" {
			continue
		}
		var payload map[string]any
		if json.Unmarshal([]byte(data), &payload) != nil {
			continue
		}
		if candidates, ok := payload["candidates"].([]any); ok && len(candidates) > 0 {
			candidate, _ := candidates[0].(map[string]any)
			if candidate != nil {
				var text string
				var toolCalls []any
				if content, ok := candidate["content"].(map[string]any); ok {
					converted, calls, _ := geminiPartsToOpenAI(content["parts"])
					text = contentToPlainText(converted)
					toolCalls = calls
				}
				if (text != "" || len(toolCalls) > 0) && !roleSent {
					frames = append(frames, openAISSEFrame(openaiChunk("chatcmpl-gemini", model, map[string]any{"role": "assistant"}, nil)))
					roleSent = true
				}
				if text != "" {
					frames = append(frames, openAISSEFrame(openaiChunk("chatcmpl-gemini", model, map[string]any{"content": text}, nil)))
				}
				if len(toolCalls) > 0 {
					frames = append(frames, openAISSEFrame(openaiChunk("chatcmpl-gemini", model, map[string]any{"tool_calls": toolCalls}, nil)))
				}
				if reason := stringValue(candidate["finishReason"]); reason != "" && !finished {
					frames = append(frames, openAISSEFrame(openaiChunk("chatcmpl-gemini", model, map[string]any{}, geminiFinishToOpenAI(reason))))
					finished = true
				}
			}
		}
		if usage, ok := payload["usageMetadata"]; ok {
			chunk := openaiChunk("chatcmpl-gemini", model, map[string]any{}, nil)
			var parsed map[string]any
			if json.Unmarshal(chunk, &parsed) == nil {
				parsed["usage"] = geminiUsageToOpenAI(usage)
				if encoded, err := json.Marshal(parsed); err == nil {
					frames = append(frames, openAISSEFrame(encoded))
				}
			}
		}
	}
	if !roleSent {
		frames = append(frames, openAISSEFrame(openaiChunk("chatcmpl-gemini", model, map[string]any{"role": "assistant"}, nil)))
	}
	if !finished {
		frames = append(frames, openAISSEFrame(openaiChunk("chatcmpl-gemini", model, map[string]any{}, "stop")))
	}
	frames = append(frames, []byte("data: [DONE]\n\n"))
	return JoinSSEFrames(frames)
}

func OpenAISSEToGeminiSSE(body []byte, model string) []byte {
	frames := make([][]byte, 0)
	for _, event := range parseSSEEvents(string(body)) {
		data := strings.TrimSpace(event.Data)
		if data == "" || data == "[DONE]" {
			continue
		}
		var payload map[string]any
		if json.Unmarshal([]byte(data), &payload) != nil {
			continue
		}
		choices, _ := payload["choices"].([]any)
		if len(choices) == 0 {
			continue
		}
		choice, _ := choices[0].(map[string]any)
		if choice == nil {
			continue
		}
		delta, _ := choice["delta"].(map[string]any)
		parts := []any{}
		if delta != nil {
			parts = append(parts, openAIContentToGeminiParts(delta["content"])...)
			parts = append(parts, openAIToolCallsToGeminiParts(delta["tool_calls"])...)
		}
		candidate := map[string]any{}
		if len(parts) > 0 {
			candidate["content"] = map[string]any{"role": "model", "parts": parts}
		}
		if reason := stringValue(choice["finish_reason"]); reason != "" {
			candidate["finishReason"] = openAIFinishToGemini(reason)
		}
		if len(candidate) == 0 {
			continue
		}
		out := map[string]any{"candidates": []any{candidate}, "modelVersion": model}
		encoded, err := json.Marshal(out)
		if err == nil {
			frames = append(frames, append([]byte("data: "), append(encoded, []byte("\n\n")...)...))
		}
	}
	return JoinSSEFrames(frames)
}

func geminiContent(role string, parts []any) map[string]any {
	if len(parts) == 0 {
		parts = []any{map[string]any{"text": ""}}
	}
	return map[string]any{"role": role, "parts": parts}
}

func openAIContentToGeminiParts(content any) []any {
	switch value := content.(type) {
	case string:
		return []any{map[string]any{"text": value}}
	case []any:
		out := make([]any, 0, len(value))
		for _, raw := range value {
			part, _ := raw.(map[string]any)
			if part == nil {
				continue
			}
			typ, _ := part["type"].(string)
			switch typ {
			case "text", "input_text":
				if text, ok := part["text"].(string); ok {
					out = append(out, map[string]any{"text": text})
				}
			case "image_url", "input_image":
				if image, ok := part["image_url"].(map[string]any); ok {
					if rawURL, ok := image["url"].(string); ok && rawURL != "" {
						out = append(out, map[string]any{"text": rawURL})
					}
				}
			default:
				if text, ok := part["text"].(string); ok {
					out = append(out, map[string]any{"text": text})
				}
			}
		}
		return out
	default:
		return []any{map[string]any{"text": contentToPlainText(content)}}
	}
}

func openAIToolCallsToGeminiParts(raw any) []any {
	calls, _ := raw.([]any)
	out := make([]any, 0, len(calls))
	for _, rawCall := range calls {
		call, _ := rawCall.(map[string]any)
		function, _ := call["function"].(map[string]any)
		if function == nil {
			continue
		}
		args := any(map[string]any{})
		if text, ok := function["arguments"].(string); ok && strings.TrimSpace(text) != "" {
			_ = json.Unmarshal([]byte(text), &args)
		}
		out = append(out, map[string]any{"functionCall": map[string]any{
			"name": function["name"], "args": args,
		}})
	}
	return out
}

func openAIToolsToGemini(raw any) []any {
	tools, _ := raw.([]any)
	out := make([]any, 0, len(tools))
	for _, rawTool := range tools {
		tool, _ := rawTool.(map[string]any)
		function, _ := tool["function"].(map[string]any)
		if function == nil {
			continue
		}
		out = append(out, map[string]any{
			"name": function["name"], "description": function["description"], "parameters": function["parameters"],
		})
	}
	return out
}

func geminiPartsToOpenAI(parts any) (any, []any, []any) {
	rawParts, _ := parts.([]any)
	content := make([]any, 0, len(rawParts))
	toolCalls := []any{}
	toolResponses := []any{}
	for i, raw := range rawParts {
		part, _ := raw.(map[string]any)
		if part == nil {
			continue
		}
		if text, ok := part["text"].(string); ok {
			content = append(content, map[string]any{"type": "text", "text": text})
		}
		if inline, ok := part["inlineData"].(map[string]any); ok {
			mime, _ := inline["mimeType"].(string)
			data, _ := inline["data"].(string)
			if mime != "" && data != "" {
				content = append(content, map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:" + mime + ";base64," + data}})
			}
		}
		if functionCall, ok := part["functionCall"].(map[string]any); ok {
			args, _ := json.Marshal(functionCall["args"])
			toolCalls = append(toolCalls, map[string]any{
				"id": "call_gemini_" + strconvItoa(i), "type": "function",
				"function": map[string]any{"name": functionCall["name"], "arguments": string(args)},
			})
		}
		if response, ok := part["functionResponse"].(map[string]any); ok {
			payload, _ := json.Marshal(response["response"])
			toolResponses = append(toolResponses, map[string]any{
				"role": "tool", "tool_call_id": response["name"], "name": response["name"], "content": string(payload),
			})
		}
	}
	if len(content) == 0 {
		return "", toolCalls, toolResponses
	}
	if len(content) == 1 {
		if item, ok := content[0].(map[string]any); ok && item["type"] == "text" {
			return item["text"], toolCalls, toolResponses
		}
	}
	return content, toolCalls, toolResponses
}

func geminiPartsToOpenAIContent(parts any) any {
	content, _, _ := geminiPartsToOpenAI(parts)
	return content
}

func geminiToolsToOpenAI(raw any) []any {
	tools, _ := raw.([]any)
	out := []any{}
	for _, rawTool := range tools {
		tool, _ := rawTool.(map[string]any)
		declarations, _ := tool["functionDeclarations"].([]any)
		for _, rawDeclaration := range declarations {
			declaration, _ := rawDeclaration.(map[string]any)
			if declaration == nil {
				continue
			}
			out = append(out, map[string]any{"type": "function", "function": map[string]any{
				"name": declaration["name"], "description": declaration["description"], "parameters": declaration["parameters"],
			}})
		}
	}
	return out
}

func geminiUsageToOpenAI(raw any) map[string]any {
	usage, _ := raw.(map[string]any)
	in := geminiMapInt(usage, "promptTokenCount")
	out := geminiMapInt(usage, "candidatesTokenCount")
	total := geminiMapInt(usage, "totalTokenCount")
	if total == 0 {
		total = in + out
	}
	return map[string]any{"prompt_tokens": in, "completion_tokens": out, "total_tokens": total}
}

func geminiFinishToOpenAI(reason string) string {
	switch strings.ToUpper(strings.TrimSpace(reason)) {
	case "MAX_TOKENS":
		return "length"
	case "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT":
		return "content_filter"
	case "STOP", "FINISH_REASON_UNSPECIFIED", "":
		return "stop"
	default:
		return "stop"
	}
}

func openAIFinishToGemini(reason string) string {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "length":
		return "MAX_TOKENS"
	case "content_filter":
		return "SAFETY"
	default:
		return "STOP"
	}
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func geminiMapInt(values map[string]any, keys ...string) int {
	for _, key := range keys {
		value, ok := values[key]
		if !ok {
			continue
		}
		switch n := value.(type) {
		case float64:
			return int(n)
		case int:
			return n
		case int64:
			return int(n)
		case json.Number:
			if parsed, err := n.Int64(); err == nil {
				return int(parsed)
			}
		}
	}
	return 0
}

func strconvItoa(v int) string {
	return fmt.Sprintf("%d", v)
}
