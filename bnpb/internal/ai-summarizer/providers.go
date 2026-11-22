package aisummarizer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/anthropics/anthropic-sdk-go/v2"
	"github.com/anthropics/anthropic-sdk-go/v2/option"
)

const (
	maxOutputTokens  = 400
	maxProviderBytes = 1 << 20
	openRouterURL    = "https://openrouter.ai/api/v1/chat/completions"
	geminiURL        = "https://generativelanguage.googleapis.com/v1beta/models/"
)

type provider interface {
	Name() string
	Model() string
	Complete(ctx context.Context, system, user string) (string, error)
}

// callError carries the provider's HTTP status so the chain can tell quota from a transient failure.
type callError struct {
	Status int
	Err    error
}

func (err *callError) Error() string { return fmt.Sprintf("status %d: %v", err.Status, err.Err) }
func (err *callError) Unwrap() error { return err.Err }

var errRefused = errors.New("provider declined to answer")

func newProvider(config providerConfig, client *http.Client) provider {
	switch config.Name {
	case "anthropic":
		return &anthropicProvider{model: config.Model, client: anthropic.NewClient(
			option.WithAPIKey(config.Key), option.WithHTTPClient(client), option.WithMaxRetries(0))}
	case "openrouter":
		return &openRouterProvider{model: config.Model, key: config.Key, url: openRouterURL, client: client}
	default:
		return &geminiProvider{model: config.Model, key: config.Key, url: geminiURL, client: client}
	}
}

type anthropicProvider struct {
	model  string
	client anthropic.Client
}

func (p *anthropicProvider) Name() string  { return "anthropic" }
func (p *anthropicProvider) Model() string { return p.model }

func (p *anthropicProvider) Complete(ctx context.Context, system, user string) (string, error) {
	response, err := p.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:        anthropic.Model(p.model),
		MaxTokens:    maxOutputTokens,
		System:       []anthropic.TextBlockParam{{Text: system}},
		Messages:     []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock(user))},
		OutputConfig: anthropic.OutputConfigParam{Effort: anthropic.OutputConfigEffortLow},
	})
	if err != nil {
		var apiErr *anthropic.Error
		if errors.As(err, &apiErr) {
			return "", &callError{Status: apiErr.StatusCode, Err: err}
		}
		return "", err
	}
	if response.StopReason == anthropic.StopReasonRefusal {
		return "", errRefused
	}
	var text strings.Builder
	for _, block := range response.Content {
		if variant, ok := block.AsAny().(anthropic.TextBlock); ok {
			text.WriteString(variant.Text)
		}
	}
	return text.String(), nil
}

type openRouterProvider struct {
	model, key, url string
	client          *http.Client
}

func (p *openRouterProvider) Name() string  { return "openrouter" }
func (p *openRouterProvider) Model() string { return p.model }

func (p *openRouterProvider) Complete(ctx context.Context, system, user string) (string, error) {
	body := map[string]any{
		"model":      p.model,
		"max_tokens": maxOutputTokens,
		"messages": []map[string]string{
			{"role": "system", "content": system},
			{"role": "user", "content": user},
		},
	}
	var reply struct {
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Message      struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	headers := map[string]string{"Authorization": "Bearer " + p.key}
	if err := postJSON(ctx, p.client, p.url, headers, body, &reply); err != nil {
		return "", err
	}
	if len(reply.Choices) == 0 {
		return "", errors.New("no choices in reply")
	}
	if reply.Choices[0].FinishReason == "content_filter" {
		return "", errRefused
	}
	return reply.Choices[0].Message.Content, nil
}

type geminiProvider struct {
	model, key, url string
	client          *http.Client
}

func (p *geminiProvider) Name() string  { return "gemini" }
func (p *geminiProvider) Model() string { return p.model }

func (p *geminiProvider) Complete(ctx context.Context, system, user string) (string, error) {
	body := map[string]any{
		"systemInstruction": map[string]any{"parts": []map[string]string{{"text": system}}},
		"contents":          []map[string]any{{"role": "user", "parts": []map[string]string{{"text": user}}}},
		"generationConfig":  map[string]any{"maxOutputTokens": maxOutputTokens},
	}
	var reply struct {
		Candidates []struct {
			FinishReason string `json:"finishReason"`
			Content      struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	headers := map[string]string{"x-goog-api-key": p.key}
	if err := postJSON(ctx, p.client, p.url+p.model+":generateContent", headers, body, &reply); err != nil {
		return "", err
	}
	if len(reply.Candidates) == 0 {
		return "", errRefused
	}
	candidate := reply.Candidates[0]
	if candidate.FinishReason == "SAFETY" || candidate.FinishReason == "PROHIBITED_CONTENT" {
		return "", errRefused
	}
	var text strings.Builder
	for _, part := range candidate.Content.Parts {
		text.WriteString(part.Text)
	}
	return text.String(), nil
}

func postJSON(ctx context.Context, client *http.Client, url string, headers map[string]string, body any, reply any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxProviderBytes))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		// The body can echo request details, so only its length is kept.
		return &callError{Status: response.StatusCode, Err: fmt.Errorf("%d-byte error body", len(payload))}
	}
	return json.Unmarshal(payload, reply)
}
