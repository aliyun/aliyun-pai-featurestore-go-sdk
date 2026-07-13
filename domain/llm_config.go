package domain

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb"
)

const (
	LLMModelTypeTextEmbedding       = "TEXT_EMBEDDING"
	LLMModelTypeMultiModalEmbedding = "MULTI_MODAL_EMBEDDING"
)

type LLMConfig struct {
	*api.LLMConfig
	instanceId string
	signature  string
}

func NewLLMConfig(cfg *api.LLMConfig, instanceId, signature string) *LLMConfig {
	return &LLMConfig{
		LLMConfig:  cfg,
		instanceId: instanceId,
		signature:  signature,
	}
}

type MultiModalContent struct {
	Text        string   `json:"text,omitempty"`
	Image       string   `json:"image,omitempty"`
	Video       string   `json:"video,omitempty"`
	MultiImages []string `json:"multi_images,omitempty"`
}

func (l *LLMConfig) CreateTextEmbeddings(ctx context.Context, input []string) ([][]float32, error) {
	if l.ModelType != LLMModelTypeTextEmbedding {
		return nil, fmt.Errorf("llm config %q is %s, not a text embedding config; use the matching method", l.Name, l.ModelType)
	}
	if len(input) == 0 {
		return nil, fmt.Errorf("input is empty")
	}
	body := map[string]interface{}{
		"input": input,
	}
	return l.doEmbeddings(ctx, "text_embeddings", body)
}

func (l *LLMConfig) CreateMultiModalEmbeddings(ctx context.Context, input []MultiModalContent) ([][]float32, error) {
	if l.ModelType != LLMModelTypeMultiModalEmbedding {
		return nil, fmt.Errorf("llm config %q is %s, not a multi-modal embedding config; use the matching method", l.Name, l.ModelType)
	}
	if len(input) == 0 {
		return nil, fmt.Errorf("input is empty")
	}
	body := map[string]interface{}{
		"multi_modal_input": input,
	}
	return l.doEmbeddings(ctx, "multi_modal_embeddings", body)
}

func (l *LLMConfig) doEmbeddings(ctx context.Context, endpoint string, body map[string]interface{}) ([][]float32, error) {
	fdbClient, err := featuredb.GetFeatureDBClient()
	if err != nil {
		return nil, err
	}
	if l.signature == "" {
		return nil, fmt.Errorf("featuredb signature is empty, please set it with WithFeatureDBLogin")
	}

	reqBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/api/v1/llm_configs/%s/%d/%s", l.instanceId, l.LLMConfigId, endpoint)

	doRequest := func(address string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, address+path, bytes.NewReader(reqBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fdbClient.Token)
		req.Header.Set("Auth", l.signature)
		return fdbClient.Client.Do(req)
	}

	response, err := doRequest(fdbClient.GetCurrentAddress(false))
	if err != nil {
		if response != nil {
			response.Body.Close()
		}
		response, err = doRequest(fdbClient.GetCurrentAddress(true))
		if err != nil {
			if response != nil {
				response.Body.Close()
			}
			return nil, err
		}
	}
	defer response.Body.Close()

	bodyBytes, _ := io.ReadAll(response.Body)
	var resp struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Data    struct {
			Embeddings [][]float32 `json:"embeddings"`
		} `json:"data"`
	}
	if err := json.Unmarshal(bodyBytes, &resp); err != nil {
		return nil, fmt.Errorf("decode response failed (status %d): %v", response.StatusCode, err)
	}
	if resp.Code != "OK" {
		return nil, fmt.Errorf("llm embeddings failed, code: %s, message: %s", resp.Code, resp.Message)
	}
	return resp.Data.Embeddings, nil
}
