package api

type LLMConfig struct {
	LLMConfigId int    `json:"llm_config_id"`
	Name        string `json:"name"`
	Model       string `json:"model,omitempty"`
	ModelType   string `json:"model_type,omitempty"`
	BaseUrl     string `json:"base_url,omitempty"`
	MaxTokens   int    `json:"max_tokens,omitempty"`
	BatchSize   int    `json:"batch_size,omitempty"`
	Rps         int    `json:"rps,omitempty"`
	WorkspaceId string `json:"workspace_id,omitempty"`
}
