package api

type ListLLMConfigsResponse struct {
	TotalCount int          `json:"total_count,omitempty"`
	LLMConfigs []*LLMConfig `json:"llm_configs,omitempty"`
}
