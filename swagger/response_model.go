package swagger

type ListModelsResponse struct {
	BaseResponse
	Data *ListModelsResponseData `json:"data,omitempty"`
}

type ListModelsResponseData struct {
	TotalCount int32   `json:"total_count,omitempty"`
	Models     []Model `json:"models,omitempty"`
}

type GetModelResponse struct {
	BaseResponse
	Data *GetModelResponseData `json:"data,omitempty"`
}
type GetModelResponseData struct {
	Models    []Model         `json:"models,omitempty"`
	Relations *ModelRelations `json:"relations,omitempty"`
}
