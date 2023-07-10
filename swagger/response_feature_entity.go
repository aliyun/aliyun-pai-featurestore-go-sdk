package swagger

type ListFeatureEntitiesResponse struct {
	BaseResponse
	Data map[string][]FeatureEntity `json:"data,omitempty"`
}

type GetFeatureEntityResponse struct {
	BaseResponse
	Data map[string][]FeatureEntity `json:"data,omitempty"`
}
