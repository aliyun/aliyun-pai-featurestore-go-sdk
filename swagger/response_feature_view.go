package swagger

type ListFeatureViewsResponse struct {
	BaseResponse
	Data ListFeatureViewsResponseData `json:"data,omitempty"`
}
type ListFeatureViewsResponseData struct {
	TotalCount   int            `json:"total_count"`
	FeatureViews []*FeatureView `json:"feature_views"`
}

type GetFeatureViewResponse struct {
	BaseResponse
	Data map[string][]FeatureView `json:"data,omitempty"`
}
