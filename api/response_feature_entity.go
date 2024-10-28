package api

type ListFeatureEntitiesResponse struct {
	TotalCount      int `json:"total_count"`
	FeatureEntities []*FeatureEntity
}

type GetFeatureEntityResponse struct {
	FeatureEntity *FeatureEntity
}
