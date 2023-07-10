package swagger

type FeatureEntity struct {
	FeatureEntityId     int32  `json:"feature_entity_id,omitempty"`
	ProjectId           int64  `json:"project_id"`
	ProjectName         string `json:"project_name,omitempty"`
	FeatureEntityName   string `json:"feature_entity_name"`
	FeatureEntityJoinid string `json:"feature_entity_joinid"`
}
