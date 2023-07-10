package swagger

type FeatureView struct {
	FeatureViewId     int64               `json:"feature_view_id"`
	ProjectId         int64               `json:"project_id"`
	ProjectName       string              `json:"project_name,omitempty"`
	Name              string              `json:"name,omitempty"`
	FeatureEntityId   int32               `json:"feature_entity_id,omitempty"`
	FeatureEntityName string              `json:"feature_entity_name,omitempty"`
	Owner             string              `json:"owner"`
	Type_             string              `json:"type"`
	Online            bool                `json:"online"`
	IsRegister        bool                `json:"is_register"`
	RegisterTable     string              `json:"register_table"`
	Ttl               int32               `json:"ttl"`
	Tags              []string            `json:"tags"`
	Config            string              `json:"config"`
	Fields            []FeatureViewFields `json:"fields"`
}
