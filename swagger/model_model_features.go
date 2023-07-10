package swagger

type ModelFeatures struct {
	FeatureViewId   int32  `json:"feature_view_id,omitempty"`
	FeatureViewName string `json:"feature_view_name,omitempty"`
	Name            string `json:"name,omitempty"`
	AliasName       string `json:"alias_name,omitempty"`
	Type_           int32  `json:"type,omitempty"`
	TypeStr         string `json:"type_str,omitempty"`
}
