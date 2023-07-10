package swagger

type ModelsmodelIdonlinefeaturesFeatureSelectors struct {
	FeatureView   string       `json:"feature_view,omitempty"`
	FeatureEntity string       `json:"feature_entity,omitempty"`
	Features      []string     `json:"features,omitempty"`
	Alias         *interface{} `json:"alias,omitempty"`
}
