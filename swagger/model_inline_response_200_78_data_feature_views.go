package swagger

type InlineResponse20078DataFeatureViews struct {
	FeatureViewId   string                            `json:"feature_view_id,omitempty"`
	FeatureViewName string                            `json:"feature_view_name,omitempty"`
	Features        []InlineResponse20078DataFeatures `json:"features,omitempty"`
}
