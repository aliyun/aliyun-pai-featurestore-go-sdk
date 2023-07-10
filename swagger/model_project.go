package swagger

type Project struct {
	ProjectId             int64  `json:"project_id,omitempty"`
	ProjectName           string `json:"project_name"`
	Description           string `json:"description"`
	OfflineDatasourceType string `json:"offline_datasource_type"`
	OfflineDatasourceId   int64  `json:"offline_datasource_id"`
	OnlineDatasourceType  string `json:"online_datasource_type"`
	OnlineDatasourceId    int64  `json:"online_datasource_id"`
	OfflineLifecycle      int32  `json:"offline_lifecycle"`
	FeatureEntityCount    int32  `json:"feature_entity_count,omitempty"`
	FeatureViewCount      int32  `json:"feature_view_count,omitempty"`
	ModelCount            int32  `json:"model_count,omitempty"`

	OfflineDataSource *Datasource `json:"-"`
	OnlineDataSource  *Datasource `json:"-"`
}
