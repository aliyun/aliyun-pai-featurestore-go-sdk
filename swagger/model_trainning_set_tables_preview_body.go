package swagger

type TrainningSetTablesPreviewBody struct {
	ProjectId            int32                                     `json:"project_id,omitempty"`
	ModelName            string                                    `json:"model_name,omitempty"`
	LabelDatasourceTable string                                    `json:"label_datasource_table,omitempty"`
	Features             []ModelstrainningSetTablespreviewFeatures `json:"features,omitempty"`
}
