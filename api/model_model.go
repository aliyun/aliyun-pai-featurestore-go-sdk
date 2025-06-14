package api

type Model struct {
	ModelId              int              `json:"model_id"`
	ProjectId            int              `json:"project_id"`
	ProjectName          string           `json:"project_name,omitempty"`
	Name                 string           `json:"name"`
	Owner                string           `json:"owner"`
	LabelTableId         int              `json:"label_table_id"`
	LabelDatasourceId    int              `json:"label_datasource_id,omitempty"`
	LabelDatasourceTable string           `json:"label_datasource_table"`
	LabelEventTime       string           `json:"label_event_time"`
	TrainningSetTable    string           `json:"trainning_set_table"`
	Features             []*ModelFeatures `json:"features"`
	LabelPriorityLevel   int              `json:"label_priority_level"`
	//Relations            *ModelRelations  `json:"relations,omitempty"`
}
