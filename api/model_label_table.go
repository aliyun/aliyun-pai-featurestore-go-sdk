package api

type LabelTable struct {
	LabelTableId   int                `json:"label_table_id"`
	Name           string             `json:"name"`
	ProjectId      int                `json:"project_id"`
	ProjectName    string             `json:"project_name,omitempty"`
	DatasourceId   int                `json:"datasource_id"`
	DatasourceName string             `json:"datasource_name,omitempty"`
	Owner          string             `json:"owner"`
	Fields         []*LabelTableField `json:"fields"`
}
