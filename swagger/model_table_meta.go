package swagger

type TableMeta struct {
	TableMetaId int64            `json:"table_meta_id"`
	Name        string           `json:"name"`
	Datasource  string           `json:"datasource"`
	Dsn         string           `json:"dsn"`
	Table       string           `json:"table"`
	Module      string           `json:"module"`
	Fields      []TableMetaField `json:"fields"`
}
