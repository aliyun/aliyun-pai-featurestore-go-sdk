package swagger

type TableMetaField struct {
	Name           string `json:"name"`
	Meaning        string `json:"meaning"`
	Type_          string `json:"type"`
	DimensionField bool   `json:"dimension_field"`
}
