package swagger

type WriteTableRequest struct {
	Partitions    *interface{}                    `json:"partitions,omitempty"`
	Ossdatasource *WriteTableRequestOssdatasource `json:"ossdatasource,omitempty"`
	Mode          string                          `json:"mode,omitempty"`
}
