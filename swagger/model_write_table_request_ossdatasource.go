package swagger

type WriteTableRequestOssdatasource struct {
	AccessId   string `json:"access_id,omitempty"`
	AccessKey  string `json:"access_key,omitempty"`
	Endpoint   string `json:"endpoint,omitempty"`
	Path       string `json:"path,omitempty"`
	Delimiter  string `json:"delimiter,omitempty"`
	OmitHeader bool   `json:"omit_header,omitempty"`
}
