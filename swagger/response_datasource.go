package swagger

type GetDatasourceResponse struct {
	RequestId string                 `json:"request_id,omitempty"`
	Code      string                 `json:"code,omitempty"`
	Message   string                 `json:"message,omitempty"`
	Data      map[string]*Datasource `json:"data,omitempty"`
}
