package swagger

type InlineResponse20083 struct {
	RequestId string                   `json:"request_id,omitempty"`
	Code      string                   `json:"code,omitempty"`
	Message   string                   `json:"message,omitempty"`
	Data      *InlineResponse20083Data `json:"data,omitempty"`
}
