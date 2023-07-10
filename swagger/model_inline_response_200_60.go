package swagger

type InlineResponse20060 struct {
	RequestId string                   `json:"request_id,omitempty"`
	Code      string                   `json:"code,omitempty"`
	Message   string                   `json:"message,omitempty"`
	Data      *InlineResponse20060Data `json:"data,omitempty"`
}
