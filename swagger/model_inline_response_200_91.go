package swagger

type InlineResponse20091 struct {
	RequestId string                   `json:"request_id,omitempty"`
	Code      string                   `json:"code,omitempty"`
	Message   string                   `json:"message,omitempty"`
	Data      *InlineResponse20091Data `json:"data,omitempty"`
}
