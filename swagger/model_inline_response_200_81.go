package swagger

type InlineResponse20081 struct {
	RequestId string                   `json:"request_id,omitempty"`
	Code      string                   `json:"code,omitempty"`
	Message   string                   `json:"message,omitempty"`
	Data      *InlineResponse20081Data `json:"data,omitempty"`
}
