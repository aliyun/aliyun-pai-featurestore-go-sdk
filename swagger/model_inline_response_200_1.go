package swagger

type InlineResponse2001 struct {
	RequestId string                  `json:"request_id,omitempty"`
	Code      string                  `json:"code,omitempty"`
	Message   string                  `json:"message,omitempty"`
	Data      *InlineResponse2001Data `json:"data,omitempty"`
}
