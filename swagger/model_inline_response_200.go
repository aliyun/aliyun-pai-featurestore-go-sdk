package swagger

type InlineResponse200 struct {
	RequestId string       `json:"request_id,omitempty"`
	Code      string       `json:"code,omitempty"`
	Message   string       `json:"message,omitempty"`
	Data      *interface{} `json:"data,omitempty"`
}
