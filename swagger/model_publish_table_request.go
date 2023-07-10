package swagger

type PublishTableRequest struct {
	Partitions *interface{} `json:"partitions,omitempty"`
	Mode       string       `json:"mode,omitempty"`
}
