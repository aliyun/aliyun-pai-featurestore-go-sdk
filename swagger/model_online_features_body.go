package swagger

type OnlineFeaturesBody struct {
	JoinIds  *interface{} `json:"join_ids,omitempty"`
	Features []string     `json:"features,omitempty"`
	Alias    *interface{} `json:"alias,omitempty"`
}
