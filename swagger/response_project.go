package swagger

type ListProjectsResponse struct {
	BaseResponse
	Data map[string][]*Project `json:"data,omitempty"`
}
