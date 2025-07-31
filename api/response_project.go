package api

type ListProjectsResponse struct {
	TotalCount int
	Projects   []*Project
}
