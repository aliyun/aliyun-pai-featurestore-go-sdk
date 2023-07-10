package swagger

type ModelRelations struct {
	Domains *interface{}          `json:"domains,omitempty"`
	Links   []ModelRelationsLinks `json:"links,omitempty"`
}
