package swagger

type Scene struct {
	SceneId   int64  `json:"scene_id,omitempty"`
	SceneName string `json:"scene_name"`
	SceneInfo string `json:"scene_info"`
}
