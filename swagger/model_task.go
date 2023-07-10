package swagger

type Task struct {
	TaskId            int64  `json:"task_id,omitempty"`
	ProjectId         int64  `json:"project_id"`
	TaskName          string `json:"task_name,omitempty"`
	TaskNameStr       string `json:"task_name_str,omitempty"`
	TaskSubject       string `json:"task_subject,omitempty"`
	TaskSubjectType   string `json:"task_subject_type,omitempty"`
	State             int32  `json:"state,omitempty"`
	CreateTime        string `json:"create_time,omitempty"`
	ExecuteTime       string `json:"execute_time,omitempty"`
	FinishTime        string `json:"finish_time,omitempty"`
	TaskConfig        string `json:"task_config,omitempty"`
	TaskRunningConfig string `json:"task_running_config,omitempty"`
	TaskLog           string `json:"task_log,omitempty"`
}
