package api

import "github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"

type FeatureViewFields struct {
	Name         string           `json:"name,omitempty"`
	Type         constants.FSType `json:"type,omitempty"`
	IsPartition  bool             `json:"is_partition,omitempty"`
	IsPrimaryKey bool             `json:"is_primary_key,omitempty"`
	IsEventTime  bool             `json:"is_event_time,omitempty"`
	Position     int              `json:"position,omitempty"`
}
