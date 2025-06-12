package api

import "github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"

type LabelTableField struct {
	Name             string           `json:"name"`
	Type             constants.FSType `json:"type"`
	IsPartition      bool             `json:"is_partition,omitempty"`
	IsFeatureField   bool             `json:"is_feature_field,omitempty"`
	IsFgReserveField bool             `json:"is_fg_reserve_field,omitempty"`
	IsEventTime      bool             `json:"is_event_time,omitempty"`
	IsLabelField     bool             `json:"is_label_field,omitempty"`
	Position         int              `json:"position,omitempty"`
}
