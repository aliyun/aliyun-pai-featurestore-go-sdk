package domain

import "context"

type FeatureViewOptions struct {
	Ctx      context.Context `json:"-"`
	DlrmHSTU bool            `json:"dlrm_hstu"`
	// HardSearch warms up sequences that have filter_field instead of returning them.
	HardSearch bool `json:"hard_search"`
	count      int
}

type ModelOptions struct {
	Ctx      context.Context `json:"-"`
	DlrmHSTU bool            `json:"dlrm_hstu"`
	// HardSearch warms up sequences that have filter_field instead of returning them.
	HardSearch bool `json:"hard_search"`
}
