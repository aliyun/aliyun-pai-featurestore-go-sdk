package domain

import "context"

type FeatureViewOptions struct {
	Ctx               context.Context `json:"-"`
	DlrmHSTU          bool            `json:"dlrm_hstu"`
	SequenceDelimiter string          `json:"sequence_delimiter"`
	count             int
}

type ModelOptions struct {
	Ctx               context.Context `json:"-"`
	DlrmHSTU          bool            `json:"dlrm_hstu"`
	SequenceDelimiter string          `json:"sequence_delimiter"`
}
