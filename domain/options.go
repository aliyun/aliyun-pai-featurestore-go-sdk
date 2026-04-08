package domain

import "context"

type FeatureViewOptions struct {
	Ctx      context.Context
	DlrmHSTU bool
	count    int
}

type ModelOptions struct {
	Ctx      context.Context
	DlrmHSTU bool
}
