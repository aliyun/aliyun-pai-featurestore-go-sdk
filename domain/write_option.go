package domain

// WriteOptions controls per-call behavior of FeatureView.WriteFeatures.
//
// Options are immutable from the caller's perspective and only consumed by
// the FeatureView implementation; they exist primarily so that new write
// modes (such as the synchronous /write_direct path) can be added without
// breaking the public WriteFeatures signature.
type WriteOptions struct {
	// Direct routes the call to the FeatureDB /write_direct endpoint.
	//
	// Semantics differ from the default async path:
	//   - the request is sent immediately and returns synchronously;
	//   - the in-memory ticker buffer is bypassed entirely;
	//   - only KV-typed feature tables backed by FeatureDB are supported;
	//   - only the FullRowWrite mode is accepted (PartialFieldWrite is rejected).
	// Non-FeatureDB DAOs return an error when this option is set.
	Direct bool

	// InsertMode overrides the default write mode for the legacy /write
	// path. Valid values are constants.FullRowWrite (default) and
	// constants.PartialFieldWrite. When combined with Direct, only
	// FullRowWrite is permitted.
	InsertMode string
}

// WriteOption configures a single FeatureView.WriteFeatures invocation.
type WriteOption func(*WriteOptions)

// WithDirect routes the WriteFeatures call to FeatureDB's synchronous
// /write_direct endpoint, bypassing the datahub-backed pipeline used by
// the default /write path. Suitable for low-latency writes against KV
// tables when the caller needs the per-row success/error result inline.
func WithDirect() WriteOption {
	return func(o *WriteOptions) { o.Direct = true }
}

// WithInsertMode sets the FeatureDB write_mode for this call.
// Use constants.FullRowWrite (default) or constants.PartialFieldWrite.
// Combining WithInsertMode(PartialFieldWrite) with WithDirect returns
// an error because /write_direct only supports FullRowWrite.
func WithInsertMode(mode string) WriteOption {
	return func(o *WriteOptions) { o.InsertMode = mode }
}

// resolveWriteOptions folds a slice of WriteOption into a WriteOptions value.
func resolveWriteOptions(opts []WriteOption) WriteOptions {
	var o WriteOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}
