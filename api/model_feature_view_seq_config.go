package api

type FeatureViewSeqConfig struct {
	RegistrationMode              string       `json:"registration_mode"`
	ReferencedFeatureViewId       int          `json:"referenced_feature_view_id,omitempty"`
	ReferencedFeatureViewName     string       `json:"referenced_feature_view_name,omitempty"`
	ItemIdField                   string       `json:"item_id_field"`
	EventField                    string       `json:"event_field"`
	TimestampField                string       `json:"timestamp_field"`
	PlayTimeField                 string       `json:"play_time_field,omitempty"`
	PlayTimeFilter                string       `json:"play_time_filter,omitempty"`
	DeduplicationMethod           []string     `json:"deduplication_method"`
	DeduplicationMethodNum        int          `json:"-"`
	OfflineSeqTableName           string       `json:"offline_seq_table_name"`
	OfflineSeqTablePkField        string       `json:"offline_seq_table_pk_field"`
	OfflineSeqTableEventTimeField string       `json:"offline_seq_table_event_time_field"`
	OfflineSeqTablePartitionField string       `json:"offline_seq_table_partition_field"`
	SeqLenOnline                  int          `json:"seq_len_online"`
	SeqConfig                     []*SeqConfig `json:"seq_config"`
}

type SeqConfig struct {
	OfflineSeqName            string `json:"offline_seq_name"`
	SeqEvent                  string `json:"seq_event"`
	SeqLen                    int    `json:"seq_len"`
	OnlineSeqName             string `json:"online_seq_name"`
	OnlineBehaviorTableFields []string `json:"online_behavior_table_fields"`
}
