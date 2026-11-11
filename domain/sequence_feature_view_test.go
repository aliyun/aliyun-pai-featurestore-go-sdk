package domain

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/dao"
)

type sequenceRecordingDao struct {
	dao.UnimplementedFeatureViewDao
	config api.FeatureViewSeqConfig
	ctx    context.Context
	keys   []interface{}
}

func (d *sequenceRecordingDao) GetUserSequenceFeatureWithContext(ctx context.Context, keys []interface{}, userIdField string, config api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) ([]map[string]interface{}, error) {
	d.ctx, d.config, d.keys = ctx, config, keys
	return []map[string]interface{}{{userIdField: keys[0]}}, nil
}

func (d *sequenceRecordingDao) GetUserAggregatedSequenceFeatureWithContext(ctx context.Context, keys []interface{}, userIdField string, config api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) (map[string]interface{}, error) {
	d.ctx, d.config, d.keys = ctx, config, keys
	return map[string]interface{}{userIdField: keys[0]}, nil
}

func (d *sequenceRecordingDao) GetFeaturesWithContext(ctx context.Context, keys []interface{}, fields []string, weight int) ([]map[string]interface{}, error) {
	return []map[string]interface{}{{"user_id": keys[0], "child_id": "child"}}, nil
}

func TestSequenceFeatureViewReadOptions(t *testing.T) {
	for _, aggregated := range []bool{false, true} {
		for _, delimiter := range []string{"", ","} {
			name := "normal/" + delimiter
			if aggregated {
				name = "aggregated/" + delimiter
			}
			t.Run(name, func(t *testing.T) {
				d := &sequenceRecordingDao{}
				f := &SequenceFeatureView{
					FeatureEntity: &FeatureEntity{FeatureEntity: &api.FeatureEntity{FeatureEntityJoinid: "user_id"}},
					userIdField:   "user_id", featureViewDao: d,
					sequenceConfig: api.FeatureViewSeqConfig{SeqConfig: []*api.SeqConfig{{OnlineSeqName: "seq"}}},
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				keys := []interface{}{"a", "b"}
				opts := FeatureViewOptions{Ctx: ctx, SequenceDelimiter: delimiter}
				var err error
				if aggregated {
					_, err = f.GetOnlineAggregatedFeaturesWithOptions(keys, []string{"seq"}, nil, opts)
				} else {
					_, err = f.GetOnlineFeaturesWithOptions(keys, []string{"seq"}, nil, opts)
				}
				if err != nil {
					t.Fatal(err)
				}
				if d.ctx != ctx || d.config.SequenceDelimiter != delimiter || !reflect.DeepEqual(d.keys, keys) {
					t.Fatalf("options or keys not propagated: config=%+v, keys=%v", d.config, d.keys)
				}
				if f.sequenceConfig.SequenceDelimiter != "" {
					t.Fatal("shared sequence config was modified")
				}

				if aggregated {
					_, err = f.GetOnlineAggregatedFeatures(keys, []string{"seq"}, nil)
				} else {
					_, err = f.GetOnlineFeatures(keys, []string{"seq"}, nil)
				}
				if err != nil || d.config.SequenceDelimiter != "" {
					t.Fatalf("legacy call should use default delimiter: config=%+v, err=%v", d.config, err)
				}

				cancel()
				if aggregated {
					_, err = f.GetOnlineAggregatedFeaturesWithContext(ctx, keys, []string{"seq"}, nil)
				} else {
					_, err = f.GetOnlineFeaturesWithContext(ctx, keys, []string{"seq"}, nil)
				}
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("got %v, want context.Canceled", err)
				}
			})
		}
	}
}

func TestModelSequenceDelimiter(t *testing.T) {
	for _, method := range []string{"normal", "entity", "aggregated"} {
		for _, child := range []bool{false, true} {
			name := method + "/root"
			if child {
				name = method + "/child"
			}
			t.Run(name, func(t *testing.T) {
				d := &sequenceRecordingDao{}
				entity := &FeatureEntity{FeatureEntity: &api.FeatureEntity{FeatureEntityJoinid: "user_id"}}
				f := &SequenceFeatureView{
					FeatureView:   &api.FeatureView{Name: "seq_view", Type: constants.Feature_View_Type_Sequence},
					FeatureEntity: entity, userIdField: "user_id", featureViewDao: d,
					sequenceConfig: api.FeatureViewSeqConfig{SeqConfig: []*api.SeqConfig{{OnlineSeqName: "seq"}}},
				}
				m := &Model{
					Model:                   &api.Model{},
					featureEntityMap:        map[string]*FeatureEntity{"user": entity},
					featureEntityJoinIdList: []string{"user_id"},
					featureEntityJoinIdMap:  map[string]map[string]FeatureView{"user_id": {"seq_view": f}},
					featureNamesMap:         map[string][]string{"seq_view": {"seq"}},
					rootJoinIdSet:           map[string]bool{"user_id": true},
				}
				wantKeys := []interface{}{"a"}
				if child {
					f.userIdField = "child_id"
					f.FeatureEntity = &FeatureEntity{FeatureEntity: &api.FeatureEntity{FeatureEntityJoinid: "child_id"}}
					parent := &BaseFeatureView{
						FeatureView: &api.FeatureView{Name: "parent"}, FeatureEntity: entity,
						primaryKeyField: api.FeatureViewFields{Name: "user_id"}, featureFields: []string{"child_id"},
						featureViewDao: &sequenceRecordingDao{},
					}
					m.featureEntityJoinIdMap["user_id"] = map[string]FeatureView{"parent": parent}
					m.featureEntityJoinIdMap["child_id"] = map[string]FeatureView{"seq_view": f}
					m.featureNamesMap["parent"] = []string{"child_id"}
					m.childEntitiesMap = map[string][]string{"user_id": {"child_id"}}
					wantKeys = []interface{}{"child"}
				} else if method == "aggregated" {
					wantKeys = []interface{}{"a", "b"}
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				opts := ModelOptions{Ctx: ctx, SequenceDelimiter: "::"}
				keys := map[string][]interface{}{"user_id": {"a"}}
				var err error
				switch method {
				case "normal":
					_, err = m.GetOnlineFeaturesWithOptions(keys, opts)
				case "entity":
					_, err = m.GetOnlineFeaturesWithEntityWithOptions(keys, "user", opts)
				case "aggregated":
					_, err = m.GetOnlineFeaturesWithAggregatedSequenceWithOptions("a", []interface{}{"a", "b"}, "user", opts)
				}
				if err != nil {
					t.Fatal(err)
				}
				if d.ctx != ctx || d.config.SequenceDelimiter != "::" || !reflect.DeepEqual(d.keys, wantKeys) {
					t.Fatalf("options or keys not propagated: config=%+v, keys=%v, want keys=%v", d.config, d.keys, wantKeys)
				}
				if method == "aggregated" {
					_, err = m.GetOnlineFeaturesWithAggregatedSequence("a", []interface{}{"a", "b"}, "user")
					if err != nil || d.config.SequenceDelimiter != "" {
						t.Fatalf("legacy aggregate should use default delimiter: config=%+v, err=%v", d.config, err)
					}
					_, err = m.GetOnlineFeaturesWithAggregatedSequenceWithContext(ctx, "a", []interface{}{"a", "b"}, "user")
					if err != nil || d.ctx != ctx {
						t.Fatalf("legacy aggregate should preserve context: err=%v", err)
					}
					cancel()
					_, err = m.GetOnlineFeaturesWithAggregatedSequenceWithContext(ctx, "a", []interface{}{"a", "b"}, "user")
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("got %v, want context.Canceled", err)
					}
				}
			})
		}
	}
}
