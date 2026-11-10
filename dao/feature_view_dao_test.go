package dao

import (
	"reflect"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
)

func TestSequenceDelimiter(t *testing.T) {
	sequences := []*sequenceInfo{
		{itemId: "a", event: "click", timestamp: 1000, playTime: 1.5, customFieldValue: "s1", onlineBehaviourTableFieldsMap: map[string]string{"category": "c1"}},
		{itemId: "b", event: "buy", timestamp: 900, playTime: 2.5, customFieldValue: "s2", onlineBehaviourTableFieldsMap: map[string]string{"category": "c2"}},
	}
	seqConfig := &api.SeqConfig{OnlineSeqName: "seq", SeqLen: 2, OnlineBehaviorTableFields: []string{"category"}}
	for _, mode := range []string{"standard", "featuredb", "dlrm_hstu"} {
		for _, delimiter := range []string{"", ",", "::"} {
			t.Run(mode+"/"+delimiter, func(t *testing.T) {
				config := api.FeatureViewSeqConfig{
					ItemIdField: "item_id", EventField: "event", TimestampField: "timestamp", PlayTimeField: "play_time",
					DeduplicationMethodNum: 1, CustomDeduplicationField: "session_id", SequenceDelimiter: delimiter,
				}
				sep := delimiter
				if sep == "" {
					sep = ";"
				}
				want := map[string]interface{}{
					"seq":            strings.Join([]string{"a", "b"}, sep),
					"seq__item_id":   strings.Join([]string{"a", "b"}, sep),
					"seq__timestamp": strings.Join([]string{"1000", "900"}, sep),
					"seq__play_time": strings.Join([]string{"1.50", "2.50"}, sep),
					"seq__ts":        strings.Join([]string{"100", "200"}, sep),
					"seq__event":     strings.Join([]string{"click", "buy"}, sep),
				}
				var got map[string]interface{}
				switch mode {
				case "standard":
					got = makeSequenceFeatures(nil, sequences, seqConfig, config, 1100)
				case "featuredb":
					want["seq__category"] = strings.Join([]string{"c1", "c2"}, sep)
					got = makeSequenceFeatures4FeatureDB(sequences, seqConfig, config, 1100)
				case "dlrm_hstu":
					want["seq__category"] = strings.Join([]string{"c1", "c2"}, sep)
					want["seq__session_id"] = strings.Join([]string{"s1", "s2"}, sep)
					want["seq__event"] = strings.Join([]string{"click|view", "buy"}, sep)
					input := append([]*sequenceInfo{}, sequences...)
					input = append(input, &sequenceInfo{itemId: "a", event: "view", timestamp: 950, customFieldValue: "s1"})
					got = makeSequenceFeatures4DlrmHSTU(input, seqConfig, config, 1100, 2)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %#v, want %#v", got, want)
				}
			})
		}
	}
}
