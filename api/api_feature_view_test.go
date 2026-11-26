package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/domain"
)

func TestDynamicEmbeddingFieldMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typeOf string
		want   constants.FSType
		marked bool
	}{
		{"float", "ARRAY<FLOAT>", constants.FS_ARRAY_FLOAT, true},
		{"int8", "ARRAY<INT8>", constants.FS_ARRAY_INT8, true},
		{"uint8", "ARRAY<UINT8>", constants.FS_ARRAY_UINT8, true},
		{"legacy", "ARRAY<FLOAT>", constants.FS_ARRAY_FLOAT, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := []api.FeatureViewFields{
				{Name: "primary_key", Type: constants.FS_STRING, IsPrimaryKey: true, Position: 1},
				{Name: "sub_key", Type: constants.FS_INT64, IsSubKey: true, Position: 2},
				{Name: "embedding", Type: tc.want, IsEmbedding: tc.marked, Dimension: 8, Position: 3},
				{Name: "score", Type: constants.FS_INT64, Position: 4},
				{Name: "count", Type: constants.FS_INT32, Position: 5},
				{Name: "weight", Type: constants.FS_FLOAT, Position: 6},
				{Name: "ratio", Type: constants.FS_DOUBLE, Position: 7},
				{Name: "label", Type: constants.FS_STRING, Position: 8},
			}
			types := []string{"STRING", "INT64", tc.typeOf, "INT64", "INT32", "FLOAT", "DOUBLE", "STRING"}
			if !tc.marked {
				want = want[:3]
			}
			fields := make([]map[string]interface{}, len(want))
			for i, field := range want {
				attrs := []string{}
				if field.IsPrimaryKey {
					attrs = append(attrs, "PrimaryKey")
				}
				if field.IsSubKey {
					attrs = append(attrs, "SubKey")
				}
				if field.IsEmbedding {
					attrs = append(attrs, "Embedding")
				}
				fields[i] = map[string]interface{}{"Name": field.Name, "Type": types[i], "Attributes": attrs, "Dimension": field.Dimension}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(map[string]interface{}{
					"ProjectName": "project", "ProjectId": "1", "FeatureEntityName": "entity", "FeatureEntityId": "2",
					"Name": "view", "Type": "DynamicEmbedding", "SyncOnlineTable": true, "TTL": 0, "Config": "{}", "Fields": fields,
				}); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			cfg := api.NewConfiguration("cn-test", "test-id", "test-secret", "", "project")
			cfg.SetDomain(strings.TrimPrefix(server.URL, "http://"))
			client, err := api.NewAPIClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			client.Protocol = tea.String("http")
			response, err := client.FeatureViewApi.GetFeatureViewByID("3")
			if err != nil {
				t.Fatal(err)
			}
			got := domain.NewDynamicEmbeddingFeatureView(response.FeatureView, nil, nil).GetFields()
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("fields = %#v, want %#v", got, want)
			}
			data, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var serialized []map[string]interface{}
			if err := json.Unmarshal(data, &serialized); err != nil {
				t.Fatal(err)
			}
			for i, field := range serialized {
				value, present := field["is_embedding"]
				if present != want[i].IsEmbedding || (present && value != true) {
					t.Errorf("field %s: is_embedding = %v, present = %v", want[i].Name, value, present)
				}
			}
		})
	}
}
