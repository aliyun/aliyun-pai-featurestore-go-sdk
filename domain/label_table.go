package domain

import "github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"

type LabelTable struct {
	*api.LabelTable
}

func NewLabelTable(labelTable *api.LabelTable) *LabelTable {
	labelTableDomain := &LabelTable{
		LabelTable: labelTable,
	}

	return labelTableDomain
}

func (l *LabelTable) GetFields() []*api.LabelTableField {
	return l.Fields
}

func (l *LabelTable) GetFeatureNames() []string {
	var featureNames []string
	for _, field := range l.Fields {
		if field.IsPartition {
			continue
		}
		featureNames = append(featureNames, field.Name)
	}

	return featureNames
}
