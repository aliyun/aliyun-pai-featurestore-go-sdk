package dao

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/utils"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/parser"
)

type FeatureViewDao interface {
	GetFeatures(keys []interface{}, selectFields []string) ([]map[string]interface{}, error)
	GetUserSequenceFeature(keys []interface{}, userIdField string, sequenceConfig api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) ([]map[string]interface{}, error)
	GetUserAggregatedSequenceFeature(keys []interface{}, userIdField string, sequenceConfig api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) (map[string]interface{}, error)
	GetUserBehaviorFeature(userIds []interface{}, events []interface{}, selectFields []string, sequenceConfig api.FeatureViewSeqConfig) ([]map[string]interface{}, error)
	RowCount(string) int
	RowCountIds(string) ([]string, int, error)
	ScanAndIterateData(filter string, ch chan<- string) ([]string, error)
}

type UnimplementedFeatureViewDao struct {
}

func (d *UnimplementedFeatureViewDao) GetFeatures(keys []interface{}, selectFields []string) ([]map[string]interface{}, error) {
	return nil, nil
}
func (d *UnimplementedFeatureViewDao) GetUserSequenceFeature(keys []interface{}, userIdField string, sequenceConfig api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) ([]map[string]interface{}, error) {
	return nil, nil
}
func (d *UnimplementedFeatureViewDao) GetUserAggregatedSequenceFeature(keys []interface{}, userIdField string, sequenceConfig api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) (map[string]interface{}, error) {
	return nil, nil
}
func (d *UnimplementedFeatureViewDao) GetUserBehaviorFeature(userIds []interface{}, events []interface{}, selectFields []string, sequenceConfig api.FeatureViewSeqConfig) ([]map[string]interface{}, error) {
	return nil, nil
}
func (d *UnimplementedFeatureViewDao) RowCount(string) int {
	return 0
}
func (d *UnimplementedFeatureViewDao) RowCountIds(string) ([]string, int, error) {
	return nil, 0, nil
}
func (d *UnimplementedFeatureViewDao) ScanAndIterateData(filter string, ch chan<- string) ([]string, error) {
	return nil, nil
}

func NewFeatureViewDao(config DaoConfig) FeatureViewDao {
	if config.DatasourceType == constants.Datasource_Type_Hologres {
		return NewFeatureViewHologresDao(config)
	} else if config.DatasourceType == constants.Datasource_Type_IGraph {
		return NewFeatureViewIGraphDao(config)
	} else if config.DatasourceType == constants.Datasource_Type_TableStore {
		return NewFeatureViewTableStoreDao(config)
	} else if config.DatasourceType == constants.Datasource_Type_FeatureDB {
		return NewFeatureViewFeatureDBDao(config)
	}

	panic("not found FeatureViewDao implement")
}

type sequenceInfo struct {
	itemId                        string
	event                         string
	playTime                      float64
	timestamp                     int64
	onlineBehaviourTableFieldsMap map[string]string
}

func makePlayTimeMap(playTimeFilter string) map[string]float64 {
	sequencePlayTimeMap := make(map[string]float64)
	if playTimeFilter != "" {
		playTimes := strings.Split(playTimeFilter, ";")
		for _, eventTime := range playTimes {
			strs := strings.Split(eventTime, ":")
			if len(strs) == 2 {
				if t, err := strconv.ParseFloat(strs[1], 64); err == nil {
					sequencePlayTimeMap[strs[0]] = t
				}
			}
		}
	}

	return sequencePlayTimeMap
}

func makeSequenceFeatures(offlineSequences, onlineSequences []*sequenceInfo, seqConfig *api.SeqConfig, sequenceConfig api.FeatureViewSeqConfig, currTime int64) map[string]interface{} {
	//combine offlineSequences and onlineSequences
	if len(offlineSequences) > 0 {
		index := 0
		for index < len(onlineSequences) {
			if onlineSequences[index].timestamp < offlineSequences[0].timestamp {
				break
			}
			index++
		}

		onlineSequences = onlineSequences[:index]
		onlineSequences = append(onlineSequences, offlineSequences...)
		if len(onlineSequences) > seqConfig.SeqLen {
			onlineSequences = onlineSequences[:seqConfig.SeqLen]
		}
	}

	//produce seqeunce feature correspond to easyrec processor
	sequencesValueMap := make(map[string][]string)
	sequenceMap := make(map[string]struct{})

	for _, seq := range onlineSequences {
		var key string
		switch sequenceConfig.DeduplicationMethodNum {
		case 1:
			key = fmt.Sprintf("%s#%s", seq.itemId, seq.event)
		case 2:
			key = fmt.Sprintf("%s#%s#%d", seq.itemId, seq.event, seq.timestamp)
		}
		if _, exist := sequenceMap[key]; !exist {
			sequenceMap[key] = struct{}{}
			sequencesValueMap[sequenceConfig.ItemIdField] = append(sequencesValueMap[sequenceConfig.ItemIdField], seq.itemId)
			sequencesValueMap[sequenceConfig.TimestampField] = append(sequencesValueMap[sequenceConfig.TimestampField], fmt.Sprintf("%d", seq.timestamp))
			sequencesValueMap[sequenceConfig.EventField] = append(sequencesValueMap[sequenceConfig.EventField], seq.event)
			if sequenceConfig.PlayTimeField != "" {
				sequencesValueMap[sequenceConfig.PlayTimeField] = append(sequencesValueMap[sequenceConfig.PlayTimeField], fmt.Sprintf("%.2f", seq.playTime))
			}
			sequencesValueMap["ts"] = append(sequencesValueMap["ts"], fmt.Sprintf("%d", currTime-seq.timestamp))
		}
	}

	properties := make(map[string]interface{})
	for key, value := range sequencesValueMap {
		curSequenceSubName := (seqConfig.OnlineSeqName + "__" + key)
		properties[curSequenceSubName] = strings.Join(value, ";")
	}
	properties[seqConfig.OnlineSeqName] = strings.Join(sequencesValueMap[sequenceConfig.ItemIdField], ";")

	return properties

}

func makeSequenceFeatures4FeatureDB(sequencesInfos []*sequenceInfo, seqConfig *api.SeqConfig, sequenceConfig api.FeatureViewSeqConfig, currTime int64) map[string]interface{} {
	//produce seqeunce feature correspond to easyrec processor
	sequencesValueMap := make(map[string][]string)

	for _, seq := range sequencesInfos {
		sequencesValueMap[sequenceConfig.ItemIdField] = append(sequencesValueMap[sequenceConfig.ItemIdField], seq.itemId)
		sequencesValueMap[sequenceConfig.TimestampField] = append(sequencesValueMap[sequenceConfig.TimestampField], fmt.Sprintf("%d", seq.timestamp))
		sequencesValueMap[sequenceConfig.EventField] = append(sequencesValueMap[sequenceConfig.EventField], seq.event)
		if sequenceConfig.PlayTimeField != "" {
			sequencesValueMap[sequenceConfig.PlayTimeField] = append(sequencesValueMap[sequenceConfig.PlayTimeField], fmt.Sprintf("%.2f", seq.playTime))
		}
		sequencesValueMap["ts"] = append(sequencesValueMap["ts"], fmt.Sprintf("%d", currTime-seq.timestamp))
		for _, behaviorField := range seqConfig.OnlineBehaviorTableFields {
			sequencesValueMap[behaviorField] = append(sequencesValueMap[behaviorField], seq.onlineBehaviourTableFieldsMap[behaviorField])
		}

	}

	properties := make(map[string]interface{})
	for key, value := range sequencesValueMap {
		curSequenceSubName := (seqConfig.OnlineSeqName + "__" + key)
		properties[curSequenceSubName] = strings.Join(value, ";")
	}
	properties[seqConfig.OnlineSeqName] = strings.Join(sequencesValueMap[sequenceConfig.ItemIdField], ";")

	return properties

}

func combineBehaviorFeatures(offlineBehaviorInfo, onlineBehaviorInfo []map[string]interface{}, timestampField string) []map[string]interface{} {
	// combine offline and online features
	if len(offlineBehaviorInfo) > 0 {
		index := 0
		for index < len(onlineBehaviorInfo) {
			if utils.ToInt64(onlineBehaviorInfo[index][timestampField], 0) < utils.ToInt64(offlineBehaviorInfo[0][timestampField], 0) {
				break
			}
			index++
		}

		onlineBehaviorInfo = onlineBehaviorInfo[:index]
		onlineBehaviorInfo = append(onlineBehaviorInfo, offlineBehaviorInfo...)
	}
	return onlineBehaviorInfo
}

// ExtractVariables 从 expr 表达式字符串中解析并提取所有变量名。
func ExtractVariables(code string) ([]string, error) {
	tree, err := parser.Parse(code)
	if err != nil {
		return nil, fmt.Errorf("failed to parse expression: %w", err)
	}

	variables := make(map[string]struct{})
	walk(tree.Node, variables)

	var result []string
	for v := range variables {
		result = append(result, v)
	}

	sort.Strings(result)

	return result, nil
}

// walk 是一个递归函数，用于遍历 AST 并提取变量名
func walk(node ast.Node, variables map[string]struct{}) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *ast.IdentifierNode:
		variables[n.Value] = struct{}{}

	case *ast.BinaryNode:
		walk(n.Left, variables)
		walk(n.Right, variables)

	case *ast.UnaryNode:
		walk(n.Node, variables)

	case *ast.MemberNode:
		walk(n.Node, variables)

	case *ast.CallNode:
		for _, arg := range n.Arguments {
			walk(arg, variables)
		}
		walk(n.Callee, variables)

	case *ast.ConditionalNode:
		walk(n.Cond, variables)
		walk(n.Exp1, variables)
		walk(n.Exp2, variables)

	case *ast.ArrayNode:
		for _, elem := range n.Nodes {
			walk(elem, variables)
		}

	case *ast.MapNode:
		for _, pair := range n.Pairs {
			walk(pair, variables)
		}

	case *ast.PairNode:
		walk(n.Key, variables)
		walk(n.Value, variables)

	case *ast.NilNode, *ast.IntegerNode, *ast.FloatNode, *ast.BoolNode, *ast.StringNode:
		// Do nothing

	default:
		log.Printf("unhandled node type: %T\n", n)
	}
}
