package domain

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/dao"
)

type SequenceFeatureView struct {
	*api.FeatureView
	Project                  *Project
	FeatureEntity            *FeatureEntity
	userIdField              string
	behaviorFields           []string
	sequenceConfig           api.FeatureViewSeqConfig
	featureViewDao           dao.FeatureViewDao
	offline_2_online_seq_map map[string]string
}

func NewSequenceFeatureView(view *api.FeatureView, p *Project, entity *FeatureEntity) *SequenceFeatureView {
	sequenceFeatureView := &SequenceFeatureView{
		FeatureView:   view,
		Project:       p,
		FeatureEntity: entity,
	}
	for _, field := range view.Fields {
		if field.IsPrimaryKey {
			sequenceFeatureView.userIdField = field.Name
			break
		}
	}

	err := json.Unmarshal([]byte(view.Config), &sequenceFeatureView.sequenceConfig)
	if err != nil {
		panic("sequence featureview config unmarshal failed")
	}

	if sequenceFeatureView.sequenceConfig.RegistrationMode == "" {
		sequenceFeatureView.sequenceConfig.RegistrationMode = constants.Seq_Registration_Mode_Full_Sequence
	}

	sequenceFeatureView.offline_2_online_seq_map = make(map[string]string, len(sequenceFeatureView.sequenceConfig.SeqConfig))
	for _, field := range view.Fields {
		if field.IsPartition {
			continue
		} else {
			sequenceFeatureView.behaviorFields = append(sequenceFeatureView.behaviorFields, field.Name)
		}
	}
	if sequenceFeatureView.sequenceConfig.RegistrationMode == constants.Seq_Registration_Mode_Full_Sequence {
		for _, seqConfig := range sequenceFeatureView.sequenceConfig.SeqConfig {
			sequenceFeatureView.offline_2_online_seq_map[seqConfig.OfflineSeqName] = seqConfig.OnlineSeqName
		}

		seen := make(map[string]bool)
		var uniqueSeqConfigs []*api.SeqConfig
		for _, seqConfig := range sequenceFeatureView.sequenceConfig.SeqConfig {
			if !seen[seqConfig.OnlineSeqName] {
				uniqueSeqConfigs = append(uniqueSeqConfigs, seqConfig)
				seen[seqConfig.OnlineSeqName] = true
			}
		}
		sequenceFeatureView.sequenceConfig.SeqConfig = uniqueSeqConfigs
	}

	requiredElements1 := []string{"user_id", "item_id", "event"}
	requiredElements2 := []string{"user_id", "item_id", "event", "timestamp"}
	if len(sequenceFeatureView.sequenceConfig.DeduplicationMethod) == len(requiredElements1) {
		for i, v := range sequenceFeatureView.sequenceConfig.DeduplicationMethod {
			if v != requiredElements1[i] {
				panic("deduplication_method invalid")
			}
		}
		sequenceFeatureView.sequenceConfig.DeduplicationMethodNum = 1
	} else if len(sequenceFeatureView.sequenceConfig.DeduplicationMethod) == len(requiredElements2) {
		for i, v := range sequenceFeatureView.sequenceConfig.DeduplicationMethod {
			if v != requiredElements2[i] {
				panic("deduplication_method invalid")
			}
		}
		sequenceFeatureView.sequenceConfig.DeduplicationMethodNum = 2
	} else {
		panic("deduplication_method invalid")
	}

	daoConfig := dao.DaoConfig{
		DatasourceType:  p.OnlineDatasourceType,
		PrimaryKeyField: sequenceFeatureView.userIdField,
	}

	if view.WriteToFeatureDB || p.OnlineDatasourceType == constants.Datasource_Type_FeatureDB {
		daoConfig.DatasourceType = constants.Datasource_Type_FeatureDB
		daoConfig.FeatureDBDatabaseName = p.InstanceId
		daoConfig.FeatureDBSchemaName = p.ProjectName
		if sequenceFeatureView.sequenceConfig.ReferencedFeatureViewId == 0 {
			daoConfig.FeatureDBTableName = sequenceFeatureView.Name
		} else {
			daoConfig.FeatureDBTableName = sequenceFeatureView.sequenceConfig.ReferencedFeatureViewName
		}
		daoConfig.FeatureDBSignature = p.Signature

		fieldTypeMap := make(map[string]constants.FSType, len(view.Fields))
		for _, field := range view.Fields {
			if field.IsPartition {
				continue
			} else {
				fieldTypeMap[field.Name] = field.Type
			}
		}
		daoConfig.FieldTypeMap = fieldTypeMap
		daoConfig.Fields = sequenceFeatureView.behaviorFields
	} else {
		if sequenceFeatureView.sequenceConfig.ReferencedFeatureViewId == 0 {
			switch p.OnlineDatasourceType {
			case constants.Datasource_Type_Hologres:
				daoConfig.HologresName = p.OnlineStore.GetDatasourceName()
				daoConfig.HologresOfflineTableName = p.OnlineStore.GetSeqOfflineTableName(sequenceFeatureView)
				daoConfig.HologresOnlineTableName = p.OnlineStore.GetSeqOnlineTableName(sequenceFeatureView)
			case constants.Datasource_Type_TableStore:
				daoConfig.TableStoreName = p.OnlineStore.GetDatasourceName()
				daoConfig.TableStoreOfflineTableName = p.OnlineStore.GetSeqOfflineTableName(sequenceFeatureView)
				daoConfig.TableStoreOnlineTableName = p.OnlineStore.GetSeqOnlineTableName(sequenceFeatureView)

			case constants.Datasource_Type_IGraph:
				daoConfig.SaveOriginalField = true
				daoConfig.IGraphName = p.OnlineStore.GetDatasourceName()
				daoConfig.GroupName = p.ProjectName
				daoConfig.IgraphEdgeName = p.OnlineStore.GetSeqOnlineTableName(sequenceFeatureView)

				fieldTypeMap := make(map[string]constants.FSType, len(view.Fields))
				for _, field := range view.Fields {
					if field.IsPartition {
						continue
					} else {
						fieldTypeMap[field.Name] = field.Type
					}
				}
				daoConfig.FieldTypeMap = fieldTypeMap

			default:

			}
		} else {
			referencedFeatureView := p.GetFeatureView(sequenceFeatureView.sequenceConfig.ReferencedFeatureViewName)
			if referencedFeatureView == nil {
				panic(fmt.Sprintf("referenced feature view :%s not found", sequenceFeatureView.sequenceConfig.ReferencedFeatureViewName))
			}
			if referencedFeatureView.GetType() != constants.Feature_View_Type_Sequence {
				panic(fmt.Sprintf("referenced feature view :%s is not sequence feature view", sequenceFeatureView.sequenceConfig.ReferencedFeatureViewName))
			}
			referencedSeqFeatureView := referencedFeatureView.(*SequenceFeatureView)
			switch p.OnlineDatasourceType {
			case constants.Datasource_Type_Hologres:
				daoConfig.HologresName = p.OnlineStore.GetDatasourceName()
				daoConfig.HologresOfflineTableName = p.OnlineStore.GetSeqOfflineTableName(referencedSeqFeatureView)
				daoConfig.HologresOnlineTableName = p.OnlineStore.GetSeqOnlineTableName(referencedSeqFeatureView)
			case constants.Datasource_Type_TableStore:
				daoConfig.TableStoreName = p.OnlineStore.GetDatasourceName()
				daoConfig.TableStoreOfflineTableName = p.OnlineStore.GetSeqOfflineTableName(referencedSeqFeatureView)
				daoConfig.TableStoreOnlineTableName = p.OnlineStore.GetSeqOnlineTableName(referencedSeqFeatureView)

			case constants.Datasource_Type_IGraph:
				daoConfig.SaveOriginalField = true
				daoConfig.IGraphName = p.OnlineStore.GetDatasourceName()
				daoConfig.GroupName = p.ProjectName
				daoConfig.IgraphEdgeName = p.OnlineStore.GetSeqOnlineTableName(referencedSeqFeatureView)

				fieldTypeMap := make(map[string]constants.FSType, len(view.Fields))
				for _, field := range view.Fields {
					if field.IsPartition {
						continue
					} else {
						fieldTypeMap[field.Name] = field.Type
					}
				}
				daoConfig.FieldTypeMap = fieldTypeMap

			default:

			}
		}
	}

	featureViewDao := dao.NewFeatureViewDao(daoConfig)
	sequenceFeatureView.featureViewDao = featureViewDao

	return sequenceFeatureView
}

func (f *SequenceFeatureView) GetOnlineFeatures(joinIds []interface{}, features []string, alias map[string]string) ([]map[string]interface{}, error) {
	if f.sequenceConfig.RegistrationMode == constants.Seq_Registration_Mode_Only_Behavior {
		return nil, errors.New("only full_sequence registration mode supports GetOnlineFeatures, please use GetBehaviorFeatures")
	}
	sequenceConfig := f.sequenceConfig
	onlineConfig := []*api.SeqConfig{}
	seenFields := make(map[string]bool)

	for _, feature := range features {
		if feature == "*" {
			onlineConfig = sequenceConfig.SeqConfig
			break
		} else {
			found := false
			for _, seqConfig := range sequenceConfig.SeqConfig {
				if seqConfig.OnlineSeqName == feature {
					found = true
					if !seenFields[feature] {
						onlineConfig = append(onlineConfig, seqConfig)
						seenFields[feature] = true
					}
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("sequence feature name :%s not found in feature view config", feature)
			}
		}
	}

	sequenceFeatureResults, err := f.featureViewDao.GetUserSequenceFeature(joinIds, f.userIdField, sequenceConfig, onlineConfig)

	if f.userIdField != f.FeatureEntity.FeatureEntityJoinid {
		for _, sequencefeatureMap := range sequenceFeatureResults {
			sequencefeatureMap[f.FeatureEntity.FeatureEntityJoinid] = sequencefeatureMap[f.userIdField]
			delete(sequencefeatureMap, f.userIdField)
		}
	}

	return sequenceFeatureResults, err
}

func (f *SequenceFeatureView) GetBehaviorFeatures(userIds []interface{}, events []interface{}, features []string) ([]map[string]interface{}, error) {
	var selectFields []string
	seenFields := make(map[string]bool)

	for _, feature := range features {
		if feature == "*" {
			selectFields = append(selectFields, f.behaviorFields...)
			break
		} else {
			if seenFields[feature] {
				continue
			}
			found := false
			for _, field := range f.behaviorFields {
				if field == feature {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("behavior feature name :%s not found in feature view config", feature)
			}

			selectFields = append(selectFields, feature)
			seenFields[feature] = true
		}
	}

	behaviorFeatureResult, err := f.featureViewDao.GetUserBehaviorFeature(userIds, events, selectFields, f.sequenceConfig)

	if f.userIdField != f.FeatureEntity.FeatureEntityJoinid {
		for _, behaviorFeatureMap := range behaviorFeatureResult {
			behaviorFeatureMap[f.FeatureEntity.FeatureEntityJoinid] = behaviorFeatureMap[f.userIdField]
			delete(behaviorFeatureMap, f.userIdField)
		}
	}

	return behaviorFeatureResult, err
}

func (f *SequenceFeatureView) GetName() string {
	return f.Name
}

func (f *SequenceFeatureView) GetFeatureEntityName() string {
	return f.FeatureEntityName
}

func (f *SequenceFeatureView) GetType() string {
	return f.Type
}

func (f *SequenceFeatureView) Offline2Online(input string) string {
	if f.sequenceConfig.RegistrationMode == constants.Seq_Registration_Mode_Only_Behavior {
		return input
	}
	return f.offline_2_online_seq_map[input]
}

func (f *SequenceFeatureView) GetFields() []api.FeatureViewFields {
	fields := make([]api.FeatureViewFields, len(f.Fields))
	for i, field := range f.Fields {
		if field != nil {
			fields[i] = *field
		}
	}
	return fields
}

func (f *SequenceFeatureView) GetIsWriteToFeatureDB() bool {
	return f.WriteToFeatureDB || f.Project.OnlineDatasourceType == constants.Datasource_Type_FeatureDB
}

func (f *SequenceFeatureView) GetTTL() int {
	return f.Ttl
}

func (f *SequenceFeatureView) RowCount(string) int {
	return 0
}
func (f *SequenceFeatureView) RowCountIds(string) ([]string, int, error) {
	return nil, 0, nil
}

// ScanAndIterateData implements FeatureView.
func (f *SequenceFeatureView) ScanAndIterateData(filter string, ch chan<- string) ([]string, error) {
	return nil, errors.New("unimplemented")
}
