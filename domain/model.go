package domain

import (
	"fmt"
	"sync"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/utils"
)

type Model struct {
	*api.Model
	project                 *Project
	featureViewMap          map[string]FeatureView
	featureEntityMap        map[string]*FeatureEntity
	featureNamesMap         map[string][]string               // featureview : feature names
	aliasNamesMap           map[string]map[string]string      // featureview : alias names
	featureEntityJoinIdMap  map[string]map[string]FeatureView // feature entity joinid : featureviews
	featureEntityJoinIdList []string                          // slice of root feature entity joinids
	rootJoinIdSet           map[string]bool                   // set of root feature entity joinids
	childEntitiesMap        map[string][]string               // parent joinid : children's joinid
	labelTable              *LabelTable
}

func NewModel(model *api.Model, p *Project, lt *LabelTable) *Model {
	m := &Model{
		Model:                  model,
		project:                p,
		labelTable:             lt,
		featureViewMap:         make(map[string]FeatureView),
		featureEntityMap:       make(map[string]*FeatureEntity),
		featureNamesMap:        make(map[string][]string),
		aliasNamesMap:          make(map[string]map[string]string),
		featureEntityJoinIdMap: make(map[string]map[string]FeatureView),
		rootJoinIdSet:          make(map[string]bool),
		childEntitiesMap:       make(map[string][]string),
	}

	for _, feature := range m.Features {
		featureView := m.project.GetFeatureView(feature.FeatureViewName)

		featureEntity := m.project.GetFeatureEntity(featureView.GetFeatureEntityName())
		m.featureViewMap[feature.FeatureViewName] = featureView
		m.featureEntityMap[featureView.GetFeatureEntityName()] = featureEntity
		m.featureNamesMap[feature.FeatureViewName] = append(m.featureNamesMap[feature.FeatureViewName], featureView.Offline2Online(feature.Name))

		if feature.AliasName != "" {
			aliasMap, ok := m.aliasNamesMap[feature.FeatureViewName]
			if !ok {
				aliasMap = make(map[string]string)
			}
			aliasMap[feature.Name] = feature.AliasName
			m.aliasNamesMap[feature.FeatureViewName] = aliasMap
		}
		featureViewMap, ok := m.featureEntityJoinIdMap[featureEntity.FeatureEntityJoinid]
		if !ok {
			featureViewMap = make(map[string]FeatureView)
		}
		featureViewMap[feature.FeatureViewName] = featureView
		m.featureEntityJoinIdMap[featureEntity.FeatureEntityJoinid] = featureViewMap

	}

	for _, entity := range m.featureEntityMap {
		if entity.ParentFeatureEntityId == 0 {
			m.featureEntityJoinIdList = append(m.featureEntityJoinIdList, entity.FeatureEntityJoinid)
			m.rootJoinIdSet[entity.FeatureEntityJoinid] = true
		} else {
			m.childEntitiesMap[entity.ParentJoinId] = append(m.childEntitiesMap[entity.ParentJoinId], entity.FeatureEntityJoinid)
		}
	}

	return m
}

func (m *Model) GetOnlineFeatures(joinIds map[string][]interface{}) ([]map[string]interface{}, error) {

	size := -1
	for _, joinid := range m.featureEntityJoinIdList {
		keys, ok := joinIds[joinid]
		if !ok {
			return nil, fmt.Errorf("join id:%s not found", joinid)
		}
		if size == -1 {
			size = len(keys)
		} else {
			if size != len(keys) {
				return nil, fmt.Errorf("join id:%s length not equal", joinid)
			}
		}
	}

	var mu sync.Mutex

	var wg sync.WaitGroup
	joinIdFeaturesMap := make(map[string][]map[string]interface{})
	// read features of root entities
	for _, rootJoinId := range m.featureEntityJoinIdList {
		keys := joinIds[rootJoinId]
		featureViewMap := m.featureEntityJoinIdMap[rootJoinId]

		for _, featureView := range featureViewMap {
			wg.Add(1)
			go func(featureView FeatureView, joinId string, keys []interface{}) {
				defer wg.Done()
				features, err := featureView.GetOnlineFeatures(keys, m.featureNamesMap[featureView.GetName()], m.aliasNamesMap[featureView.GetName()])
				if err != nil {
					fmt.Println(err)
				}

				mu.Lock()
				joinIdFeaturesMap[joinId] = append(joinIdFeaturesMap[joinId], features...)
				mu.Unlock()

			}(featureView, rootJoinId, keys)
		}
	}
	wg.Wait()

	// read keys of child entities with deduplication
	childJoinIdKeys := make(map[string][]interface{})
	childJoinIdKeySet := make(map[string]map[string]struct{})
	for _, rootJoinId := range m.featureEntityJoinIdList {
		if children, ok := m.childEntitiesMap[rootJoinId]; ok {
			for _, row := range joinIdFeaturesMap[rootJoinId] {
				for _, childJoinId := range children {
					if val, exists := row[childJoinId]; exists && val != nil {
						valStr := utils.ToString(val, "")
						if valStr == "" {
							continue
						}
						if childJoinIdKeySet[childJoinId] == nil {
							childJoinIdKeySet[childJoinId] = make(map[string]struct{})
						}
						if _, seen := childJoinIdKeySet[childJoinId][valStr]; !seen {
							childJoinIdKeySet[childJoinId][valStr] = struct{}{}
							childJoinIdKeys[childJoinId] = append(childJoinIdKeys[childJoinId], val)
						}
					}
				}
			}
		}
	}

	// read features of child entities
	if len(childJoinIdKeys) > 0 {
		var childWg sync.WaitGroup
		for childJoinId, keys := range childJoinIdKeys {
			if featureViewMap, exists := m.featureEntityJoinIdMap[childJoinId]; exists {
				for _, featureView := range featureViewMap {
					childWg.Add(1)
					go func(featureView FeatureView, joinId string, keys []interface{}) {
						defer childWg.Done()
						features, err := featureView.GetOnlineFeatures(keys, m.featureNamesMap[featureView.GetName()], m.aliasNamesMap[featureView.GetName()])
						if err != nil {
							fmt.Println(err)
						}

						mu.Lock()
						joinIdFeaturesMap[joinId] = append(joinIdFeaturesMap[joinId], features...)
						mu.Unlock()
					}(featureView, childJoinId, keys)
				}
			}
		}
		childWg.Wait()
	}

	featuresResult := make([]map[string]interface{}, size)
	for i := 0; i < size; i++ {
		featuresResult[i] = make(map[string]interface{})
	}
	// merge features of root entities
	rootKeyToIndex := make(map[string]map[string]int)
	for _, joinId := range m.featureEntityJoinIdList {
		idxMap := make(map[string]int)
		for idx, key := range joinIds[joinId] {
			idxMap[utils.ToString(key, "")] = idx
		}
		rootKeyToIndex[joinId] = idxMap
	}
	for _, rootJoinId := range m.featureEntityJoinIdList {
		keyMap := rootKeyToIndex[rootJoinId]
		for _, row := range joinIdFeaturesMap[rootJoinId] {
			if joinIdVal, ok := row[rootJoinId]; ok {
				joinIdValStr := utils.ToString(joinIdVal, "")
				if idx, exists := keyMap[joinIdValStr]; exists {
					for k, v := range row {
						featuresResult[idx][k] = v
					}
				}
			}
		}
	}
	if len(m.childEntitiesMap) == 0 {
		return featuresResult, nil
	}

	// merge features of child entities
	childFeatureIndex := make(map[string]map[string]map[string]interface{}) // join id : key : row values
	for childJoinId, rows := range joinIdFeaturesMap {
		if m.rootJoinIdSet[childJoinId] {
			continue
		}

		keyToRow := make(map[string]map[string]interface{})
		for _, row := range rows {
			if val, ok := row[childJoinId]; ok {
				valStr := utils.ToString(val, "")
				if valStr == "" {
					continue
				}
				if keyToRow[valStr] == nil {
					keyToRow[valStr] = make(map[string]interface{})
				}
				for k, v := range row {
					keyToRow[valStr][k] = v
				}
			}
		}
		childFeatureIndex[childJoinId] = keyToRow
	}
	for _, rootJoinId := range m.featureEntityJoinIdList {
		if children, ok := m.childEntitiesMap[rootJoinId]; ok {
			for _, childJoinId := range children {
				if childIndex, ok := childFeatureIndex[childJoinId]; ok {
					for i := 0; i < size; i++ {
						if rootChildVal, exists := featuresResult[i][childJoinId]; exists && rootChildVal != nil {
							keyStr := utils.ToString(rootChildVal, "")
							if keyStr == "" {
								continue
							}
							if childRow, found := childIndex[keyStr]; found {
								for k, v := range childRow {
									featuresResult[i][k] = v
								}
							}
						}
					}
				}
			}
		}
	}

	return featuresResult, nil
}

func (m *Model) GetOnlineFeaturesWithEntity(joinIds map[string][]interface{}, featureEntityName string) ([]map[string]interface{}, error) {
	featureEntity, ok := m.featureEntityMap[featureEntityName]
	if !ok {
		return nil, fmt.Errorf("feature entity name:%s not found", featureEntityName)
	}
	joinId := featureEntity.FeatureEntityJoinid
	keys, ok := joinIds[joinId]
	if !ok {
		return nil, fmt.Errorf("join id:%s not found", joinId)
	}

	size := len(keys)

	var wg sync.WaitGroup
	joinIdFeaturesMap := make(map[string][]map[string]interface{})
	featureViewMap := m.featureEntityJoinIdMap[joinId]

	var mu sync.Mutex

	for _, featureView := range featureViewMap {
		wg.Add(1)
		go func(featureView FeatureView, joinId string, keys []interface{}) {
			defer wg.Done()
			features, err := featureView.GetOnlineFeatures(keys, m.featureNamesMap[featureView.GetName()], m.aliasNamesMap[featureView.GetName()])
			if err != nil {
				fmt.Println(err)
			}
			mu.Lock()
			joinIdFeaturesMap[joinId] = append(joinIdFeaturesMap[joinId], features...)
			mu.Unlock()

		}(featureView, featureEntity.FeatureEntityJoinid, joinIds[featureEntity.FeatureEntityJoinid])
	}
	wg.Wait()

	featuresResult := make([]map[string]interface{}, size)
	for i := 0; i < size; i++ {
		featuresResult[i] = make(map[string]interface{})
	}
	keyToIdx := make(map[string]int)
	for idx, key := range keys {
		keyToIdx[utils.ToString(key, "")] = idx
	}
	for _, row := range joinIdFeaturesMap[joinId] {
		if joinIdVal, ok := row[joinId]; ok {
			joinIdValStr := utils.ToString(joinIdVal, "")
			if idx, exists := keyToIdx[joinIdValStr]; exists {
				for k, v := range row {
					featuresResult[idx][k] = v
				}
			}
		}
	}

	// get features of child entities if exist
	if children := m.childEntitiesMap[joinId]; len(children) > 0 {
		// read keys of child entities with deduplication
		childJoinIdKeys := make(map[string][]interface{})
		childJoinIdKeySet := make(map[string]map[string]struct{})
		for _, row := range joinIdFeaturesMap[joinId] {
			for _, childJoinId := range children {
				if val, exists := row[childJoinId]; exists && val != nil {
					valStr := utils.ToString(val, "")
					if valStr == "" {
						continue
					}
					if childJoinIdKeySet[childJoinId] == nil {
						childJoinIdKeySet[childJoinId] = make(map[string]struct{})
					}
					if _, seen := childJoinIdKeySet[childJoinId][valStr]; !seen {
						childJoinIdKeySet[childJoinId][valStr] = struct{}{}
						childJoinIdKeys[childJoinId] = append(childJoinIdKeys[childJoinId], val)
					}
				}
			}
		}

		// read features of child entities
		if len(childJoinIdKeys) > 0 {
			var childWg sync.WaitGroup
			for childJoinId, keys := range childJoinIdKeys {
				if featureViewMap, exists := m.featureEntityJoinIdMap[childJoinId]; exists {
					for _, featureView := range featureViewMap {
						childWg.Add(1)
						go func(featureView FeatureView, joinId string, keys []interface{}) {
							defer childWg.Done()
							features, err := featureView.GetOnlineFeatures(keys, m.featureNamesMap[featureView.GetName()], m.aliasNamesMap[featureView.GetName()])
							if err != nil {
								fmt.Println(err)
							}

							mu.Lock()
							joinIdFeaturesMap[joinId] = append(joinIdFeaturesMap[joinId], features...)
							mu.Unlock()
						}(featureView, childJoinId, keys)
					}
				}
			}
			childWg.Wait()
		}

		// merge features of child entities
		childFeatureIndex := make(map[string]map[string]map[string]interface{}) // join id : key : row values
		for childJoinId, rows := range joinIdFeaturesMap {
			if m.rootJoinIdSet[childJoinId] {
				continue
			}

			keyToRow := make(map[string]map[string]interface{})
			for _, row := range rows {
				if val, ok := row[childJoinId]; ok {
					valStr := utils.ToString(val, "")
					if valStr == "" {
						continue
					}
					if keyToRow[valStr] == nil {
						keyToRow[valStr] = make(map[string]interface{})
					}
					for k, v := range row {
						keyToRow[valStr][k] = v
					}
				}
			}
			childFeatureIndex[childJoinId] = keyToRow
		}
		for _, childJoinId := range children {
			if childIndex, ok := childFeatureIndex[childJoinId]; ok {
				for i := 0; i < size; i++ {
					if rootChildVal, exists := featuresResult[i][childJoinId]; exists && rootChildVal != nil {
						keyStr := utils.ToString(rootChildVal, "")
						if keyStr == "" {
							continue
						}
						if childRow, found := childIndex[keyStr]; found {
							for k, v := range childRow {
								featuresResult[i][k] = v
							}
						}
					}
				}
			}
		}
	}

	return featuresResult, nil
}

func (m *Model) GetLabelPriorityLevel() int {
	return m.LabelPriorityLevel
}

func (m *Model) GetLabelTable() *LabelTable {
	return m.labelTable
}
