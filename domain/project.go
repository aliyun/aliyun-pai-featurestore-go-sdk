package domain

import (
	"fmt"
	"strconv"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/hologres"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/igraph"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/tablestore"
)

type Project struct {
	*api.Project
	OnlineStore      OnlineStore
	FeatureViewMap   map[string]FeatureView
	FeatureEntityMap map[string]*FeatureEntity
	ModelMap         map[string]*Model
	LabelTableMap    map[int]*LabelTable

	apiClient *api.APIClient
}

func NewProject(p *api.Project, isInitClient bool) *Project {
	project := Project{
		Project:          p,
		FeatureViewMap:   make(map[string]FeatureView),
		FeatureEntityMap: make(map[string]*FeatureEntity),
		ModelMap:         make(map[string]*Model),
		LabelTableMap:    make(map[int]*LabelTable),
	}

	switch p.OnlineDatasourceType {
	case constants.Datasource_Type_Hologres:
		onlineStore := &HologresOnlineStore{
			Datasource: p.OnlineDataSource,
		}
		if isInitClient {
			dsn := onlineStore.Datasource.GenerateDSN(constants.Datasource_Type_Hologres)
			hologres.RegisterHologres(onlineStore.Name, dsn)
		}
		project.OnlineStore = onlineStore
	case constants.Datasource_Type_IGraph:
		onlineStore := &IGraphOnlineStore{
			Datasource: p.OnlineDataSource,
		}

		if isInitClient {
			if p.OnlineDataSource.TestMode {
				client := igraph.NewGraphClient(p.OnlineDataSource.PublicAddress, p.OnlineDataSource.User, p.OnlineDataSource.Pwd)
				igraph.RegisterGraphClient(onlineStore.Name, client)
			} else {
				client := igraph.NewGraphClient(p.OnlineDataSource.VpcAddress, p.OnlineDataSource.User, p.OnlineDataSource.Pwd)
				igraph.RegisterGraphClient(onlineStore.Name, client)
			}
		}
		project.OnlineStore = onlineStore
	case constants.Datasource_Type_TableStore:
		onlineStore := &TableStoreOnlineStore{
			Datasource: p.OnlineDataSource,
		}

		if isInitClient {
			client := onlineStore.Datasource.NewTableStoreClient()
			tablestore.RegisterTableStoreClient(onlineStore.Name, client)
		}
		project.OnlineStore = onlineStore
	case constants.Datasource_Type_FeatureDB:
		onlineStore := &FeatureDBOnlineStore{
			Datasource: p.OnlineDataSource,
		}

		project.OnlineStore = onlineStore
	default:
		panic("not support onlinestore type")
	}

	if p.FeatureDBAddress != "" && p.FeatureDBToken != "" {
		featuredb.InitFeatureDBClient(p.FeatureDBAddress, p.FeatureDBToken, p.FeatureDBVpcAddress)
	}

	return &project
}

func (p *Project) SetApiClient(apiClient *api.APIClient) {
	p.apiClient = apiClient
}

func (p *Project) GetFeatureView(name string) FeatureView {
	if _, exists := p.FeatureViewMap[name]; !exists {
		err := p.loadFeatureView(name)
		if err != nil {
			return nil
		}
	}

	return p.FeatureViewMap[name]
}

func (p *Project) GetFeatureEntity(name string) *FeatureEntity {
	return p.FeatureEntityMap[name]
}

func (p *Project) GetLabelTable(labelTableId int) *LabelTable {
	if _, exists := p.LabelTableMap[labelTableId]; !exists {
		err := p.loadLabelTable(labelTableId)
		if err != nil {
			return nil
		}
	}

	return p.LabelTableMap[labelTableId]
}

func (p *Project) GetModel(name string) *Model {
	if _, exists := p.ModelMap[name]; !exists {
		err := p.loadModelFeature(name)
		if err != nil {
			return nil
		}
	}

	return p.ModelMap[name]
}
func (p *Project) GetModelFeature(name string) *Model {
	if _, exists := p.ModelMap[name]; !exists {
		err := p.loadModelFeature(name)
		if err != nil {
			return nil
		}
	}

	return p.ModelMap[name]
}

func (p *Project) loadFeatureView(featureViewName string) error {
	pageNumber := 1
	pageSize := 100
	for {
		listFeatureViews, err := p.apiClient.FeatureViewApi.ListFeatureViewsByName(int32(pageSize), int32(pageNumber), strconv.Itoa(p.ProjectId), featureViewName)
		if err != nil {
			fmt.Printf("list feature views error, err=%v", err)
			return err
		}
		for _, view := range listFeatureViews.FeatureViews {
			getFeatureViewResponse, err := p.apiClient.FeatureViewApi.GetFeatureViewByID(strconv.Itoa(int(view.FeatureViewId)))
			if err != nil {
				fmt.Printf("get feature view error, err=%v", err)
				return err
			}
			featureView := getFeatureViewResponse.FeatureView
			if featureView.RegisterDatasourceId > 0 {
				getDataSourceResponse, err := p.apiClient.DatasourceApi.DatasourceDatasourceIdGet(featureView.RegisterDatasourceId, 0, "")
				if err != nil {
					fmt.Printf("get datasource error, err=%v", err)
					return err
				}
				featureView.RegisterDataSource = getDataSourceResponse.Datasource
			}

			entity, exist := p.FeatureEntityMap[featureView.FeatureEntityName]
			if !exist {
				fmt.Printf("feature entity not exist, name=%s", featureView.FeatureEntityName)
				return fmt.Errorf("feature entity not exist, name=%s", featureView.FeatureEntityName)
			}
			featureViewDomain := NewFeatureView(featureView, p, entity)
			p.FeatureViewMap[featureView.Name] = featureViewDomain
		}

		if len(listFeatureViews.FeatureViews) == 0 || pageSize*pageNumber > listFeatureViews.TotalCount {
			break
		}

		pageNumber++
	}

	return nil
}

func (p *Project) loadLabelTable(labelTableId int) error {
	getLabelTableResponse, err := p.apiClient.LabelTableApi.GetLabelTableByID(strconv.Itoa(labelTableId))
	if err != nil {
		fmt.Printf("get label table error, err=%v", err)
		return err
	}
	labelTableDomain := NewLabelTable(getLabelTableResponse.LabelTable)
	p.LabelTableMap[labelTableId] = labelTableDomain

	return nil
}

func (p *Project) loadModelFeature(modelFeatureName string) error {
	pageNumber := 1
	pageSize := 100
	for {
		listModelFeatures, err := p.apiClient.FsModelApi.ListModelsByName(pageSize, pageNumber, strconv.Itoa(p.ProjectId), modelFeatureName)
		if err != nil {
			fmt.Printf("list model features error, err=%v", err)
			return err
		}
		for _, m := range listModelFeatures.Models {
			getModelFeatureResponse, err := p.apiClient.FsModelApi.GetModelByID(strconv.Itoa(m.ModelId))
			if err != nil {
				fmt.Printf("get model feature error, err=%v", err)
				return err
			}
			model := getModelFeatureResponse.Model
			var labelTableDomain *LabelTable
			if _, exists := p.LabelTableMap[model.LabelTableId]; !exists {
				err := p.loadLabelTable(model.LabelTableId)
				if err != nil {
					fmt.Printf("get label table error, err=%v", err)
					return err
				}
			}
			labelTableDomain = p.LabelTableMap[model.LabelTableId]
			modelDomain := NewModel(model, p, labelTableDomain)
			p.ModelMap[model.Name] = modelDomain
		}

		if len(listModelFeatures.Models) == 0 || pageSize*pageNumber > listModelFeatures.TotalCount {
			break
		}

		pageNumber++
	}

	return nil
}
