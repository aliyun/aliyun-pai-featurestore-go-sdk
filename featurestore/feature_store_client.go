package featurestore

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/domain"
)

type ClientOption func(c *FeatureStoreClient)

func WithLogger(l Logger) ClientOption {
	return func(e *FeatureStoreClient) {
		e.Logger = l
	}
}

func WithErrorLogger(l Logger) ClientOption {
	return func(e *FeatureStoreClient) {
		e.ErrorLogger = l
	}
}

// WithDomain set custom domain
func WithDomain(domian string) ClientOption {
	return func(e *FeatureStoreClient) {
		e.domain = domian
	}
}

func WithLoopData(loopLoad bool) ClientOption {
	return func(e *FeatureStoreClient) {
		e.loopLoadData = loopLoad
	}
}

func WithNoDatasourceInitClient() ClientOption {
	return func(e *FeatureStoreClient) {
		e.datasourceInitClient = false
	}

}

func WithTestMode() ClientOption {
	return func(e *FeatureStoreClient) {
		e.testMode = true
	}
}

func WithFeatureDBLogin(username, password string) ClientOption {
	return func(e *FeatureStoreClient) {
		auth := username + ":" + password
		signature := base64.StdEncoding.EncodeToString([]byte(auth))

		e.signature = signature
	}
}

func WithHologresLogin(username, password string) ClientOption {
	return func(e *FeatureStoreClient) {
		e.hologresAuth = username + ":" + password
	}
}

func WithHologresPublicAddress(hologresPublicAddress string) ClientOption {
	return func(e *FeatureStoreClient) {
		e.hologresPublicAddress = hologresPublicAddress
	}
}

func WithHologresPort(port int) ClientOption {
	return func(e *FeatureStoreClient) {
		e.hologresPort = port
	}
}

func WithToken(token string) ClientOption {
	return func(e *FeatureStoreClient) {
		e.token = token
	}
}

func WithHologresPrefix(hologresPrefix string) ClientOption {
	return func(e *FeatureStoreClient) {
		e.hologresPrefix = hologresPrefix
	}
}

type FeatureStoreClient struct {
	// loopLoadData flag to invoke loopLoadProjectData  function
	loopLoadData bool

	// datasourceInitClient flag to init onlinestore  client
	datasourceInitClient bool

	domain string

	client *api.APIClient

	projectMap map[string]*domain.Project

	// Logger specifies a logger used to report internal changes within the writer
	Logger Logger

	// ErrorLogger is the logger to report errors
	ErrorLogger Logger

	// testMode to get features by public address
	testMode bool

	// signature to get data from featurestore db
	signature string

	// hologres authorization info
	hologresAuth string

	// custom hologres public address (including port num)
	hologresPublicAddress string

	// hologres port number, default 80
	hologresPort int

	// sts token
	token string

	// hologres prefix for sts token
	hologresPrefix string

	// stopChan to stop loopLoadProjectData
	stopChan chan struct{}
}

func NewFeatureStoreClient(regionId, accessKeyId, accessKeySecret, projectName string, opts ...ClientOption) (fsclient *FeatureStoreClient, err error) {
	defer func() {
		if r := recover(); r != nil {
			//err = fmt.Errorf("error: %v", r)
		}
	}()
	client := FeatureStoreClient{
		projectMap:           make(map[string]*domain.Project, 0),
		loopLoadData:         true,
		datasourceInitClient: true,
		hologresPort:         80,
		stopChan:             make(chan struct{}),
	}

	for _, opt := range opts {
		opt(&client)
	}

	cfg := api.NewConfiguration(regionId, accessKeyId, accessKeySecret, client.token, projectName)

	if client.testMode {
		cfg.SetDomain(fmt.Sprintf("paifeaturestore.%s.aliyuncs.com", regionId))
	}
	if client.domain != "" {
		cfg.SetDomain(client.domain)
	}

	apiClient, err := api.NewAPIClient(cfg)
	if err != nil {
		return nil, err
	}

	client.client = apiClient

	if err = client.Validate(); err != nil {
		return nil, err
	}

	if err = client.LoadProjectData(); err != nil {
		return nil, err
	}

	if client.loopLoadData {
		go client.loopLoadProjectData()
	}

	return &client, nil
}

// Validate check the  FeatureStoreClient value
func (e *FeatureStoreClient) Validate() error {
	// check instance
	if err := e.client.InstanceApi.GetInstance(); err != nil {
		return err
	}

	return nil
}

func (c *FeatureStoreClient) GetProject(name string) (*domain.Project, error) {
	project, ok := c.projectMap[name]
	if ok {
		return project, nil
	}

	return nil, fmt.Errorf("not found project, name:%s", name)
}

func (c *FeatureStoreClient) logError(err error) {
	if c.ErrorLogger != nil {
		c.ErrorLogger.Printf(err.Error())
		return
	}

	if c.Logger != nil {
		c.Logger.Printf(err.Error())
	}
}

// LoadProjectData specifies a function to load data from featurestore server
func (c *FeatureStoreClient) LoadProjectData() error {
	ak := api.Ak{
		AccesskeyId:     c.client.GetConfig().AccessKeyId,
		AccesskeySecret: c.client.GetConfig().AccessKeySecret,
	}
	if c.client.GetConfig().Token != "" {
		ak.SecurityToken = c.client.GetConfig().Token
	}
	projectData := make(map[string]*domain.Project, 0)

	listProjectsResponse, err := c.client.FsProjectApi.ListProjects()
	if err != nil {
		c.logError(fmt.Errorf("list projects error, err=%v", err))
		return err
	}

	for _, p := range listProjectsResponse.Projects {
		if p.ProjectName != c.client.GetConfig().ProjectName {
			continue
		}
		// get datasource
		getDataSourceResponse, err := c.client.DatasourceApi.DatasourceDatasourceIdGet(p.OnlineDatasourceId, c.hologresPort, c.hologresPublicAddress)
		if err != nil {
			c.logError(fmt.Errorf("get datasource error, err=%v", err))
			return err
		}

		p.OnlineDataSource = getDataSourceResponse.Datasource
		p.OnlineDataSource.Ak = ak
		p.OnlineDataSource.TestMode = c.testMode
		p.OnlineDataSource.HologresPrefix = c.hologresPrefix
		p.OnlineDataSource.HologresAuth = c.hologresAuth

		getDataSourceResponse, err = c.client.DatasourceApi.DatasourceDatasourceIdGet(p.OfflineDatasourceId, c.hologresPort, c.hologresPublicAddress)
		if err != nil {
			c.logError(fmt.Errorf("get datasource error, err=%v", err))
			return err
		}

		p.OfflineDataSource = getDataSourceResponse.Datasource
		p.OfflineDataSource.Ak = ak
		p.OfflineDataSource.TestMode = c.testMode

		// get featuredb datasource
		p.FeatureDBAddress, p.FeatureDBToken, p.FeatureDBVpcAddress, err = c.client.DatasourceApi.GetFeatureDBDatasourceInfo(c.testMode, p.OfflineDataSource.WorkspaceId)
		if err != nil {
			c.logError(fmt.Errorf("get featuredb datasource, err=%v", err))
			return err
		}

		p.Signature = c.signature

		project := domain.NewProject(p, c.datasourceInitClient)
		projectData[project.ProjectName] = project

		var (
			pagesize   = 100
			pagenumber = 1
		)

		// get feature entities
		for {
			listFeatureEntitiesResponse, err := c.client.FeatureEntityApi.ListFeatureEntities(int32(pagesize), int32(pagenumber), strconv.Itoa(p.ProjectId))
			if err != nil {
				c.logError(fmt.Errorf("list feature entities error, err=%v", err))
				return err
			}

			for _, entity := range listFeatureEntitiesResponse.FeatureEntities {
				project.FeatureEntityMap[entity.FeatureEntityName] = domain.NewFeatureEntity(entity)
			}

			if len(listFeatureEntitiesResponse.FeatureEntities) == 0 || pagesize*pagenumber > listFeatureEntitiesResponse.TotalCount {
				break
			}

			pagenumber++

		}

		pagenumber = 1
		// get feature views
		for {
			listFeatureViews, err := c.client.FeatureViewApi.ListFeatureViews(int32(pagesize), int32(pagenumber), strconv.Itoa(p.ProjectId))
			if err != nil {
				c.logError(fmt.Errorf("list feature views error, err=%v", err))
				return err
			}

			for _, view := range listFeatureViews.FeatureViews {
				getFeatureViewResponse, err := c.client.FeatureViewApi.GetFeatureViewByID(strconv.Itoa(int(view.FeatureViewId)))
				if err != nil {
					c.logError(fmt.Errorf("get feature view error, err=%v", err))
					return err
				}
				featureView := getFeatureViewResponse.FeatureView
				if featureView.RegisterDatasourceId > 0 {
					getDataSourceResponse, err := c.client.DatasourceApi.DatasourceDatasourceIdGet(featureView.RegisterDatasourceId, c.hologresPort, c.hologresPublicAddress)
					if err != nil {
						c.logError(fmt.Errorf("get datasource error, err=%v", err))
						return err
					}
					featureView.RegisterDataSource = getDataSourceResponse.Datasource
				}

				featureViewDomain := domain.NewFeatureView(featureView, project, project.FeatureEntityMap[featureView.FeatureEntityName])
				project.FeatureViewMap[featureView.Name] = featureViewDomain

			}

			if len(listFeatureViews.FeatureViews) == 0 || pagesize*pagenumber > listFeatureViews.TotalCount {
				break
			}

			pagenumber++

		}

		pagenumber = 1
		// get model
		labelTableMap := make(map[string]*domain.LabelTable)
		for {
			listModelsResponse, err := c.client.FsModelApi.ListModels(pagesize, pagenumber, strconv.Itoa(project.ProjectId))
			if err != nil {
				c.logError(fmt.Errorf("list models error, err=%v", err))
				return err
			}

			for _, m := range listModelsResponse.Models {
				getModelResponse, err := c.client.FsModelApi.GetModelByID(strconv.Itoa(m.ModelId))
				if err != nil {
					c.logError(fmt.Errorf("get model error, err=%v", err))
					return err
				}
				model := getModelResponse.Model
				var labelTableDomain *domain.LabelTable
				if labelTable, exists := labelTableMap[model.LabelDatasourceTable]; !exists || labelTable == nil {
					getLabelTableResponse, err := c.client.LabelTableApi.GetLabelTableByID(strconv.Itoa(model.LabelTableId))
					if err != nil {
						c.logError(fmt.Errorf("get label table error, err=%v", err))
						return err
					}
					labelTableDomain = domain.NewLabelTable(getLabelTableResponse.LabelTable)
					labelTableMap[model.LabelDatasourceTable] = labelTableDomain
				} else {
					labelTableDomain = labelTable
				}
				modelDomain := domain.NewModel(model, project, labelTableDomain)
				project.ModelMap[model.Name] = modelDomain

			}

			if len(listModelsResponse.Models) == 0 || pagenumber*pagesize > int(listModelsResponse.TotalCount) {
				break
			}

			pagenumber++

		}

	}

	if len(projectData) > 0 {
		c.projectMap = projectData
	}

	return nil
}

func (c *FeatureStoreClient) loopLoadProjectData() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopChan:
			return
		case <-ticker.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Println("Recovered from panic:", r)
					}
				}()

				c.LoadProjectData()
			}()
		}
	}
}

func (c *FeatureStoreClient) Stop() {
	close(c.stopChan)
}
