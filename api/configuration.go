package api

import "fmt"

type Configuration struct {
	regionId        string
	AccessKeyId     string
	AccessKeySecret string
	Token           string
	ProjectName     string
	UserAgent       string
	domain          string
}

func NewConfiguration(regionId, accessKeyId, accessKeySecret, token, projectName string) *Configuration {
	cfg := &Configuration{
		UserAgent:       "PAI-FeatureStore/1.0.0/go",
		regionId:        regionId,
		ProjectName:     projectName,
		AccessKeyId:     accessKeyId,
		AccessKeySecret: accessKeySecret,
		Token:           token,
	}
	return cfg
}

func (c *Configuration) SetDomain(domain string) {
	c.domain = domain
}

func (c *Configuration) GetDomain() string {
	if c.domain == "" {
		c.domain = fmt.Sprintf("paifeaturestore-vpc.%s.aliyuncs.com", c.regionId)
	}

	return c.domain
}
