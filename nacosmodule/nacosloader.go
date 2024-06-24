package nacosmodule

import (
	"errors"
	"github.com/acexy/golang-toolkit/crypto/hashing/md5"
	"github.com/acexy/golang-toolkit/logger"
	"github.com/acexy/golang-toolkit/util/json"
	"github.com/golang-acexy/starter-parent/parentmodule/declaration"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"gopkg.in/yaml.v3"
	"strconv"
	"sync"
	"time"
)

var configInstance config_client.IConfigClient
var namingInstance naming_client.INamingClient

type ConfigType string

// ConfigChangeListener 文件变动监听回调
type ConfigChangeListener func(namespace, group, dataId, data string)

const (
	ConfigTypeJson ConfigType = "json"
	ConfigTypeYaml ConfigType = "yaml"
)

var nm *nacosManager

type nacosManager struct {
	ccl sync.Mutex
	ncl sync.Mutex
	cc  map[string]*configClient
	nc  map[string]*namingClient
}

type NacosServerConfig struct {
	Services []constant.ServerConfig
}

type NacosClientConfig struct {
	*constant.ClientConfig
}

type InitConfigSettings struct {
	ConfigSetting []*ConfigFileSetting
	GroupName     string
}

type NacosModule struct {

	// nacos组件模块设置
	GrpcModuleConfig *declaration.ModuleConfig

	ServerConfig *NacosServerConfig
	ClientConfig *NacosClientConfig

	// 禁用配置功能
	DisableConfig bool
	// 禁用服务发现功能
	DisableDiscovery bool

	// 需要立即初始化的配置
	// 该设置将在nacos就绪后立即执行，适用于初始化配置其他模块可以立即在后续读取
	InitConfigSettings *InitConfigSettings
}

func (n *NacosModule) ModuleConfig() *declaration.ModuleConfig {
	if n.GrpcModuleConfig != nil {
		return n.GrpcModuleConfig
	}
	return &declaration.ModuleConfig{
		ModuleName:               "Nacos",
		UnregisterPriority:       1,
		UnregisterAllowAsync:     true,
		UnregisterMaxWaitSeconds: 30,
	}
}

func (n *NacosModule) Register() (interface{}, error) {

	if n.DisableDiscovery && n.DisableConfig {
		return nil, errors.New("config and discover modules are disabled")
	}
	if n.ServerConfig == nil || n.ClientConfig == nil || len(n.ServerConfig.Services) == 0 {
		return nil, errors.New("bad nacos config")
	}

	nm = &nacosManager{cc: make(map[string]*configClient), nc: make(map[string]*namingClient)}

	if len(n.ServerConfig.Services) == 0 {
		return nil, errors.New("bad service config")
	}

	if n.ClientConfig.ClientConfig.NamespaceId == "public" {
		n.ClientConfig.ClientConfig.NamespaceId = ""
	}

	if !n.DisableConfig {
		cc, err := clients.NewConfigClient(vo.NacosClientParam{
			ServerConfigs: n.ServerConfig.Services,
			ClientConfig:  n.ClientConfig.ClientConfig,
		})

		if err != nil {
			return nil, err
		}

		configInstance = cc
		if n.InitConfigSettings != nil && len(n.InitConfigSettings.ConfigSetting) > 0 && n.InitConfigSettings.GroupName != "" {
			client, _ := GetConfigClient(n.InitConfigSettings.GroupName)
			err = client.LoadAndWatchConfig(n.InitConfigSettings.ConfigSetting)
			if err != nil {
				return nil, err
			}
		}
	}

	if !n.DisableDiscovery {
		nc, err := clients.NewNamingClient(
			vo.NacosClientParam{
				ClientConfig:  n.ClientConfig.ClientConfig,
				ServerConfigs: n.ServerConfig.Services,
			},
		)
		if err != nil {
			return nil, err
		}
		namingInstance = nc
	}
	return nil, nil
}

func (n *NacosModule) Unregister(maxWaitSeconds uint) (bool, error) {
	if configInstance != nil {
		configInstance.CloseClient()
	}
	if namingInstance != nil {
		done := make(chan interface{})
		go func() {
			for _, v := range nm.nc {
				for id, i := range v.registered {
					flag, err := v.Unregister(id)
					if err != nil {
						logger.Logrus().WithError(err).Error("unregister instance failed ip", i.Ip, "port", i.Port)
					} else {
						logger.Logrus().Debugln("unregister instance ip", i.Ip, "port", i.Port, "result", flag)
					}
				}
			}
			namingInstance.CloseClient()
			done <- true
		}()
		select {
		case <-done:
			return true, nil
		case <-time.After(time.Second * time.Duration(maxWaitSeconds)):
			return false, nil
		}
	}
	return true, nil
}

type configClient struct {
	mu      sync.Mutex
	group   string
	watched map[string]*vo.ConfigParam
}

type namingClient struct {
	mu         sync.Mutex
	group      string
	registered map[string]vo.RegisterInstanceParam
	watched    map[string]*vo.SubscribeParam
}

type ConfigFileSetting struct {
	DataId string
	Type   ConfigType
	Watch  bool
	Value  any
}

type ServiceInstance struct {
	Instance           *model.Instance
	InstanceIdentifier string
}

func deserializeConfig(content string, configType ConfigType, value any) error {
	switch configType {
	case ConfigTypeYaml:
		return yaml.Unmarshal([]byte(content), value)
	case ConfigTypeJson:
		return json.ParseJsonError(content, value)
	}
	return errors.New("known config type " + string(configType))
}

func GetConfigClient(group string) (*configClient, error) {
	if configInstance == nil {
		return nil, errors.New("disabled config client")
	}
	nm.ccl.Lock()
	defer nm.ccl.Unlock()
	v, ok := nm.cc[group]
	if ok {
		return v, nil
	}
	v = &configClient{group: group, watched: make(map[string]*vo.ConfigParam)}
	nm.cc[group] = v
	return v, nil
}

func GetNamingClient(group string) (*namingClient, error) {
	if namingInstance == nil {
		return nil, errors.New("disabled discover client")
	}
	nm.ncl.Lock()
	defer nm.ncl.Unlock()
	v, ok := nm.nc[group]
	if ok {
		return v, nil
	}
	v = &namingClient{group: group, registered: make(map[string]vo.RegisterInstanceParam), watched: make(map[string]*vo.SubscribeParam)}
	nm.nc[group] = v
	return v, nil
}

// GetConfigRawContent 获取指定配置的源文件内容
func (c *configClient) GetConfigRawContent(dataId string) (string, error) {
	return configInstance.GetConfig(vo.ConfigParam{DataId: dataId, Group: c.group})
}

// GetConfig 获取指定文件内容并反序列化
func (c *configClient) GetConfig(dataId string, configType ConfigType, value any) error {
	raw, err := c.GetConfigRawContent(dataId)
	if err != nil {
		return nil
	}
	return deserializeConfig(raw, configType, value)
}

// WatchConfig 监听文件变化
func (c *configClient) WatchConfig(dataId string, watch ConfigChangeListener) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	param := vo.ConfigParam{DataId: dataId, Group: c.group}
	watchId := md5.HexMd5(json.ToJson(param) + strconv.FormatInt(time.Now().UnixNano(), 10))
	param.OnChange = func(namespace, group, dataId, data string) {
		watch(namespace, group, dataId, data)
	}
	c.watched[watchId] = &param
	return watchId, configInstance.ListenConfig(param)
}

// UnwatchConfig 取消监听文件变化
func (c *configClient) UnwatchConfig(watchId string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.watched[watchId]
	if ok {
		return configInstance.CancelListenConfig(*v)
	}
	return errors.New("bad watchId")
}

// LoadAndWatchConfig 获取并监听配置变化
func (c *configClient) LoadAndWatchConfig(configFiles []*ConfigFileSetting) error {
	if len(configFiles) == 0 {
		return errors.New("empty config file")
	}
	for _, f := range configFiles {
		err := c.GetConfig(f.DataId, f.Type, f.Value)
		if err != nil {
			return err
		}
		if f.Watch {
			_, err = c.WatchConfig(f.DataId, func(namespace, group, dataId, data string) {
				err = deserializeConfig(data, f.Type, f.Value)
				if err != nil {
					logger.Logrus().WithError(err).Error("cant deserialize content:", data)
				}
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Register 向注册中心注册实例
func (n *namingClient) Register(ip, serviceName string, port, weight int, metadata map[string]string) (string, error) {
	i := vo.RegisterInstanceParam{
		Ip:          ip,
		ServiceName: serviceName,
		Port:        uint64(port),
		Weight:      float64(weight),
		Enable:      true,
		Healthy:     true,
		Metadata:    metadata,
		GroupName:   n.group,
		Ephemeral:   true,
	}
	ii := md5.HexMd5(json.ToJson(i))
	n.registered[ii] = i
	flag, err := namingInstance.RegisterInstance(i)
	if err != nil {
		return "", err
	}
	if !flag {
		return "", errors.New("register failed")
	}
	logger.Logrus().Traceln("registered ip", ip, "port", port, "service", serviceName)
	return ii, nil
}

// Unregister 向注册中心注销实例
func (n *namingClient) Unregister(instanceId string) (bool, error) {
	v, ok := n.registered[instanceId]
	if !ok {
		return false, nil
	}
	var param vo.DeregisterInstanceParam
	json.CopyStruct(v, &param)
	flag, err := namingInstance.DeregisterInstance(param)
	if err != nil {
		return false, err
	}
	if !flag {
		return false, nil
	}
	logger.Logrus().Traceln("unregistered ip", param.Ip, "port", param.Port, "service", param.ServiceName)
	return true, nil
}

// GetService 获取指定服务的注册信息 仅可用状态
func (n *namingClient) GetService(serviceName string) (model.Service, error) {
	return namingInstance.GetService(vo.GetServiceParam{
		ServiceName: serviceName,
		GroupName:   n.group,
	})
}

// GetAllServiceInfo 获取指定所有服务的注册信息
func (n *namingClient) GetAllServiceInfo(namespace string, pageNo, pageSize uint) (model.ServiceList, error) {
	return namingInstance.GetAllServicesInfo(vo.GetAllServiceInfoParam{
		NameSpace: namespace,
		GroupName: n.group,
		PageNo:    uint32(pageNo),
		PageSize:  uint32(pageSize),
	})
}

// GetAllInstance 获取指定服务的所有实例(不论当前是否可用)
func (n *namingClient) GetAllInstance(serviceName string) ([]model.Instance, error) {
	return namingInstance.SelectAllInstances(vo.SelectAllInstancesParam{ServiceName: serviceName, GroupName: n.group})
}

// ChooseOneHealthyRandom 选择一个可用的实例
func (n *namingClient) ChooseOneHealthyRandom(serviceName string) (*ServiceInstance, error) {
	instance, err := namingInstance.SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{ServiceName: serviceName, GroupName: n.group})
	if err != nil {
		return nil, err
	}
	return &ServiceInstance{InstanceIdentifier: md5.HexMd5(json.ToJson(instance)), Instance: instance}, nil
}

// WatchNaming 监控服务的实例变化
func (n *namingClient) WatchNaming(serviceName string, watch func(instance []model.Instance, err error)) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	param := &vo.SubscribeParam{ServiceName: serviceName, GroupName: n.group}
	watchId := md5.HexMd5(json.ToJson(param) + strconv.FormatInt(time.Now().UnixNano(), 10))
	param.SubscribeCallback = watch
	n.watched[watchId] = param
	return watchId, namingInstance.Subscribe(param)
}

// UnwatchNaming 取消监控服务实例变化
func (n *namingClient) UnwatchNaming(watchId string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	v, ok := n.watched[watchId]
	if !ok {
		return errors.New("bad watchId")
	}
	return namingInstance.Unsubscribe(v)
}

func RawConfigInstance() config_client.IConfigClient {
	return configInstance
}

func RawNamingInstance() naming_client.INamingClient {
	return namingInstance
}
