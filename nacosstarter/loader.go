package nacosstarter

import (
	"errors"
	"github.com/acexy/golang-toolkit/logger"
	"github.com/acexy/golang-toolkit/util/json"
	"github.com/golang-acexy/starter-parent/parent"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"gopkg.in/yaml.v3"
	"sync"
	"time"
)

var configInstance config_client.IConfigClient
var namingInstance naming_client.INamingClient
var nm *nacosManager
var namespace string

// 针对多group的nacos实例管理器
type nacosManager struct {
	configLocker sync.Mutex
	namingLocker sync.Mutex

	// key = groupName
	configClient map[string]*ConfigClient
	namingClient map[string]*NamingClient
}

type ConfigClient struct {
	mu      sync.Mutex
	group   string
	watched map[string]*vo.ConfigParam
}

type NamingClient struct {
	mu         sync.Mutex
	group      string
	registered map[string]vo.RegisterInstanceParam
	watched    map[string]*vo.SubscribeParam
}

type Instance struct {
	Ip          string
	ServiceName string
	Port        uint
	Weight      uint
	Metadata    map[string]string
}

type InstanceBatch struct {
	Ip       string
	Port     uint
	Weight   uint
	Metadata map[string]string
}

type RegisteredInstance struct {
	Instance           model.Instance
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

func GetConfigClient(group string) (*ConfigClient, error) {
	if configInstance == nil {
		return nil, errors.New("disabled config client")
	}
	nm.configLocker.Lock()
	defer nm.configLocker.Unlock()
	v, ok := nm.configClient[group]
	if ok {
		return v, nil
	}
	v = &ConfigClient{group: group, watched: make(map[string]*vo.ConfigParam)}
	nm.configClient[group] = v
	return v, nil
}

func GetNamingClient(group string) (*NamingClient, error) {
	if namingInstance == nil {
		return nil, errors.New("disabled discover client")
	}
	nm.namingLocker.Lock()
	defer nm.namingLocker.Unlock()
	v, ok := nm.namingClient[group]
	if ok {
		return v, nil
	}
	v = &NamingClient{group: group, registered: make(map[string]vo.RegisterInstanceParam), watched: make(map[string]*vo.SubscribeParam)}
	nm.namingClient[group] = v
	return v, nil
}

type NacosServerConfig struct {
	Services []constant.ServerConfig
}

type NacosClientConfig struct {
	*constant.ClientConfig
}

type ConfigFileSetting struct {
	DataId string
	Type   ConfigType
	Watch  bool
	Value  any
}

type InitConfigSettings struct {
	ConfigSetting []*ConfigFileSetting
	GroupName     string
}

type NacosConfig struct {
	ServerConfig *NacosServerConfig
	ClientConfig *NacosClientConfig

	// 禁用配置功能
	DisableConfig bool
	// 禁用服务发现功能
	DisableDiscovery bool

	// 需要立即初始化的配置
	// 该设置将在nacos就绪后立即执行，适用于初始化配置其他模块可以立即在后续读取
	InitConfigSettings *InitConfigSettings

	// Nacos启动完毕后执行的函数
	AfterInit func(config config_client.IConfigClient, naming naming_client.INamingClient)
}

type NacosStarter struct {
	Config     NacosConfig
	LazyConfig func() NacosConfig

	config       *NacosConfig
	NacosSetting *parent.Setting
}

func (n *NacosStarter) getConfig() *NacosConfig {
	if n.config == nil {
		var config NacosConfig
		if n.LazyConfig != nil {
			config = n.LazyConfig()
		} else {
			config = n.Config
		}
		n.config = &config
	}
	return n.config
}

func (n *NacosStarter) Setting() *parent.Setting {
	if n.NacosSetting != nil {
		return n.NacosSetting
	}
	return parent.NewSetting("Nacos-Starter", 1, false, time.Second*30, nil)
}

func (n *NacosStarter) Start() (interface{}, error) {

	config := n.getConfig()

	if config.DisableDiscovery && config.DisableConfig {
		return nil, errors.New("config and discover modules are disabled")
	}
	if config.ServerConfig == nil || config.ClientConfig == nil || len(config.ServerConfig.Services) == 0 {
		return nil, errors.New("bad nacos config")
	}

	nm = &nacosManager{configClient: make(map[string]*ConfigClient), namingClient: make(map[string]*NamingClient)}
	if len(config.ServerConfig.Services) == 0 {
		return nil, errors.New("bad service config")
	}
	if config.ClientConfig.ClientConfig.NamespaceId == "public" {
		config.ClientConfig.ClientConfig.NamespaceId = ""
	}
	namespace = config.ClientConfig.NamespaceId
	if !config.DisableConfig {
		cc, err := clients.NewConfigClient(vo.NacosClientParam{
			ServerConfigs: config.ServerConfig.Services,
			ClientConfig:  config.ClientConfig.ClientConfig,
		})
		if err != nil {
			return nil, err
		}
		configInstance = cc
		if config.InitConfigSettings != nil && len(config.InitConfigSettings.ConfigSetting) > 0 && config.InitConfigSettings.GroupName != "" {
			client, _ := GetConfigClient(config.InitConfigSettings.GroupName)
			client.LoadAndWatchConfig(config.InitConfigSettings.ConfigSetting)
		}
	}
	if !config.DisableDiscovery {
		nc, err := clients.NewNamingClient(
			vo.NacosClientParam{
				ClientConfig:  config.ClientConfig.ClientConfig,
				ServerConfigs: config.ServerConfig.Services,
			},
		)
		if err != nil {
			return nil, err
		}
		namingInstance = nc
	}
	if config.AfterInit != nil {
		config.AfterInit(configInstance, namingInstance)
	}
	return nil, nil
}

func (n *NacosStarter) Stop(maxWaitTime time.Duration) (gracefully, stopped bool, err error) {
	if configInstance != nil {
		configInstance.CloseClient()
	}
	if namingInstance != nil {
		done := make(chan interface{})
		go func() {
			for _, v := range nm.namingClient {
				for id, i := range v.registered {
					flag, err := v.Unregister(id)
					if err != nil {
						logger.Logrus().WithError(err).Error("unregister instance failed ip:", i.Ip, "port:", i.Port)
					} else {
						logger.Logrus().Traceln("unregister instance ip:", i.Ip, "port:", i.Port, "result:", flag)
					}
				}
			}
			namingInstance.CloseClient()
			done <- true
		}()
		select {
		case <-done:
			return true, true, nil
		case <-time.After(maxWaitTime):
			return false, true, nil
		}
	}
	return true, true, nil
}

func RawConfigInstance() config_client.IConfigClient {
	return configInstance
}

func RawNamingInstance() naming_client.INamingClient {
	return namingInstance
}
