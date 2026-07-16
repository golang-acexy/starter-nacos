package nacosstarter

import (
	"fmt"
	"reflect"
	"time"

	"github.com/acexy/golang-toolkit/logger"
	"github.com/acexy/golang-toolkit/util/json"
	"github.com/golang-acexy/starter-parent/parent"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"gopkg.in/yaml.v3"
)

// 以下包级变量由 Start() 初始化、Stop() 清理，生命周期由 StarterLoader 串行化保证，无需额外同步。
var configInstance config_client.IConfigClient
var namingInstance naming_client.INamingClient
var nm *nacosManager
var namespace string

func deserializeConfig(content string, configType ConfigType, value any) error {
	switch configType {
	case ConfigTypeYaml:
		// 确保value是指针
		rv := reflect.ValueOf(value)
		if rv.Kind() != reflect.Ptr || rv.IsNil() {
			return ErrValueMustBeNonNilPointer
		}
		elem := rv.Elem()
		newValue := reflect.New(elem.Type()).Interface()
		err := yaml.Unmarshal([]byte(content), newValue)
		if err != nil {
			return err
		}
		elem.Set(reflect.ValueOf(newValue).Elem())
		return nil
	case ConfigTypeJson:
		return json.ParseStringError(content, value)
	}
	return fmt.Errorf("%w: %s", ErrUnknownConfigType, configType)
}

// GetConfigClient 获取指定group的配置客户端，group不存在时自动创建。
func GetConfigClient(group string) (*ConfigClient, error) {
	if configInstance == nil || nm == nil {
		return nil, ErrDisabledConfigClient
	}
	if group == "" {
		group = DefaultGroup
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

// GetNamingClient 获取指定group的服务发现客户端，group不存在时自动创建。
func GetNamingClient(group string) (*NamingClient, error) {
	if namingInstance == nil || nm == nil {
		return nil, ErrDisabledDiscoveryClient
	}
	if group == "" {
		group = DefaultGroup
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

func currentConfigInstance() (config_client.IConfigClient, error) {
	if configInstance == nil {
		return nil, ErrDisabledConfigClient
	}
	return configInstance, nil
}

func currentNamingInstance() (naming_client.INamingClient, error) {
	if namingInstance == nil {
		return nil, ErrDisabledDiscoveryClient
	}
	return namingInstance, nil
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
	return parent.NewSetting("Nacos-Starter", false, 0, false, time.Second*30, nil)
}

func (n *NacosStarter) Start() (any, error) {
	config := n.getConfig()

	if config.DisableDiscovery && config.DisableConfig {
		return nil, ErrConfigAndDiscoveryDisabled
	}
	if config.ServerConfig == nil || config.ClientConfig == nil || config.ClientConfig.ClientConfig == nil || len(config.ServerConfig.Services) == 0 {
		return nil, ErrBadNacosConfig
	}
	if nm != nil {
		return nil, ErrNacosStarterAlreadyStarted
	}

	nm = &nacosManager{configClient: make(map[string]*ConfigClient), namingClient: make(map[string]*NamingClient)}
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
			closeAndClearNacosState()
			return nil, err
		}
		configInstance = cc
		if config.InitConfigSettings != nil && len(config.InitConfigSettings.ConfigSetting) > 0 {
			groupName := config.InitConfigSettings.GroupName
			if groupName == "" {
				groupName = DefaultGroup
			}
			client, err := GetConfigClient(groupName)
			if err != nil {
				logger.Logrus().WithError(err).Errorln("GetConfigClient failed for InitConfigSettings")
			} else {
				client.LoadAndWatchConfig(config.InitConfigSettings.ConfigSetting)
			}
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
			closeAndClearNacosState()
			return nil, err
		}
		namingInstance = nc
	}
	configClient := configInstance
	namingClient := namingInstance
	if config.InitFunc != nil {
		config.InitFunc(configClient, namingClient)
	}
	return nil, nil
}

func (n *NacosStarter) Stop(maxWaitTime time.Duration) (gracefully, stopped bool, err error) {
	configClient := configInstance
	rawNamingClient := namingInstance

	if rawNamingClient != nil {
		done := make(chan struct{}, 1)
		go func() {
			for _, managedNamingClient := range snapshotNamingClients() {
				for id, instance := range managedNamingClient.snapshotRegistered() {
					flag, err := managedNamingClient.Unregister(id)
					if err != nil {
						logger.Logrus().WithError(err).Error("unregister instance failed ip:", instance.Ip, "port:", instance.Port)
					} else {
						logger.Logrus().Traceln("unregister instance ip:", instance.Ip, "port:", instance.Port, "result:", flag)
					}
				}
			}
			rawNamingClient.CloseClient()
			done <- struct{}{}
		}()
		select {
		case <-done:
			if configClient != nil {
				configClient.CloseClient()
			}
			clearNacosState()
			return true, true, nil
		case <-time.After(maxWaitTime):
			return false, false, ErrNacosStopTimeout
		}
	}
	if configClient != nil {
		configClient.CloseClient()
	}
	clearNacosState()
	return true, true, nil
}

// RawConfigInstance 返回底层Nacos配置客户端，供需要直接操作SDK的集成代码使用。
func RawConfigInstance() config_client.IConfigClient {
	return configInstance
}

// RawNamingInstance 返回底层Nacos服务发现客户端，供需要直接操作SDK的集成代码使用。
func RawNamingInstance() naming_client.INamingClient {
	return namingInstance
}

func clearNacosState() {
	configInstance = nil
	namingInstance = nil
	nm = nil
	namespace = ""
}

func closeAndClearNacosState() {
	if configInstance != nil {
		configInstance.CloseClient()
	}
	if namingInstance != nil {
		namingInstance.CloseClient()
	}
	clearNacosState()
}
