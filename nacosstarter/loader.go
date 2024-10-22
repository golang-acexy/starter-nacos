package nacosstarter

import (
	"errors"
	"github.com/acexy/golang-toolkit/logger"
	"github.com/golang-acexy/starter-parent/parent"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"time"
)

var configInstance config_client.IConfigClient
var namingInstance naming_client.INamingClient

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

type NacosStarter struct {

	// nacos组件模块设置
	GrpcModuleConfig *parent.Setting

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

func (n *NacosStarter) Setting() *parent.Setting {
	if n.GrpcModuleConfig != nil {
		return n.GrpcModuleConfig
	}
	return parent.NewSetting("Nacos-Starter", 1, false, time.Second*30, nil)
}

func (n *NacosStarter) Start() (interface{}, error) {

	if n.DisableDiscovery && n.DisableConfig {
		return nil, errors.New("config and discover modules are disabled")
	}
	if n.ServerConfig == nil || n.ClientConfig == nil || len(n.ServerConfig.Services) == 0 {
		return nil, errors.New("bad nacos config")
	}

	nm = &nacosManager{cc: make(map[string]*ConfigClient), nc: make(map[string]*NamingClient)}

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

func (n *NacosStarter) Stop(maxWaitTime time.Duration) (gracefully, stopped bool, err error) {
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
