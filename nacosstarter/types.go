package nacosstarter

import (
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// Nacos默认分组名称
const DefaultGroup = "DEFAULT_GROUP"

// nacosManager 针对多group的nacos实例管理器
type nacosManager struct {
	configLocker sync.Mutex
	namingLocker sync.Mutex

	// key = groupName
	configClient map[string]*ConfigClient
	namingClient map[string]*NamingClient
}

// ConfigClient Nacos配置客户端封装
type ConfigClient struct {
	mu      sync.Mutex
	group   string
	watched map[string]*vo.ConfigParam
}

// NamingClient Nacos服务发现客户端封装
type NamingClient struct {
	mu         sync.Mutex
	group      string
	registered map[string]vo.RegisterInstanceParam
	watched    map[string]*vo.SubscribeParam
}

// Instance 服务实例注册参数
type Instance struct {
	Ip          string
	ServiceName string
	Port        uint
	Weight      uint
	Metadata    map[string]string
}

// InstanceBatch 批量服务实例注册参数
type InstanceBatch struct {
	Ip       string
	Port     uint
	Weight   uint
	Metadata map[string]string
}

// RegisteredInstance 已注册的服务实例信息
type RegisteredInstance struct {
	Instance           model.Instance
	InstanceIdentifier string
}

// NacosServerConfig Nacos服务端配置
type NacosServerConfig struct {
	Services []constant.ServerConfig
}

// NacosClientConfig Nacos客户端配置
type NacosClientConfig struct {
	*constant.ClientConfig
}

// ConfigFileSetting 配置文件设置
type ConfigFileSetting struct {
	DataId string
	Type   ConfigType
	Watch  bool
	Value  any
}

// InitConfigSettings 初始化配置设置
type InitConfigSettings struct {
	ConfigSetting []*ConfigFileSetting
	GroupName     string
}

// NacosConfig Nacos完整配置
type NacosConfig struct {
	ServerConfig *NacosServerConfig
	ClientConfig *NacosClientConfig

	// DisableConfig 禁用配置功能
	DisableConfig bool
	// DisableDiscovery 禁用服务发现功能
	DisableDiscovery bool

	// InitConfigSettings 需要立即初始化的配置。
	// 该设置将在config client创建完成后立即执行，优先级高于InitFunc。
	// 适用于提前加载配置，确保InitFunc和后续starter可以直接读取初始化后的配置值。
	InitConfigSettings *InitConfigSettings

	// InitFunc Nacos启动完毕后执行的初始化函数。
	// 执行顺序晚于InitConfigSettings，此时config client和naming client已经按配置完成初始化。
	InitFunc func(config config_client.IConfigClient, naming naming_client.INamingClient)
}

