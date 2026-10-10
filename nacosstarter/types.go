package nacosstarter

import (
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// DefaultGroup is used when a client accessor receives an empty group.
const DefaultGroup = "DEFAULT_GROUP"

// nacosManager caches independently synchronized group wrappers.
type nacosManager struct {
	configLocker sync.Mutex
	namingLocker sync.Mutex

	configClient map[string]*ConfigClient
	namingClient map[string]*NamingClient
}

// ConfigClient manages configuration subscriptions for one group.
// Obtain it through GetConfigClient; do not reuse it across starter lifetimes.
// Bookkeeping is synchronized, but callbacks and caller-owned values need caller synchronization.
type ConfigClient struct {
	mu      sync.Mutex
	group   string
	watched map[string]*vo.ConfigParam
}

// NamingClient manages registrations and subscriptions for one group.
// Obtain it through GetNamingClient; do not reuse it across starter lifetimes.
// Bookkeeping is synchronized; callers must coordinate operations on the same registration or watch.
type NamingClient struct {
	mu         sync.Mutex
	group      string
	registered map[string]vo.RegisterInstanceParam
	watched    map[string]*vo.SubscribeParam
}

// Instance describes an ephemeral registration in the SDK's default cluster.
type Instance struct {
	// Ip is the advertised address.
	Ip string
	// ServiceName identifies the service within the client's group.
	ServiceName string
	// Port is the advertised service port.
	Port uint
	// Weight is forwarded unchanged; zero excludes the instance from healthy selection.
	Weight uint
	// Metadata is passed to the SDK without copying; do not mutate it during SDK use.
	Metadata map[string]string
}

// InstanceBatch describes an instance in a service-specific ephemeral batch.
type InstanceBatch struct {
	// Ip is the advertised address.
	Ip string
	// Port is the advertised service port.
	Port uint
	// Weight is forwarded unchanged; zero excludes the instance from healthy selection.
	Weight uint
	// Metadata is passed to the SDK without copying; do not mutate it during SDK use.
	Metadata map[string]string
}

// RegisteredInstance pairs discovered SDK data with a group/service/address identifier.
// Discovery does not add the instance to this wrapper's local registration registry.
type RegisteredInstance struct {
	// Instance contains the SDK result; nested maps are not copied by this wrapper.
	Instance model.Instance
	// InstanceIdentifier identifies the group, service, IP, and port.
	InstanceIdentifier string
}

// NacosServerConfig supplies at least one SDK server endpoint.
type NacosServerConfig struct {
	// Services lists server endpoints passed to both enabled clients.
	Services []constant.ServerConfig
}

// NacosClientConfig wraps shared SDK settings; keep them stable during starter use.
type NacosClientConfig struct {
	// ClientConfig must be nonnil; startup normalizes NamespaceId "public" to "" in place.
	*constant.ClientConfig
}

// ConfigFileSetting describes one best-effort configuration load and optional watch.
// Keep the setting and destination stable while the watch is active.
type ConfigFileSetting struct {
	// DataId identifies the remote configuration file.
	DataId string
	// Type selects JSON or YAML decoding.
	Type ConfigType
	// Watch enables subsequent updates even when the initial load fails.
	Watch bool
	// Value must be a nonnil destination pointer; external access requires caller synchronization.
	Value any
}

// InitConfigSettings supplies nonnil file settings loaded before InitFunc.
type InitConfigSettings struct {
	// ConfigSetting lists files; individual failures do not abort startup.
	ConfigSetting []*ConfigFileSetting
	// GroupName defaults to DefaultGroup when empty.
	GroupName string
}

// NacosConfig configures enabled capabilities and startup initialization.
type NacosConfig struct {
	// ServerConfig must contain at least one server endpoint.
	ServerConfig *NacosServerConfig
	// ClientConfig and its embedded SDK configuration must be nonnil.
	ClientConfig *NacosClientConfig

	// DisableConfig prevents creation of the configuration client.
	DisableConfig bool
	// DisableDiscovery prevents creation of the naming client; both capabilities cannot be disabled.
	DisableDiscovery bool
	// InitConfigSettings loads and optionally watches files before InitFunc on a best-effort basis.
	InitConfigSettings *InitConfigSettings
	// InitFunc runs synchronously after initial loading; disabled SDK clients are nil.
	// It cannot return an error; callers must arrange startup-critical readiness themselves.
	InitFunc func(config config_client.IConfigClient, naming naming_client.INamingClient)
}
