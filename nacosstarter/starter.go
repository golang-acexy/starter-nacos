package nacosstarter

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
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

var nacosRuntimeState atomic.Pointer[nacosRuntime]
var nacosLifecycleLock sync.Mutex
var nacosState nacosLifecycleState

type nacosRuntime struct {
	config    config_client.IConfigClient
	naming    naming_client.INamingClient
	manager   *nacosManager
	namespace string
}

type nacosLifecycleState uint8

const (
	nacosStopped nacosLifecycleState = iota
	nacosStarting
	nacosRunning
	nacosStopping
)

func deserializeConfig(content string, configType ConfigType, value any) error {
	switch configType {
	case ConfigTypeYaml:
		// Decode into a fresh value so failed YAML decoding leaves the target unchanged.
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

// GetConfigClient returns a cached group wrapper; an empty group means DefaultGroup.
// It returns ErrDisabledConfigClient when the capability is unavailable.
func GetConfigClient(group string) (*ConfigClient, error) {
	runtime := nacosRuntimeState.Load()
	if runtime == nil || runtime.config == nil || runtime.manager == nil {
		return nil, ErrDisabledConfigClient
	}
	if group == "" {
		group = DefaultGroup
	}
	runtime.manager.configLocker.Lock()
	defer runtime.manager.configLocker.Unlock()
	v, ok := runtime.manager.configClient[group]
	if ok {
		return v, nil
	}
	v = &ConfigClient{group: group, watched: make(map[string]*vo.ConfigParam)}
	runtime.manager.configClient[group] = v
	return v, nil
}

// GetNamingClient returns a cached group wrapper; an empty group means DefaultGroup.
// It returns ErrDisabledDiscoveryClient when the capability is unavailable.
func GetNamingClient(group string) (*NamingClient, error) {
	runtime := nacosRuntimeState.Load()
	if runtime == nil || runtime.naming == nil || runtime.manager == nil {
		return nil, ErrDisabledDiscoveryClient
	}
	if group == "" {
		group = DefaultGroup
	}
	runtime.manager.namingLocker.Lock()
	defer runtime.manager.namingLocker.Unlock()
	v, ok := runtime.manager.namingClient[group]
	if ok {
		return v, nil
	}
	v = &NamingClient{group: group, registered: make(map[string]vo.RegisterInstanceParam), watched: make(map[string]*vo.SubscribeParam)}
	runtime.manager.namingClient[group] = v
	return v, nil
}

func currentConfigInstance() (config_client.IConfigClient, error) {
	runtime := nacosRuntimeState.Load()
	if runtime == nil || runtime.config == nil {
		return nil, ErrDisabledConfigClient
	}
	return runtime.config, nil
}

func currentNamingInstance() (naming_client.INamingClient, error) {
	runtime := nacosRuntimeState.Load()
	if runtime == nil || runtime.naming == nil {
		return nil, ErrDisabledDiscoveryClient
	}
	return runtime.naming, nil
}

// NacosStarter owns process-wide SDK clients under the parent lifecycle.
// Configuration resolution and lifecycle calls on this instance require external sequencing.
type NacosStarter struct {
	// Config is used when LazyConfig is nil; the resolved value is cached.
	Config NacosConfig
	// LazyConfig takes precedence over Config and is evaluated on first configuration access.
	LazyConfig func() NacosConfig

	// NacosSetting overrides default parent lifecycle settings when nonnil.
	NacosSetting *parent.Setting

	config *NacosConfig
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

// Setting returns an override or defaults: no restart, priority zero, and a 30-second startup wait.
func (n *NacosStarter) Setting() *parent.Setting {
	if n.NacosSetting != nil {
		return n.NacosSetting
	}
	return parent.NewSetting("Nacos-Starter", false, 0, false, time.Second*30, nil)
}

// Start initializes enabled SDK clients and runs configuration loading before InitFunc.
// It returns nil on success, not an SDK resource; initial file failures do not fail startup.
// Configuration is cached and the shared SDK NamespaceId "public" is normalized in place.
func (n *NacosStarter) Start() (any, error) {
	config := n.getConfig()

	if config.DisableDiscovery && config.DisableConfig {
		return nil, ErrConfigAndDiscoveryDisabled
	}
	if config.ServerConfig == nil || config.ClientConfig == nil || config.ClientConfig.ClientConfig == nil || len(config.ServerConfig.Services) == 0 {
		return nil, ErrBadNacosConfig
	}
	nacosLifecycleLock.Lock()
	if nacosState != nacosStopped {
		nacosLifecycleLock.Unlock()
		return nil, ErrNacosStarterAlreadyStarted
	}
	nacosState = nacosStarting
	nacosLifecycleLock.Unlock()
	started := false
	defer func() {
		if !started {
			nacosLifecycleLock.Lock()
			nacosState = nacosStopped
			nacosLifecycleLock.Unlock()
		}
	}()

	manager := &nacosManager{configClient: make(map[string]*ConfigClient), namingClient: make(map[string]*NamingClient)}
	if config.ClientConfig.ClientConfig.NamespaceId == "public" {
		config.ClientConfig.ClientConfig.NamespaceId = ""
	}
	namespace := config.ClientConfig.NamespaceId
	var configClient config_client.IConfigClient
	var namingClient naming_client.INamingClient
	if !config.DisableConfig {
		cc, err := clients.NewConfigClient(vo.NacosClientParam{
			ServerConfigs: config.ServerConfig.Services,
			ClientConfig:  config.ClientConfig.ClientConfig,
		})
		if err != nil {
			return nil, err
		}
		configClient = cc
	}
	if !config.DisableDiscovery {
		nc, err := clients.NewNamingClient(
			vo.NacosClientParam{
				ClientConfig:  config.ClientConfig.ClientConfig,
				ServerConfigs: config.ServerConfig.Services,
			},
		)
		if err != nil {
			if configClient != nil {
				configClient.CloseClient()
			}
			return nil, err
		}
		namingClient = nc
	}
	runtime := &nacosRuntime{config: configClient, naming: namingClient, manager: manager, namespace: namespace}
	nacosLifecycleLock.Lock()
	nacosRuntimeState.Store(runtime)
	nacosState = nacosRunning
	nacosLifecycleLock.Unlock()
	started = true

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
	if config.InitFunc != nil {
		config.InitFunc(configClient, namingClient)
	}
	return nil, nil
}

// Stop withdraws the clients, deregisters tracked instances, and closes SDK resources.
// Timeout reports stopped=true while cleanup continues; deregistration errors are only logged.
// Callers must stop client operations before shutdown; retained wrappers must not be reused.
func (n *NacosStarter) Stop(maxWaitTime time.Duration) (gracefully, stopped bool, err error) {
	nacosLifecycleLock.Lock()
	runtime := nacosRuntimeState.Load()
	if nacosState != nacosRunning || runtime == nil {
		nacosLifecycleLock.Unlock()
		return false, true, ErrNacosStarterNotStarted
	}
	nacosRuntimeState.Store(nil)
	nacosState = nacosStopping
	nacosLifecycleLock.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			nacosLifecycleLock.Lock()
			nacosState = nacosStopped
			nacosLifecycleLock.Unlock()
		}()
		if runtime.naming != nil {
			for _, managedNamingClient := range snapshotNamingClients(runtime.manager) {
				for id, instance := range managedNamingClient.snapshotRegistered() {
					flag, err := runtime.naming.DeregisterInstance(vo.DeregisterInstanceParam{
						Ip:          instance.Ip,
						Port:        instance.Port,
						Cluster:     instance.ClusterName,
						ServiceName: instance.ServiceName,
						GroupName:   instance.GroupName,
						Ephemeral:   instance.Ephemeral,
					})
					if err != nil {
						logger.Logrus().WithError(err).Error("unregister instance failed ip:", instance.Ip, "port:", instance.Port)
					} else {
						if flag {
							managedNamingClient.mu.Lock()
							delete(managedNamingClient.registered, id)
							managedNamingClient.mu.Unlock()
						}
						logger.Logrus().Traceln("unregister instance ip:", instance.Ip, "port:", instance.Port, "result:", flag)
					}
				}
			}
			runtime.naming.CloseClient()
		}
		if runtime.config != nil {
			runtime.config.CloseClient()
		}
	}()

	timer := time.NewTimer(maxWaitTime)
	defer timer.Stop()
	select {
	case <-done:
		gracefully = true
	case <-timer.C:
		err = ErrNacosStopTimeout
	}
	return gracefully, true, err
}

// RawConfigInstance returns the shared SDK client or nil when unavailable.
// The starter owns its closure; direct SDK subscriptions bypass wrapper bookkeeping.
func RawConfigInstance() config_client.IConfigClient {
	runtime := nacosRuntimeState.Load()
	if runtime == nil {
		return nil
	}
	return runtime.config
}

// RawNamingInstance returns the shared SDK client or nil when unavailable.
// The starter owns its closure; direct SDK registrations bypass wrapper bookkeeping.
func RawNamingInstance() naming_client.INamingClient {
	runtime := nacosRuntimeState.Load()
	if runtime == nil {
		return nil
	}
	return runtime.naming
}
