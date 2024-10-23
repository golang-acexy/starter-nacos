package nacosstarter

import (
	"errors"
	"github.com/acexy/golang-toolkit/crypto/hashing"
	"github.com/acexy/golang-toolkit/logger"
	"github.com/acexy/golang-toolkit/util/json"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"gopkg.in/yaml.v3"
	"strconv"
	"sync"
	"time"
)

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
	cc  map[string]*ConfigClient
	nc  map[string]*NamingClient
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

func GetConfigClient(group string) (*ConfigClient, error) {
	if configInstance == nil {
		return nil, errors.New("disabled config client")
	}
	nm.ccl.Lock()
	defer nm.ccl.Unlock()
	v, ok := nm.cc[group]
	if ok {
		return v, nil
	}
	v = &ConfigClient{group: group, watched: make(map[string]*vo.ConfigParam)}
	nm.cc[group] = v
	return v, nil
}

func GetNamingClient(group string) (*NamingClient, error) {
	if namingInstance == nil {
		return nil, errors.New("disabled discover client")
	}
	nm.ncl.Lock()
	defer nm.ncl.Unlock()
	v, ok := nm.nc[group]
	if ok {
		return v, nil
	}
	v = &NamingClient{group: group, registered: make(map[string]vo.RegisterInstanceParam), watched: make(map[string]*vo.SubscribeParam)}
	nm.nc[group] = v
	return v, nil
}

// GetConfigRawContent 获取指定配置的源文件内容
func (c *ConfigClient) GetConfigRawContent(dataId string) (string, error) {
	return configInstance.GetConfig(vo.ConfigParam{DataId: dataId, Group: c.group})
}

// GetConfig 获取指定文件内容并反序列化
func (c *ConfigClient) GetConfig(dataId string, configType ConfigType, value any) error {
	raw, err := c.GetConfigRawContent(dataId)
	if err != nil {
		return nil
	}
	return deserializeConfig(raw, configType, value)
}

// WatchConfig 监听文件变化
func (c *ConfigClient) WatchConfig(dataId string, watch ConfigChangeListener) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	param := vo.ConfigParam{DataId: dataId, Group: c.group}
	watchId := hashing.Md5Hex(json.ToJson(param) + strconv.FormatInt(time.Now().UnixNano(), 10))
	param.OnChange = func(namespace, group, dataId, data string) {
		watch(namespace, group, dataId, data)
	}
	c.watched[watchId] = &param
	return watchId, configInstance.ListenConfig(param)
}

// UnwatchConfig 取消监听文件变化
func (c *ConfigClient) UnwatchConfig(watchId string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.watched[watchId]
	if ok {
		return configInstance.CancelListenConfig(*v)
	}
	return errors.New("bad watchId")
}

// LoadAndWatchConfig 获取并监听配置变化
func (c *ConfigClient) LoadAndWatchConfig(configFiles []*ConfigFileSetting) error {
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

// Register 向注册中心注册临时实例
func (n *NamingClient) Register(ip, serviceName string, port, weight int, metadata map[string]string) (string, error) {
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
	ii := hashing.Md5Hex(json.ToJson(i))
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
func (n *NamingClient) Unregister(instanceId string) (bool, error) {
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
func (n *NamingClient) GetService(serviceName string) (model.Service, error) {
	return namingInstance.GetService(vo.GetServiceParam{
		ServiceName: serviceName,
		GroupName:   n.group,
	})
}

// GetAllServiceInfo 获取指定所有服务的注册信息
func (n *NamingClient) GetAllServiceInfo(namespace string, pageNo, pageSize uint) (model.ServiceList, error) {
	return namingInstance.GetAllServicesInfo(vo.GetAllServiceInfoParam{
		NameSpace: namespace,
		GroupName: n.group,
		PageNo:    uint32(pageNo),
		PageSize:  uint32(pageSize),
	})
}

// GetAllInstances 获取指定服务的所有实例(不论当前是否可用)
func (n *NamingClient) GetAllInstances(serviceName string) ([]model.Instance, error) {
	return namingInstance.SelectAllInstances(vo.SelectAllInstancesParam{ServiceName: serviceName, GroupName: n.group})
}

// GetHealthyInstances 获取指定服务的可用实例
func (n *NamingClient) GetHealthyInstances(serviceName string) ([]model.Instance, error) {
	return namingInstance.SelectInstances(vo.SelectInstancesParam{ServiceName: serviceName, GroupName: n.group})
}

// ChooseOneHealthy 选择一个可用的实例
func (n *NamingClient) ChooseOneHealthy(serviceName string) (*ServiceInstance, error) {
	instance, err := namingInstance.SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{ServiceName: serviceName, GroupName: n.group})
	if err != nil {
		return nil, err
	}
	return &ServiceInstance{InstanceIdentifier: hashing.Md5Hex(json.ToJson(instance)), Instance: instance}, nil
}

// WatchNaming 监控服务的实例变化
func (n *NamingClient) WatchNaming(serviceName string, watch func(instance []model.Instance, err error)) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	param := &vo.SubscribeParam{ServiceName: serviceName, GroupName: n.group}
	watchId := hashing.Md5Hex(json.ToJson(param) + strconv.FormatInt(time.Now().UnixNano(), 10))
	param.SubscribeCallback = watch
	n.watched[watchId] = param
	return watchId, namingInstance.Subscribe(param)
}

// UnwatchNaming 取消监控服务实例变化
func (n *NamingClient) UnwatchNaming(watchId string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	v, ok := n.watched[watchId]
	if !ok {
		return errors.New("bad watchId")
	}
	return namingInstance.Unsubscribe(v)
}
