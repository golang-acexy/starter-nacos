package nacosstarter

import (
	"github.com/acexy/golang-toolkit/crypto/hashing"
	"github.com/acexy/golang-toolkit/logger"
	"github.com/acexy/golang-toolkit/math/conversion"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// Naming

// Register 向注册中心注册临时实例
// 同一group、serviceName、ip、port生成的实例标识重复注册时，会覆盖本地注册记录。
func (n *NamingClient) Register(instance Instance) (string, error) {
	client, err := currentNamingInstance()
	if err != nil {
		return "", err
	}
	var flag bool
	var id string
	param := vo.RegisterInstanceParam{
		Ip:          instance.Ip,
		ServiceName: instance.ServiceName,
		Port:        uint64(instance.Port),
		Weight:      float64(instance.Weight),
		Enable:      true,
		Healthy:     true,
		Metadata:    instance.Metadata,
		GroupName:   n.group,
		Ephemeral:   true,
	}
	flag, err = client.RegisterInstance(param)
	if err != nil {
		return "", err
	}
	if !flag {
		return "", ErrRegisterInstanceFailed
	}
	logger.Logrus().Traceln("registered ip", param.Ip, "port", param.Port, "service", param.ServiceName)
	id = buildInstanceIdentifier(param.GroupName, param.ServiceName, param.Ip, param.Port)
	n.mu.Lock()
	n.registered[id] = param
	n.mu.Unlock()
	return id, nil
}

func (n *NamingClient) RegisterBatch(serviceName string, instances []InstanceBatch) ([]string, error) {
	client, err := currentNamingInstance()
	if err != nil {
		return nil, err
	}
	if len(instances) == 0 {
		return nil, ErrEmptyInstance
	}
	var flag bool
	var param vo.BatchRegisterInstanceParam
	var instanceParam []vo.RegisterInstanceParam
	var ids []string
	for _, v := range instances {
		instanceParam = append(instanceParam, vo.RegisterInstanceParam{
			Ip:          v.Ip,
			Port:        uint64(v.Port),
			Weight:      float64(v.Weight),
			Enable:      true,
			Healthy:     true,
			Metadata:    v.Metadata,
			Ephemeral:   true,
			ServiceName: serviceName,
			GroupName:   n.group,
		})
		ids = append(ids, buildInstanceIdentifier(n.group, serviceName, v.Ip, uint64(v.Port)))
	}
	param.Instances = instanceParam
	param.GroupName = n.group
	param.ServiceName = serviceName

	flag, err = client.BatchRegisterInstance(param)
	if err != nil {
		return nil, err
	}
	if !flag {
		return nil, ErrRegisterInstanceFailed
	}
	n.mu.Lock()
	for i, v := range ids {
		n.registered[v] = instanceParam[i]
	}
	n.mu.Unlock()
	return ids, nil
}

// Unregister 向注册中心注销实例
func (n *NamingClient) Unregister(instanceId string) (bool, error) {
	client, err := currentNamingInstance()
	if err != nil {
		return false, err
	}
	n.mu.Lock()
	v, ok := n.registered[instanceId]
	n.mu.Unlock()
	if !ok {
		return false, ErrBadInstanceId
	}
	param := vo.DeregisterInstanceParam{
		Ip:          v.Ip,
		Port:        v.Port,
		Cluster:     v.ClusterName,
		ServiceName: v.ServiceName,
		GroupName:   v.GroupName,
		Ephemeral:   v.Ephemeral,
	}
	flag, err := client.DeregisterInstance(param)
	if err != nil {
		return false, err
	}
	if !flag {
		return false, nil
	}
	n.mu.Lock()
	delete(n.registered, instanceId)
	n.mu.Unlock()
	logger.Logrus().Traceln("unregistered ip", param.Ip, "port", param.Port, "service", param.ServiceName)
	return true, nil
}

// snapshotNamingClients 返回当前所有NamingClient的快照，仅在Stop()关闭流程中调用。
func snapshotNamingClients(manager *nacosManager) []*NamingClient {
	if manager == nil {
		return nil
	}
	manager.namingLocker.Lock()
	defer manager.namingLocker.Unlock()
	clients := make([]*NamingClient, 0, len(manager.namingClient))
	for _, namingClient := range manager.namingClient {
		clients = append(clients, namingClient)
	}
	return clients
}

func (n *NamingClient) snapshotRegistered() map[string]vo.RegisterInstanceParam {
	n.mu.Lock()
	defer n.mu.Unlock()
	registered := make(map[string]vo.RegisterInstanceParam, len(n.registered))
	for id, instance := range n.registered {
		registered[id] = instance
	}
	return registered
}

// GetService 获取指定服务的概要信息
func (n *NamingClient) GetService(serviceName string) (model.Service, error) {
	client, err := currentNamingInstance()
	if err != nil {
		return model.Service{}, err
	}
	return client.GetService(vo.GetServiceParam{
		ServiceName: serviceName,
		GroupName:   n.group,
	})
}

// GetServicePage 获取指定服务的注册信息
func (n *NamingClient) GetServicePage(pageNo, pageSize uint) (model.ServiceList, error) {
	runtime := nacosRuntimeState.Load()
	if runtime == nil || runtime.naming == nil {
		return model.ServiceList{}, ErrDisabledDiscoveryClient
	}
	return runtime.naming.GetAllServicesInfo(vo.GetAllServiceInfoParam{
		NameSpace: runtime.namespace,
		GroupName: n.group,
		PageNo:    uint32(pageNo),
		PageSize:  uint32(pageSize),
	})
}

// GetAllInstances 获取指定服务的所有实例(不论当前是否可用)
func (n *NamingClient) GetAllInstances(serviceName string) ([]RegisteredInstance, error) {
	client, err := currentNamingInstance()
	if err != nil {
		return nil, err
	}
	instances, err := client.SelectAllInstances(vo.SelectAllInstancesParam{ServiceName: serviceName, GroupName: n.group})
	if err != nil {
		return nil, err
	}
	var result []RegisteredInstance
	for _, v := range instances {
		result = append(result, RegisteredInstance{InstanceIdentifier: buildInstanceIdentifier(n.group, serviceName, v.Ip, v.Port), Instance: v})
	}
	return result, nil
}

// GetHealthyInstances 获取指定服务的可用实例
func (n *NamingClient) GetHealthyInstances(serviceName string) ([]RegisteredInstance, error) {
	client, err := currentNamingInstance()
	if err != nil {
		return nil, err
	}
	instances, err := client.SelectInstances(vo.SelectInstancesParam{ServiceName: serviceName, GroupName: n.group, HealthyOnly: true})
	if err != nil {
		return nil, err
	}
	var result []RegisteredInstance
	for _, v := range instances {
		result = append(result, RegisteredInstance{InstanceIdentifier: buildInstanceIdentifier(n.group, serviceName, v.Ip, v.Port), Instance: v})
	}
	return result, nil
}

// ChooseOneHealthyInstance 选择一个可用的实例
func (n *NamingClient) ChooseOneHealthyInstance(serviceName string) (*RegisteredInstance, error) {
	client, err := currentNamingInstance()
	if err != nil {
		return nil, err
	}
	instance, err := client.SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{ServiceName: serviceName, GroupName: n.group})
	if err != nil {
		return nil, err
	}
	return &RegisteredInstance{InstanceIdentifier: buildInstanceIdentifier(n.group, serviceName, instance.Ip, instance.Port), Instance: *instance}, nil
}

// WatchNaming 监控服务的实例变化
// * 如果UpdateCacheWhenEmpty=false 当前服务只有一个实例时，不会触发监听
func (n *NamingClient) WatchNaming(serviceName string, watch func(instance []model.Instance, err error)) (string, error) {
	client, err := currentNamingInstance()
	if err != nil {
		return "", err
	}
	param := &vo.SubscribeParam{ServiceName: serviceName, GroupName: n.group}
	watchId := hashing.Md5Hex(serviceName + n.group)
	n.mu.Lock()
	_, ok := n.watched[watchId]
	if ok {
		n.mu.Unlock()
		return "", ErrDuplicatedNamingWatch
	}
	param.SubscribeCallback = watch
	n.watched[watchId] = param
	n.mu.Unlock()

	err = client.Subscribe(param)
	if err != nil {
		n.mu.Lock()
		if n.watched[watchId] == param {
			delete(n.watched, watchId)
		}
		n.mu.Unlock()
	}
	return watchId, err
}

// UnwatchNaming 取消监控服务实例变化
func (n *NamingClient) UnwatchNaming(watchId string) error {
	client, err := currentNamingInstance()
	if err != nil {
		return err
	}
	n.mu.Lock()
	v, ok := n.watched[watchId]
	n.mu.Unlock()
	if !ok {
		return ErrBadWatchId
	}

	err = client.Unsubscribe(v)
	if err == nil {
		n.mu.Lock()
		if n.watched[watchId] == v {
			delete(n.watched, watchId)
		}
		n.mu.Unlock()
	}
	return err
}

func buildInstanceIdentifier(group, serviceName, ip string, port uint64) string {
	return hashing.Md5Hex(group + ":" + serviceName + ":" + ip + ":" + conversion.FromUint64(port))
}
