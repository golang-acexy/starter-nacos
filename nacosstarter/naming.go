package nacosstarter

import (
	"errors"
	"github.com/acexy/golang-toolkit/crypto/hashing"
	"github.com/acexy/golang-toolkit/logger"
	"github.com/acexy/golang-toolkit/math/conversion"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// Naming

// Register 向注册中心注册临时实例
// 一个NamingClient只能注册一个实例，重复注册将出现替换
func (n *NamingClient) Register(instance Instance) (string, error) {
	var err error
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
	flag, err = namingInstance.RegisterInstance(param)
	if err == nil && flag {
		logger.Logrus().Traceln("registered ip", param.Ip, "port", param.Port, "service", param.ServiceName)
		id = hashing.Md5Hex(param.Ip + conversion.FromUint64(param.Port))
		n.registered[id] = param
	}
	return id, err
}

func (n *NamingClient) RegisterBatch(serviceName string, instances []InstanceBatch) ([]string, error) {
	if len(instances) == 0 {
		return nil, errors.New("empty instance")
	}
	var err error
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
		ids = append(ids, hashing.Md5Hex(v.Ip+conversion.FromUint(v.Port)))
	}
	param.Instances = instanceParam
	param.GroupName = n.group
	param.ServiceName = serviceName

	flag, err = namingInstance.BatchRegisterInstance(param)
	if err == nil && flag {
		for i, v := range ids {
			n.registered[v] = instanceParam[i]
		}
		return ids, err
	}
	return nil, err
}

// Unregister 向注册中心注销实例
func (n *NamingClient) Unregister(instanceId string) (bool, error) {
	v, ok := n.registered[instanceId]
	if !ok {
		return false, errors.New("bad instanceId")
	}
	param := vo.DeregisterInstanceParam{
		Ip:          v.Ip,
		Port:        v.Port,
		Cluster:     v.ClusterName,
		ServiceName: v.ServiceName,
		GroupName:   v.GroupName,
		Ephemeral:   v.Ephemeral,
	}
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

// GetService 获取指定服务的概要信息
func (n *NamingClient) GetService(serviceName string) (model.Service, error) {
	return namingInstance.GetService(vo.GetServiceParam{
		ServiceName: serviceName,
		GroupName:   n.group,
	})
}

// GetServicePage 获取指定服务的注册信息
func (n *NamingClient) GetServicePage(pageNo, pageSize uint) (model.ServiceList, error) {
	return namingInstance.GetAllServicesInfo(vo.GetAllServiceInfoParam{
		NameSpace: namespace,
		GroupName: n.group,
		PageNo:    uint32(pageNo),
		PageSize:  uint32(pageSize),
	})
}

// GetAllInstances 获取指定服务的所有实例(不论当前是否可用)
func (n *NamingClient) GetAllInstances(serviceName string) ([]RegisteredInstance, error) {
	instances, err := namingInstance.SelectAllInstances(vo.SelectAllInstancesParam{ServiceName: serviceName, GroupName: n.group})
	if err != nil {
		return nil, err
	}
	var result []RegisteredInstance
	for _, v := range instances {
		result = append(result, RegisteredInstance{InstanceIdentifier: hashing.Md5Hex(v.Ip + conversion.FromUint64(v.Port)), Instance: v})
	}
	return result, nil
}

// GetHealthyInstances 获取指定服务的可用实例
func (n *NamingClient) GetHealthyInstances(serviceName string) ([]RegisteredInstance, error) {
	instances, err := namingInstance.SelectInstances(vo.SelectInstancesParam{ServiceName: serviceName, GroupName: n.group, HealthyOnly: true})
	if err != nil {
		return nil, err
	}
	var result []RegisteredInstance
	for _, v := range instances {
		result = append(result, RegisteredInstance{InstanceIdentifier: hashing.Md5Hex(v.Ip + conversion.FromUint64(v.Port)), Instance: v})
	}
	return result, nil
}

// ChooseOneHealthyInstance 选择一个可用的实例
func (n *NamingClient) ChooseOneHealthyInstance(serviceName string) (*RegisteredInstance, error) {
	instance, err := namingInstance.SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{ServiceName: serviceName, GroupName: n.group})
	if err != nil {
		return nil, err
	}
	return &RegisteredInstance{InstanceIdentifier: hashing.Md5Hex(instance.Ip + conversion.FromUint64(instance.Port)), Instance: *instance}, nil
}

// WatchNaming 监控服务的实例变化
func (n *NamingClient) WatchNaming(serviceName string, watch func(instance []model.Instance, err error)) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	param := &vo.SubscribeParam{ServiceName: serviceName, GroupName: n.group}
	watchId := hashing.Md5Hex(serviceName)
	_, ok := n.watched[watchId]
	if ok {
		return watchId, errors.New("duplicated watchId")
	}
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
	err := namingInstance.Unsubscribe(v)
	if err != nil {
		delete(n.watched, watchId)
	}
	return err
}
