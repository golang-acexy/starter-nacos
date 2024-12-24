package nacosstarter

import (
	"errors"
	"fmt"
	"github.com/acexy/golang-toolkit/crypto/hashing"
	"github.com/acexy/golang-toolkit/logger"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// Config

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
func (c *ConfigClient) WatchConfig(dataId string, watch func(namespace, group, dataId, data string)) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	param := vo.ConfigParam{DataId: dataId, Group: c.group}
	watchId := hashing.Md5Hex(dataId + c.group)
	_, ok := c.watched[watchId]
	if ok {
		return "", fmt.Errorf("duplicate watch %s", param.DataId)
	}
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
		err := configInstance.CancelListenConfig(*v)
		if err == nil {
			delete(c.watched, watchId)
		}
		return err
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
			return err
		}
	}
	return nil
}