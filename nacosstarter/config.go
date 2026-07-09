package nacosstarter

import (
	"fmt"

	"github.com/acexy/golang-toolkit/crypto/hashing"
	"github.com/acexy/golang-toolkit/logger"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// Config

type ConfigType string

// ConfigChangeData 文件变动监听回调
type ConfigChangeData func(namespace, group, dataId, data string)

const (
	ConfigTypeJson ConfigType = "json"
	ConfigTypeYaml ConfigType = "yaml"
)

// GetConfigRawContent 获取指定配置的源文件内容
func (c *ConfigClient) GetConfigRawContent(dataId string) (string, error) {
	client, err := currentConfigInstance()
	if err != nil {
		return "", err
	}
	return client.GetConfig(vo.ConfigParam{DataId: dataId, Group: c.group})
}

// GetConfig 获取指定文件内容并反序列化
func (c *ConfigClient) GetConfig(dataId string, configType ConfigType, value any) error {
	raw, err := c.GetConfigRawContent(dataId)
	if err != nil {
		return err
	}
	return deserializeConfig(raw, configType, value)
}

// WatchConfig 监听文件变化
func (c *ConfigClient) WatchConfig(dataId string, watch func(namespace, group, dataId, data string)) (string, error) {
	client, err := currentConfigInstance()
	if err != nil {
		return "", err
	}
	param := vo.ConfigParam{DataId: dataId, Group: c.group}
	watchId := hashing.Md5Hex(dataId + c.group)
	c.mu.Lock()
	_, ok := c.watched[watchId]
	if ok {
		c.mu.Unlock()
		return "", fmt.Errorf("%w: %s", ErrDuplicatedConfigWatch, param.DataId)
	}
	param.OnChange = func(namespace, group, dataId, data string) {
		watch(namespace, group, dataId, data)
	}
	c.watched[watchId] = &param
	c.mu.Unlock()

	err = client.ListenConfig(param)
	if err != nil {
		c.mu.Lock()
		if c.watched[watchId] == &param {
			delete(c.watched, watchId)
		}
		c.mu.Unlock()
	}
	return watchId, err
}

// UnwatchConfig 取消监听文件变化
func (c *ConfigClient) UnwatchConfig(watchId string) error {
	client, err := currentConfigInstance()
	if err != nil {
		return err
	}
	c.mu.Lock()
	v, ok := c.watched[watchId]
	c.mu.Unlock()
	if !ok {
		return ErrBadWatchId
	}

	err = client.CancelListenConfig(*v)
	if err == nil {
		c.mu.Lock()
		if c.watched[watchId] == v {
			delete(c.watched, watchId)
		}
		c.mu.Unlock()
	}
	return err
}

// LoadAndWatchConfig 获取并监听配置变化。该方法是best-effort模式：加载或监听失败仅记录日志，
// 不会中断其余文件的处理，调用方无法直接感知个别文件的失败。
func (c *ConfigClient) LoadAndWatchConfig(configFiles []*ConfigFileSetting) {
	if len(configFiles) == 0 {
		logger.Logrus().Warningln("empty config file")
		return
	}
	for _, f := range configFiles {
		err := c.GetConfig(f.DataId, f.Type, f.Value)
		if err != nil {
			logger.Logrus().Errorln("cant load config file:", f.DataId, err)
		}
		if f.Watch {
			_, err = c.WatchConfig(f.DataId, func(namespace, group, dataId, data string) {
				err = deserializeConfig(data, f.Type, f.Value)
				if err != nil {
					logger.Logrus().WithError(err).Error("cant deserialize content:", data)
				}
			})
			if err != nil {
				logger.Logrus().WithError(err).Errorln("cant watch config file:", f.DataId)
			}
		}
	}
}
