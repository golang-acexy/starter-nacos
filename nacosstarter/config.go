package nacosstarter

import (
	"fmt"
	"sync"

	"github.com/acexy/golang-toolkit/crypto/hashing"
	"github.com/acexy/golang-toolkit/logger"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// ConfigType selects the configuration decoder.
type ConfigType string

// ConfigChangeData receives asynchronous configuration updates; callers must synchronize shared state.
type ConfigChangeData func(namespace, group, dataId, data string)

const (
	// ConfigTypeJson selects JSON decoding.
	ConfigTypeJson ConfigType = "json"
	// ConfigTypeYaml selects YAML decoding.
	ConfigTypeYaml ConfigType = "yaml"
)

// GetConfigRawContent reads a data ID in this group; the configuration capability must be running.
func (c *ConfigClient) GetConfigRawContent(dataId string) (string, error) {
	client, err := currentConfigInstance()
	if err != nil {
		return "", err
	}
	return client.GetConfig(vo.ConfigParam{DataId: dataId, Group: c.group})
}

// GetConfig reads and decodes into a nonnil pointer. Callers must synchronize access to the target.
// Failed YAML decoding leaves the target unchanged; JSON may partially update it on error.
func (c *ConfigClient) GetConfig(dataId string, configType ConfigType, value any) error {
	raw, err := c.GetConfigRawContent(dataId)
	if err != nil {
		return err
	}
	return deserializeConfig(raw, configType, value)
}

// WatchConfig subscribes once per data ID in this group and returns a local watch ID.
// The callback must be nonnil and safe for concurrent asynchronous invocation.
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

// UnwatchConfig cancels a tracked subscription; failed cancellation retains its ID for retry.
// Already dispatched callbacks may still run after cancellation.
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

// LoadAndWatchConfig loads and optionally watches nonnil settings on a best-effort basis.
// Failures are logged without configuration contents and do not stop subsequent files.
// Target writes are serialized within this call; readers and other calls require caller synchronization.
func (c *ConfigClient) LoadAndWatchConfig(configFiles []*ConfigFileSetting) {
	if len(configFiles) == 0 {
		logger.Logrus().Warningln("empty config file")
		return
	}
	var updateMu sync.Mutex
	for _, f := range configFiles {
		updateMu.Lock()
		err := c.GetConfig(f.DataId, f.Type, f.Value)
		updateMu.Unlock()
		if err != nil {
			logger.Logrus().Errorln("cant load config file:", f.DataId)
		}
		if f.Watch {
			_, err = c.WatchConfig(f.DataId, func(namespace, group, dataId, data string) {
				updateMu.Lock()
				defer updateMu.Unlock()
				if decodeErr := deserializeConfig(data, f.Type, f.Value); decodeErr != nil {
					// Decoder errors can contain configuration values; log only the identity.
					logger.Logrus().WithField("dataId", dataId).WithField("group", group).Error("cant deserialize config")
				}
			})
			if err != nil {
				logger.Logrus().WithError(err).Errorln("cant watch config file:", f.DataId)
			}
		}
	}
}
