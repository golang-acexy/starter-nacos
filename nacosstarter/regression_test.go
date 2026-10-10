package nacosstarter

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/acexy/golang-toolkit/logger"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

type configRegressionClient struct {
	config_client.IConfigClient
	params  []vo.ConfigParam
	content string
}

func (c *configRegressionClient) GetConfig(vo.ConfigParam) (string, error) {
	if c.content != "" {
		return c.content, nil
	}
	return `{"Count":0}`, nil
}

func (c *configRegressionClient) ListenConfig(param vo.ConfigParam) error {
	c.params = append(c.params, param)
	// Invoke immediately to verify that subscription status and decode status are independent.
	param.OnChange("", param.Group, param.DataId, `invalid JSON`)
	return nil
}

func setRegressionRuntime(t *testing.T, runtime *nacosRuntime) {
	t.Helper()
	previous := nacosRuntimeState.Swap(runtime)
	t.Cleanup(func() { nacosRuntimeState.Store(previous) })
}

func TestLoadAndWatchConfigSerializesUpdates(t *testing.T) {
	sdk := &configRegressionClient{}
	setRegressionRuntime(t, &nacosRuntime{config: sdk})
	client := &ConfigClient{group: DefaultGroup, watched: make(map[string]*vo.ConfigParam)}
	value := struct{ Count int }{}
	client.LoadAndWatchConfig([]*ConfigFileSetting{
		{DataId: "first", Type: ConfigTypeJson, Watch: true, Value: &value},
		{DataId: "second", Type: ConfigTypeJson, Watch: true, Value: &value},
	})
	if len(sdk.params) != 2 || len(client.watched) != 2 {
		t.Fatal("decode failures must not discard successful subscriptions")
	}
	var workers sync.WaitGroup
	for i := 0; i < 40; i++ {
		workers.Add(1)
		go func(param vo.ConfigParam) {
			defer workers.Done()
			param.OnChange("", param.Group, param.DataId, `{"Count":7}`)
		}(sdk.params[i%2])
	}
	workers.Wait()
	if value.Count != 7 {
		t.Fatalf("configuration update was not applied: %+v", value)
	}
}

func TestConfigDecodeLogsExcludeContent(t *testing.T) {
	const marker = "CONFIDENTIAL_TEST_MARKER"
	sdk := &configRegressionClient{content: "Count: " + marker}
	setRegressionRuntime(t, &nacosRuntime{config: sdk})
	client := &ConfigClient{group: DefaultGroup, watched: make(map[string]*vo.ConfigParam)}
	var output bytes.Buffer
	log := logger.Logrus()
	previous := log.Out
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	value := struct{ Count int }{}
	client.LoadAndWatchConfig([]*ConfigFileSetting{
		{DataId: "settings", Type: ConfigTypeYaml, Watch: true, Value: &value},
	})
	param := sdk.params[0]
	param.OnChange("", param.Group, param.DataId, "Count: "+marker)
	if strings.Contains(output.String(), marker) {
		t.Fatal("configuration content or decoder values leaked into logs")
	}
	if !strings.Contains(output.String(), "settings") {
		t.Fatal("configuration identity missing from failure log")
	}
}

type namingRegressionClient struct {
	naming_client.INamingClient
	callbacks    map[*vo.SubscribeParam]bool
	subscribeErr error
	cleanupErr   error
	cleaned      *vo.SubscribeParam
}

func (c *namingRegressionClient) Subscribe(param *vo.SubscribeParam) error {
	c.callbacks[param] = true
	return c.subscribeErr
}

func (c *namingRegressionClient) Unsubscribe(param *vo.SubscribeParam) error {
	c.cleaned = param
	if c.cleanupErr == nil {
		delete(c.callbacks, param)
	}
	return c.cleanupErr
}

func TestNamingSubscribeFailureCleanup(t *testing.T) {
	for _, cleanupFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "cleanup succeeds", true: "cleanup requires retry"}[cleanupFails], func(t *testing.T) {
			subscribeErr := errors.New("subscribe failed")
			cleanupErr := errors.New("unsubscribe failed")
			sdk := &namingRegressionClient{callbacks: make(map[*vo.SubscribeParam]bool), subscribeErr: subscribeErr}
			if cleanupFails {
				sdk.cleanupErr = cleanupErr
			}
			setRegressionRuntime(t, &nacosRuntime{naming: sdk})
			client := &NamingClient{group: DefaultGroup, watched: make(map[string]*vo.SubscribeParam)}
			id, err := client.WatchNaming("service", func([]model.Instance, error) {})
			if !errors.Is(err, subscribeErr) || sdk.cleaned == nil {
				t.Fatal("subscription failure must preserve the original error and attempt cleanup")
			}
			if cleanupFails {
				if !errors.Is(err, cleanupErr) || client.watched[id] != sdk.cleaned {
					t.Fatal("failed cleanup must retain its exact subscription and expose both errors")
				}
				sdk.cleanupErr = nil
				if err := client.UnwatchNaming(id); err != nil {
					t.Fatalf("cleanup retry failed: %v", err)
				}
			}
			if len(sdk.callbacks) != 0 || len(client.watched) != 0 {
				t.Fatal("subscription callback or local entry remained after cleanup")
			}
			sdk.subscribeErr = nil
			id, err = client.WatchNaming("service", func([]model.Instance, error) {})
			if err != nil || len(sdk.callbacks) != 1 {
				t.Fatalf("subscription retry failed: %v", err)
			}
			if err := client.UnwatchNaming(id); err != nil || len(sdk.callbacks) != 0 {
				t.Fatalf("subscription cancellation failed: %v", err)
			}
		})
	}
}
