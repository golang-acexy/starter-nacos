package nacosstarter

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

type shutdownNamingClient struct {
	naming_client.INamingClient
	calls *[]string
	param *vo.SubscribeParam
	err   error
}

func (c *shutdownNamingClient) Unsubscribe(p *vo.SubscribeParam) error {
	c.param = p
	*c.calls = append(*c.calls, "unwatch naming")
	return c.err
}
func (c *shutdownNamingClient) DeregisterInstance(vo.DeregisterInstanceParam) (bool, error) {
	*c.calls = append(*c.calls, "deregister")
	return c.err == nil, c.err
}
func (c *shutdownNamingClient) CloseClient() { *c.calls = append(*c.calls, "close naming") }

type shutdownConfigClient struct {
	config_client.IConfigClient
	calls *[]string
	param vo.ConfigParam
	err   error
}

func (c *shutdownConfigClient) CancelListenConfig(p vo.ConfigParam) error {
	c.param = p
	*c.calls = append(*c.calls, "unwatch config")
	return c.err
}
func (c *shutdownConfigClient) CloseClient() { *c.calls = append(*c.calls, "close config") }

func TestStopCancelsManagedWatches(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "cleanup errors"}[failed], func(t *testing.T) {
			var calls []string
			var cleanupErr error
			if failed {
				cleanupErr = errors.New("test cleanup failure")
			}
			naming := &shutdownNamingClient{calls: &calls, err: cleanupErr}
			config := &shutdownConfigClient{calls: &calls, err: cleanupErr}
			namingParam := &vo.SubscribeParam{ServiceName: "service", GroupName: "test"}
			configParam := &vo.ConfigParam{DataId: "config", Group: "test", OnChange: func(_, _, _, _ string) {}}
			n := &NamingClient{watched: map[string]*vo.SubscribeParam{"watch": namingParam}, registered: map[string]vo.RegisterInstanceParam{"instance": {ServiceName: "service"}}}
			c := &ConfigClient{watched: map[string]*vo.ConfigParam{"watch": configParam}}
			state := &nacosRuntime{naming: naming, config: config, manager: &nacosManager{namingClient: map[string]*NamingClient{"test": n}, configClient: map[string]*ConfigClient{"test": c}}}
			previous := nacosRuntimeState.Swap(state)
			nacosLifecycleLock.Lock()
			previousState := nacosState
			nacosState = nacosRunning
			nacosLifecycleLock.Unlock()
			defer func() {
				nacosRuntimeState.Store(previous)
				nacosLifecycleLock.Lock()
				nacosState = previousState
				nacosLifecycleLock.Unlock()
			}()
			graceful, stopped, err := (&NacosStarter{}).Stop(time.Second)
			if !graceful || !stopped || err != nil {
				t.Fatalf("unexpected stop result: %v %v %v", graceful, stopped, err)
			}
			if naming.param != namingParam {
				t.Fatal("SDK unsubscribe must receive the original parameter pointer")
			}
			if config.param.DataId != configParam.DataId || config.param.Group != configParam.Group || config.param.OnChange == nil {
				t.Fatal("configuration cancellation parameters changed")
			}
			if len(n.watched) != 0 || len(c.watched) != 0 || len(n.registered) != 0 {
				t.Fatal("retained wrappers still hold subscription or registration records")
			}
			want := []string{"unwatch naming", "unwatch config", "deregister", "close naming", "close config"}
			if !reflect.DeepEqual(calls, want) {
				t.Fatalf("shutdown sequence = %v, want %v", calls, want)
			}
			if RawConfigInstance() != nil || RawNamingInstance() != nil {
				t.Fatal("closed runtime remains published")
			}
		})
	}
}

type blockingShutdownConfigClient struct {
	config_client.IConfigClient
	entered chan struct{}
	release chan struct{}
	closed  chan struct{}
}

func (c *blockingShutdownConfigClient) CancelListenConfig(vo.ConfigParam) error {
	close(c.entered)
	<-c.release
	return nil
}
func (c *blockingShutdownConfigClient) CloseClient() { close(c.closed) }

func TestStopTimeoutContinuesWatchCleanup(t *testing.T) {
	sdk := &blockingShutdownConfigClient{entered: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
	client := &ConfigClient{watched: map[string]*vo.ConfigParam{"watch": {DataId: "config", Group: "test"}}}
	state := &nacosRuntime{config: sdk, manager: &nacosManager{configClient: map[string]*ConfigClient{"test": client}}}
	previous := nacosRuntimeState.Swap(state)
	nacosLifecycleLock.Lock()
	previousState := nacosState
	nacosState = nacosRunning
	nacosLifecycleLock.Unlock()
	defer func() {
		nacosRuntimeState.Store(previous)
		nacosLifecycleLock.Lock()
		nacosState = previousState
		nacosLifecycleLock.Unlock()
	}()
	graceful, stopped, err := (&NacosStarter{}).Stop(time.Millisecond)
	close(sdk.release)
	select {
	case <-sdk.closed:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not continue after timeout")
	}
	deadline := time.Now().Add(time.Second)
	for {
		nacosLifecycleLock.Lock()
		done := nacosState == nacosStopped
		nacosLifecycleLock.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lifecycle remained stopping")
		}
		time.Sleep(time.Millisecond)
	}
	if graceful || !stopped || !errors.Is(err, ErrNacosStopTimeout) {
		t.Fatalf("unexpected timeout result: %v %v %v", graceful, stopped, err)
	}
	if len(client.watched) != 0 {
		t.Fatal("timed-out shutdown retained watch records")
	}
	if RawConfigInstance() != nil {
		t.Fatal("timed-out runtime remained published")
	}
}
