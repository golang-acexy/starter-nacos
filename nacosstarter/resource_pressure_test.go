package nacosstarter

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
	"weak"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// TestNacosResourcePressure uses only a unique test group and removes its configuration.
func TestNacosResourcePressure(t *testing.T) {
	if os.Getenv("NACOS_MEMORY_STRESS") != "1" {
		t.Skip("requires explicitly configured Nacos")
	}
	group := fmt.Sprintf("starter-pressure-%d", time.Now().UnixNano())
	var configs []weak.Pointer[config_client.ConfigClient]
	var namings []weak.Pointer[naming_client.NamingClient]
	var callbackPayloads []weak.Pointer[[262144]byte]
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	baseline := runtime.NumGoroutine()
	for round := 0; round < 6; round++ {
		func() {
			dir := t.TempDir()
			starter := &NacosStarter{Config: NacosConfig{
				ServerConfig: &NacosServerConfig{Services: []constant.ServerConfig{{IpAddr: os.Getenv("NACOS_PRESSURE_HOST"), Port: 8848}}},
				ClientConfig: &NacosClientConfig{ClientConfig: &constant.ClientConfig{Username: os.Getenv("NACOS_PRESSURE_USER"), Password: os.Getenv("NACOS_PRESSURE_PASSWORD"), TimeoutMs: 5000, LogLevel: "error", LogDir: dir, CacheDir: dir, NotLoadCacheAtStart: true}},
			}}
			if _, err := starter.Start(); err != nil {
				t.Fatal("Nacos start failed; inspect connection settings")
			}
			defer func() {
				if _, _, err := starter.Stop(15 * time.Second); err != nil {
					t.Error("Nacos stop failed")
				}
			}()
			raw := RawConfigInstance()
			configs = append(configs, weak.Make(raw.(*config_client.ConfigClient)))
			namings = append(namings, weak.Make(RawNamingInstance().(*naming_client.NamingClient)))
			c, _ := GetConfigClient(group)
			n, _ := GetNamingClient(group)
			dataID := fmt.Sprintf("pressure-%d", round)
			if ok, err := raw.PublishConfig(vo.ConfigParam{DataId: dataID, Group: group, Content: `{"value":1}`, Type: "json"}); err != nil || !ok {
				t.Fatal("test configuration publish failed")
			}
			defer func() {
				if ok, err := raw.DeleteConfig(vo.ConfigParam{DataId: dataID, Group: group}); err != nil || !ok {
					t.Error("test configuration cleanup failed")
				}
			}()
			changes := make(chan string, 8)
			id, err := c.WatchConfig(dataID, func(_, _, _, data string) {
				select {
				case changes <- data:
				default:
				}
			})
			if err != nil {
				t.Fatal("configuration watch failed")
			}
			if ok, err := raw.PublishConfig(vo.ConfigParam{DataId: dataID, Group: group, Content: `{"value":2}`, Type: "json"}); err != nil || !ok {
				t.Fatal("test configuration update failed")
			}
			deadline := time.After(12 * time.Second)
		waitChange:
			for {
				select {
				case data := <-changes:
					if strings.Contains(data, `2`) {
						break waitChange
					}
				case <-deadline:
					t.Fatal("configuration callback timeout")
				}
			}
			if err := c.UnwatchConfig(id); err != nil {
				t.Fatal(err)
			}
			service := fmt.Sprintf("pressure-service-%d", round)
			registration, err := n.Register(Instance{ServiceName: service, Ip: "127.0.0.1", Port: 39000 + uint(round), Weight: 1})
			if err != nil {
				t.Fatal("test instance registration failed")
			}
			defer func() {
				if ok, err := n.Unregister(registration); err != nil || !ok {
					t.Error("test instance cleanup failed")
				}
			}()
			start := time.Now()
			for i := 0; i < 100; i++ {
				wid, err := c.WatchConfig(dataID, func(_, _, _, _ string) {})
				if err != nil {
					t.Fatal(err)
				}
				if err = c.UnwatchConfig(wid); err != nil {
					t.Fatal(err)
				}
				nid, err := n.WatchNaming(service, func(_ []model.Instance, _ error) {})
				if err != nil {
					t.Fatal("naming watch failed")
				}
				if err = n.UnwatchNaming(nid); err != nil {
					t.Fatal("naming unwatch failed")
				}
				if _, err = c.GetConfigRawContent(dataID); err != nil {
					t.Fatal("configuration read failed")
				}
				if _, err = n.GetAllInstances(service); err != nil {
					t.Fatal("instance query failed")
				}
			}
			payload := new([262144]byte)
			payload[0] = byte(round)
			callbackPayloads = append(callbackPayloads, weak.Make(payload))
			retainedID, err := n.WatchNaming(service, func(_ []model.Instance, _ error) { runtime.KeepAlive(payload) })
			if err != nil {
				t.Fatal("retained naming watch failed")
			}
			if round%2 == 0 {
				if err := n.UnwatchNaming(retainedID); err != nil {
					t.Fatal("payload watch cleanup failed")
				}
			}
			t.Logf("round=%d 100 read/query/watch/unwatch cycles=%s tracked_config=%d tracked_naming=%d", round, time.Since(start), len(c.watched), len(n.watched))
		}()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		t.Logf("after stop round=%d goroutines=%d heap=%d", round, runtime.NumGoroutine(), m.HeapAlloc)
	}
	for i := 0; i < 15; i++ {
		runtime.GC()
		time.Sleep(time.Second)
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	configAlive, namingAlive := 0, 0
	for _, p := range configs {
		if p.Value() != nil {
			configAlive++
		}
	}
	for _, p := range namings {
		if p.Value() != nil {
			namingAlive++
		}
	}
	alivePayloads := 0
	for _, p := range callbackPayloads {
		if p.Value() != nil {
			alivePayloads++
		}
	}
	if alivePayloads != 0 {
		t.Errorf("shutdown retained %d managed callback payloads", alivePayloads)
	}
	t.Logf("live stopped naming callback payloads=%d bytes=%d", alivePayloads, alivePayloads*262144)
	t.Logf("settled goroutines=%d->%d heap=%d->%d liveSDK config=%d naming=%d", baseline, runtime.NumGoroutine(), before.HeapAlloc, after.HeapAlloc, configAlive, namingAlive)
	if path := os.Getenv("NACOS_PRESSURE_STACK"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		_ = pprof.Lookup("goroutine").WriteTo(f, 2)
	}
}

type pressureNamingClient struct {
	naming_client.INamingClient
	instances []model.Instance
}

func (c *pressureNamingClient) SelectAllInstances(vo.SelectAllInstancesParam) ([]model.Instance, error) {
	return c.instances, nil
}

func BenchmarkNacosInstanceMapping(b *testing.B) {
	for _, size := range []int{1, 100, 1000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			sdk := &pressureNamingClient{instances: make([]model.Instance, size)}
			for i := range sdk.instances {
				sdk.instances[i] = model.Instance{Ip: "127.0.0.1", Port: uint64(10000 + i)}
			}
			previous := nacosRuntimeState.Swap(&nacosRuntime{naming: sdk})
			defer nacosRuntimeState.Store(previous)
			client := &NamingClient{group: DefaultGroup}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := client.GetAllInstances("pressure")
				if err != nil || len(result) != size {
					b.Fatal("unexpected mapping result")
				}
			}
		})
	}
}
