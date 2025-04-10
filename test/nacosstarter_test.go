package test

import (
	"fmt"
	"github.com/acexy/golang-toolkit/sys"
	"github.com/acexy/golang-toolkit/util/json"
	"github.com/golang-acexy/starter-nacos/nacosstarter"
	"github.com/golang-acexy/starter-parent/parent"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"testing"
	"time"
)

var loader *parent.StarterLoader

func init() {
	loader = parent.NewStarterLoader([]parent.Starter{
		&nacosstarter.NacosStarter{
			Config: nacosstarter.NacosConfig{
				ServerConfig: &nacosstarter.NacosServerConfig{Services: []constant.ServerConfig{
					{IpAddr: "localhost", Port: 8848},
				}},
				ClientConfig: &nacosstarter.NacosClientConfig{
					ClientConfig: &constant.ClientConfig{
						//NamespaceId:         "public",
						Username:            "nacos",
						Password:            "nacos",
						LogLevel:            "debug",
						LogDir:              "./",
						CacheDir:            "./",
						NotLoadCacheAtStart: true,
					},
				},
				InitConfigSettings: &nacosstarter.InitConfigSettings{
					GroupName: "TEST",
					ConfigSetting: []*nacosstarter.ConfigFileSetting{
						{DataId: "json.json", Type: nacosstarter.ConfigTypeJson, Watch: true, Value: initJsonConfig},
					},
				},
			},
		},
	})
	err := loader.Start()
	if err != nil {
		println(err)
		return
	}
}

func TestGetConfig(t *testing.T) {
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				fmt.Println(json.ToJson(initJsonConfig))
				time.Sleep(time.Second * 2)
			}
		}
	}()
	sys.ShutdownCallback(func() {
		done <- struct{}{}
	})
}

func TestRawNC(t *testing.T) {
	nacosstarter.RawNamingInstance().DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          "1.1.1.1",
		ServiceName: "go",
		Port:        1,
		GroupName:   "WALLET",
	})
}
