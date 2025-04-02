package test

import (
	"github.com/golang-acexy/starter-nacos/nacosstarter"
	"github.com/golang-acexy/starter-parent/parent"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"testing"
)

var loader *parent.StarterLoader
var initConfig = new([]JsonConfig)

func init() {
	loader = parent.NewStarterLoader([]parent.Starter{
		&nacosstarter.NacosStarter{
			Config: nacosstarter.NacosConfig{
				ServerConfig: &nacosstarter.NacosServerConfig{Services: []constant.ServerConfig{
					{IpAddr: "localhost", Port: 8848},
				}},
				ClientConfig: &nacosstarter.NacosClientConfig{
					ClientConfig: &constant.ClientConfig{
						NamespaceId:         "demo",
						Username:            "nacos",
						Password:            "nacos",
						LogLevel:            "debug",
						LogDir:              "./",
						CacheDir:            "./",
						NotLoadCacheAtStart: true,
					},
				},
				InitConfigSettings: &nacosstarter.InitConfigSettings{
					GroupName: "CLOUD",
					ConfigSetting: []*nacosstarter.ConfigFileSetting{
						{DataId: "flow-rule.json", Type: nacosstarter.ConfigTypeJson, Watch: true, Value: initConfig},
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

func TestRawNC(t *testing.T) {
	nacosstarter.RawNamingInstance().DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          "1.1.1.1",
		ServiceName: "go",
		Port:        1,
		GroupName:   "WALLET",
	})
}
