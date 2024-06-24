package test

import (
	"fmt"
	"github.com/acexy/golang-toolkit/logger"
	"github.com/acexy/golang-toolkit/sys"
	"github.com/acexy/golang-toolkit/util/json"
	"github.com/golang-acexy/starter-nacos/nacosmodule"
	"github.com/golang-acexy/starter-parent/parentmodule/declaration"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"testing"
	"time"
)

var m declaration.Module

var initConfig = new([]JsonConfig)

func init() {
	m = declaration.Module{
		ModuleLoaders: []declaration.ModuleLoader{
			&nacosmodule.NacosModule{
				ServerConfig: &nacosmodule.NacosServerConfig{Services: []constant.ServerConfig{
					{IpAddr: "localhost", Port: 8848},
				}},
				ClientConfig: &nacosmodule.NacosClientConfig{
					ClientConfig: &constant.ClientConfig{
						NamespaceId:         "wallet-dev",
						Username:            "nacos",
						Password:            "nacos",
						LogLevel:            "error",
						LogDir:              "./",
						CacheDir:            "./",
						NotLoadCacheAtStart: true,
					},
				},
				InitConfigSettings: &nacosmodule.InitConfigSettings{
					GroupName: "WALLET",
					ConfigSetting: []*nacosmodule.ConfigFileSetting{
						{DataId: "gateway-flow-rule.json", Type: nacosmodule.ConfigTypeJson, Watch: true, Value: initConfig},
					},
				},
			},
		},
	}
	err := m.Load()
	if err != nil {
		println(err)
		return
	}
}

type YamlConfig struct {
	Server struct {
		Port int `yaml:"port"`
	} `yaml:"server"`
}

type JsonConfig struct {
	Resource        string `json:"resource"`
	ControlBehavior int    `json:"controlBehavior"`
	Count           int    `json:"count"`
	Grade           int    `json:"grade"`
	Strategy        int    `json:"strategy"`
	ClusterMode     bool   `json:"clusterMode"`
}

func TestInitConfig(t *testing.T) {
	fmt.Printf("inited config %+v\n", initConfig)
	time.Sleep(1 * time.Second)
	sys.ShutdownHolding()
}

func TestConfig(t *testing.T) {
	cc, _ := nacosmodule.GetConfigClient("WALLET")

	content, err := cc.GetConfigRawContent("gateway.yml")
	fmt.Println("raw gateway.yml", content, err)

	y := YamlConfig{}
	_ = cc.GetConfig("gateway.yml", nacosmodule.ConfigTypeYaml, &y)
	fmt.Printf("gateway.yml %+v\n", y)

	var j []JsonConfig
	_ = cc.GetConfig("gateway-flow-rule.json", nacosmodule.ConfigTypeJson, &j)
	fmt.Printf("gateway-flow-rule.json %+v\n", j)
	m.UnloadByConfig()
}

func TestWatch(t *testing.T) {
	cc, _ := nacosmodule.GetConfigClient("WALLET")
	_, _ = cc.WatchConfig("gateway-degrade-rule.json", func(namespace, group, dataId, data string) {
		fmt.Println(namespace, group, dataId, data)
	})
	sys.ShutdownHolding()
}

func TestLoadAndWatch(t *testing.T) {
	cc, _ := nacosmodule.GetConfigClient("WALLET")
	var j []JsonConfig

	// 加载指定的配置并自动监听
	_ = cc.LoadAndWatchConfig([]*nacosmodule.ConfigFileSetting{
		{DataId: "gateway-degrade-rule.json", Type: nacosmodule.ConfigTypeJson, Watch: true, Value: &j},
	})
	// loop 通过修改配置查看是否自动变化
	for i := 0; i <= 10; i++ {
		fmt.Printf("%+v\n", j)
		time.Sleep(time.Second * 5)
	}
}

func TestRawNC(t *testing.T) {
	nacosmodule.RawNamingInstance().DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          "1.1.1.1",
		ServiceName: "go",
		Port:        1,
		GroupName:   "WALLET",
	})
}

func TestRegister(t *testing.T) {
	nc, _ := nacosmodule.GetNamingClient("WALLET")
	id, _ := nc.Register("1.1.1.1", "go", 1, 1, nil)
	if id == "" {
		fmt.Println("register failed")
		return
	}
	m.UnloadByConfig()
}

func TestGetService(t *testing.T) {
	nc, _ := nacosmodule.GetNamingClient("WALLET")
	service, err := nc.GetService("account-server")
	if err != nil {
		println(err)
	}
	fmt.Println(json.ToJsonFormat(service))

	serviceList, err := nc.GetAllServiceInfo("wallet-dev", 1, 40)
	if err != nil {
		println(err)
	}
	fmt.Println(json.ToJsonFormat(serviceList))
	m.UnloadByConfig()
}

func TestChooseOneHealthyRandom(t *testing.T) {
	nc, _ := nacosmodule.GetNamingClient("WALLET")
	for i := 1; i <= 30; i++ {
		service, _ := nc.ChooseOneHealthyRandom("account-server")
		fmt.Println(json.ToJsonFormat(service))
		time.Sleep(time.Second * 5)
	}
}

func TestWatchNaming(t *testing.T) {
	nc, _ := nacosmodule.GetNamingClient("WALLET")
	watchId, _ := nc.WatchNaming("account-server", func(instance []model.Instance, err error) {
		if err != nil {
			logger.Logrus().WithError(err).Errorln("watch naming error")
		} else {
			logger.Logrus().Traceln(json.ToJson(instance))
		}
	})
	time.Sleep(time.Second * 30)
	fmt.Println("unwatch", nc.UnwatchNaming(watchId))
	time.Sleep(time.Second * 30)
}
