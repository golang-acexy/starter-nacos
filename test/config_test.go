package test

import (
	"fmt"
	"github.com/acexy/golang-toolkit/sys"
	"github.com/golang-acexy/starter-nacos/nacosstarter"
	"testing"
	"time"
)

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
}

func TestConfig(t *testing.T) {
	cc, _ := nacosstarter.GetConfigClient("CLOUD")
	content, err := cc.GetConfigRawContent("demo-gateway.yml")
	fmt.Println("raw gateway.yml", content, err)

	y := YamlConfig{}
	_ = cc.GetConfig("demo-gateway.yml", nacosstarter.ConfigTypeYaml, &y)
	fmt.Printf("gateway.yml %+v\n", y)

	var j []JsonConfig
	_ = cc.GetConfig("flow-rule.json", nacosstarter.ConfigTypeJson, &j)
	fmt.Printf("flow-rule.json %+v\n", j)
	_, _ = loader.StopBySetting()
}

func TestWatch(t *testing.T) {
	cc, _ := nacosstarter.GetConfigClient("CLOUD")
	watchId, err := cc.WatchConfig("demo-gateway.yml", func(namespace, group, dataId, data string) {
		fmt.Println(namespace, group, dataId, data)
	})
	if err != nil {
		fmt.Printf("watch config failed %+v\n", err)
		return
	}
	time.Sleep(10 * time.Second)
	fmt.Println("取消监听文件变化")
	_ = cc.UnwatchConfig(watchId)
	sys.ShutdownHolding()
}

func TestLoadAndWatch(t *testing.T) {
	cc, _ := nacosstarter.GetConfigClient("CLOUD")
	var j YamlConfig
	// 加载指定的配置并自动监听
	_ = cc.LoadAndWatchConfig([]*nacosstarter.ConfigFileSetting{
		{DataId: "demo-gateway.yml", Type: nacosstarter.ConfigTypeYaml, Watch: true, Value: &j},
	})
	// loop 通过管理中心修改配置 查看是否自动变化
	for i := 0; i <= 10; i++ {
		fmt.Printf("%+v\n", j)
		time.Sleep(time.Second * 5)
	}
}
