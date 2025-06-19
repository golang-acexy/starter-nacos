package test

import (
	"fmt"
	"github.com/acexy/golang-toolkit/logger"
	"github.com/acexy/golang-toolkit/sys"
	"github.com/acexy/golang-toolkit/util/json"
	"github.com/golang-acexy/starter-nacos/nacosstarter"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"testing"
	"time"
)

func TestRegister(t *testing.T) {
	nc, _ := nacosstarter.GetNamingClient("DEFAULT_GROUP")
	_, err := nc.Register(nacosstarter.Instance{Ip: "127.0.0.1", ServiceName: "go", Port: 8081, Weight: 1})
	if err != nil {
		fmt.Printf("%+v\n", err)
	}
	sys.ShutdownHolding()
}

func TestRegisterBatch(t *testing.T) {
	nc, _ := nacosstarter.GetNamingClient("DEFAULT_GROUP")
	_, err := nc.RegisterBatch("go", []nacosstarter.InstanceBatch{
		{Ip: "127.0.0.1", Port: 8082, Weight: 1},
		{Ip: "127.0.0.1", Port: 8083, Weight: 1},
	})
	if err != nil {
		fmt.Printf("%+v\n", err)
	}
	sys.ShutdownHolding()
}

func TestGetService(t *testing.T) {
	nc, _ := nacosstarter.GetNamingClient("DEFAULT_GROUP")
	service, err := nc.GetService("go")
	if err != nil {
		println(err)
	}
	fmt.Println(json.ToJsonFormat(service))
	serviceList, err := nc.GetServicePage(1, 40)
	if err != nil {
		println(err)
	}
	fmt.Println(json.ToJsonFormat(serviceList))
}

func TestGetAllInstances(t *testing.T) {
	nc, _ := nacosstarter.GetNamingClient("TEST")
	instances, err := nc.GetAllInstances("go")
	if err != nil {
		println(err)
	}
	fmt.Println(json.ToJsonFormat(instances))
}

func TestGetHealthyInstances(t *testing.T) {
	nc, _ := nacosstarter.GetNamingClient("TEST")
	for i := 1; i <= 10; i++ {
		instances, err := nc.GetHealthyInstances("go")
		if err != nil {
			println(err)
		}
		fmt.Println(json.ToJsonFormat(instances))
		fmt.Println()
		time.Sleep(time.Second * 3)
	}
}

func TestChooseOneHealthyRandom(t *testing.T) {
	nc, _ := nacosstarter.GetNamingClient("DEFAULT_GROUP")
	for i := 1; i <= 30; i++ {
		service, _ := nc.ChooseOneHealthyInstance("go")
		fmt.Println(json.ToJsonFormat(service))
		time.Sleep(time.Second)
	}
}

func TestWatchNaming(t *testing.T) {
	nc, _ := nacosstarter.GetNamingClient("DEFAULT_GROUP")
	watchId, _ := nc.WatchNaming("go", func(instance []model.Instance, err error) {
		logger.Logrus().Println("changed")
		if err != nil {
			logger.Logrus().WithError(err).Errorln("watch naming error")
		} else {
			logger.Logrus().Traceln(json.ToJson(instance))
		}
	})
	go func() {
		time.Sleep(time.Hour * 60)
		fmt.Println("unwatch", nc.UnwatchNaming(watchId))
	}()
	sys.ShutdownHolding()
}
