package nacosstarter

import "testing"

func TestNacosRuntimeSnapshotPublication(t *testing.T) {
	previous := nacosRuntimeState.Swap(nil)
	defer nacosRuntimeState.Store(previous)

	runtime := &nacosRuntime{
		manager: &nacosManager{
			configClient: make(map[string]*ConfigClient),
			namingClient: make(map[string]*NamingClient),
		},
		namespace: "app",
	}
	nacosRuntimeState.Store(runtime)
	if actual := nacosRuntimeState.Load(); actual != runtime || actual.namespace != "app" {
		t.Fatalf("Nacos 运行时快照不完整: %+v", actual)
	}

	nacosRuntimeState.Store(nil)
	if RawConfigInstance() != nil || RawNamingInstance() != nil {
		t.Fatal("摘除运行时快照后不应继续暴露 Nacos 资源")
	}
}
