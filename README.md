# starter-nacos

`starter-nacos` provides Nacos configuration and service discovery integration for the golang-acexy starter ecosystem. It is intended to be started and stopped by `starter-parent`.

## Ecosystem Role

This starter supplies shared configuration and naming clients. It can initialize application configuration before dependent components start and provides discovery data to integrations such as `starter-grpc` resolvers.

## Requirements

- Go `1.25.8`
- Nacos server, for example `localhost:8848`
- `github.com/golang-acexy/starter-parent`

## Installation

```bash
go get github.com/golang-acexy/starter-nacos
```

## Parent Loader Usage

```go
loader := parent.InitStarterLoader([]parent.Starter{
    &nacosstarter.NacosStarter{
        Config: nacosstarter.NacosConfig{
            ServerConfig: &nacosstarter.NacosServerConfig{
                Services: []constant.ServerConfig{{IpAddr: "localhost", Port: 8848}},
            },
            ClientConfig: &nacosstarter.NacosClientConfig{
                ClientConfig: &constant.ClientConfig{
                    Username: "nacos",
                    Password: "nacos",
                },
            },
        },
    },
})

err := loader.Start()
```

## Configuration Client

Use `GetConfigClient(group)` to access configuration APIs. Empty group names are normalized to `DEFAULT_GROUP`.

Supported operations:

- `GetConfigRawContent(dataId)` reads raw content.
- `GetConfig(dataId, type, value)` reads and deserializes JSON or YAML.
- `WatchConfig(dataId, callback)` subscribes to config changes.
- `UnwatchConfig(watchId)` cancels a config subscription.
- `LoadAndWatchConfig(settings)` loads multiple config files and optionally watches them in best-effort mode.

## Service Discovery Client

Use `GetNamingClient(group)` to access service discovery APIs. Empty group names are normalized to `DEFAULT_GROUP`.

Supported operations:

- `Register(instance)` registers one ephemeral instance.
- `RegisterBatch(serviceName, instances)` registers multiple ephemeral instances.
- `Unregister(instanceId)` removes a previously registered instance.
- `GetService`, `GetServicePage`, `GetAllInstances`, `GetHealthyInstances`, and `ChooseOneHealthyInstance` query service metadata and instances.
- `WatchNaming(serviceName, callback)` and `UnwatchNaming(watchId)` manage service instance subscriptions.

Instance identifiers are generated from `group`, `serviceName`, `ip`, and `port`, so the same address can be registered under different services without local ID conflicts.

## Initialization Order

`InitConfigSettings` runs after the config client is created and before `InitFunc`. Use it to load configuration values that later starters or `InitFunc` need immediately.

`InitFunc` runs after configured clients are initialized. Depending on `DisableConfig` and `DisableDiscovery`, one of the raw SDK clients passed into `InitFunc` may be `nil`.

## Lifecycle Notes

This starter is process-singleton by design. `Start` initializes package-level Nacos clients, and `Stop` unregisters naming instances, closes the naming client, closes the config client, and clears global state.

Old `ConfigClient` and `NamingClient` wrappers should not be reused after `Stop`; methods will return disabled-client errors once the raw clients are cleared.

The standard Nacos starter does not allow parent-managed restart after successful shutdown.

## Testing

The current tests are integration-oriented and expect a local Nacos server, usually `localhost:8848` with `nacos/nacos`. Some tests block for manual observation, so run them selectively.
