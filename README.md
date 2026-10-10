# starter-nacos

`starter-nacos` provides Nacos configuration and service discovery integration for the golang-acexy starter ecosystem. It is intended to be started and stopped by `starter-parent`.

## Ecosystem Role

This starter supplies shared configuration and naming clients. It can initialize application configuration before dependent components start and provides discovery data to integrations such as `starter-grpc` resolvers.

## Requirements

- Go `1.26.7`
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

if err := loader.Start(); err != nil {
    return err
}
```

## Configuration Client

Use `GetConfigClient(group)` to access configuration APIs. Empty group names are normalized to `DEFAULT_GROUP`.

Supported operations:

- `GetConfigRawContent(dataId)` reads raw content.
- `GetConfig(dataId, type, value)` reads and deserializes JSON or YAML.
- `WatchConfig(dataId, callback)` subscribes to config changes.
- `UnwatchConfig(watchId)` cancels a config subscription.
- `LoadAndWatchConfig(settings)` loads multiple config files and optionally watches them in best-effort mode. Failures do not abort startup; use error-returning APIs when configuration is required for startup.

Pass nonnil destination pointers and callbacks. Synchronize reads of dynamically updated values; automatic writes are serialized within one `LoadAndWatchConfig` call, but separate calls and external readers require your own coordination. JSON decoding can partially change the target on error; failed YAML decoding leaves it unchanged.

## Service Discovery Client

Use `GetNamingClient(group)` to access service discovery APIs. Empty group names are normalized to `DEFAULT_GROUP`.

Supported operations:

- `Register(instance)` registers one ephemeral instance.
- `RegisterBatch(serviceName, instances)` registers multiple ephemeral instances.
- `Unregister(instanceId)` removes a previously registered instance.
- `GetService`, `GetServicePage`, `GetAllInstances`, `GetHealthyInstances`, and `ChooseOneHealthyInstance` query service metadata and instances.
- `WatchNaming(serviceName, callback)` and `UnwatchNaming(watchId)` manage service instance subscriptions. Provide a nonnil callback and synchronize any shared state it accesses.

When `UpdateCacheWhenEmpty` is false, empty-instance updates are suppressed. If subscription and cleanup both fail, `WatchNaming` returns an error and a retained watch ID; retry cleanup with `UnwatchNaming(id)`. Cancellation does not wait for already dispatched callbacks.

Instance identifiers are generated from `group`, `serviceName`, `ip`, and `port`, so the same address can be registered under different services without local ID conflicts.

## Initialization Order

`InitConfigSettings` runs after the config client is created and before `InitFunc`. Use it to load configuration values that later starters or `InitFunc` need immediately.

`InitFunc` runs after configured clients are initialized. Depending on `DisableConfig` and `DisableDiscovery`, one of the raw SDK clients passed into `InitFunc` may be `nil`.

## Lifecycle Notes

This starter is process-singleton by design. Start it before requesting group clients. `Start` returns `nil, nil` on success; obtain clients through the accessor APIs. `LazyConfig` takes precedence over `Config`, and the resolved settings are cached. Startup normalizes the shared SDK namespace value `public` to an empty string.

Finish client operations before `Stop` and do not reuse old group wrappers after shutdown. A shutdown timeout reports `stopped=true`, but resource cleanup continues in the background. Individual deregistration failures are logged. Raw SDK clients are owned and closed by the starter.

The standard Nacos starter does not allow parent-managed restart after successful shutdown.

## Testing

Package-level unit and regression tests do not require Nacos:

```bash
go test -race ./nacosstarter
```

Tests under `test/` expect an explicitly configured Nacos server and currently have no integration build tag. Some block for manual observation, so avoid `go test ./...` for routine validation and run integration tests selectively:

```bash
go test ./test -run '^TestName$' -timeout=30s
```
