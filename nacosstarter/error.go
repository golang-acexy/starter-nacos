package nacosstarter

import "errors"

var (
	// ErrValueMustBeNonNilPointer reports an invalid YAML destination.
	ErrValueMustBeNonNilPointer = errors.New("value must be a non-nil pointer")
	// ErrUnknownConfigType reports an unsupported configuration decoder.
	ErrUnknownConfigType = errors.New("unknown config type")
	// ErrDisabledConfigClient reports an unavailable configuration capability.
	ErrDisabledConfigClient = errors.New("disabled config client")
	// ErrDisabledDiscoveryClient reports an unavailable naming capability.
	ErrDisabledDiscoveryClient = errors.New("disabled discover client")
	// ErrConfigAndDiscoveryDisabled reports that no capability was enabled.
	ErrConfigAndDiscoveryDisabled = errors.New("config and discover modules are disabled")
	// ErrBadNacosConfig reports missing client settings or server endpoints.
	ErrBadNacosConfig = errors.New("bad nacos config")
	// ErrNacosStarterAlreadyStarted reports a lifecycle state that cannot start.
	ErrNacosStarterAlreadyStarted = errors.New("nacos starter already started")
	// ErrNacosStarterNotStarted reports that no running starter can be stopped.
	ErrNacosStarterNotStarted = errors.New("nacos starter not started")
	// ErrNacosStopTimeout reports cleanup exceeding the requested wait; cleanup continues.
	ErrNacosStopTimeout = errors.New("nacos starter stop timeout")
	// ErrDuplicatedConfigWatch reports an already tracked configuration subscription.
	ErrDuplicatedConfigWatch = errors.New("duplicated config watch")
	// ErrBadWatchId reports an ID not tracked by this group wrapper.
	ErrBadWatchId = errors.New("bad watchId")
	// ErrEmptyInstance reports an empty batch registration.
	ErrEmptyInstance = errors.New("empty instance")
	// ErrBadInstanceId reports an instance not locally registered by this wrapper.
	ErrBadInstanceId = errors.New("bad instanceId")
	// ErrDuplicatedNamingWatch reports an already tracked service subscription.
	ErrDuplicatedNamingWatch = errors.New("duplicated naming watch")
	// ErrRegisterInstanceFailed reports an unsuccessful SDK registration without an SDK error.
	ErrRegisterInstanceFailed = errors.New("register instance failed")
)
