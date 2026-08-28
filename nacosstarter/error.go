package nacosstarter

import "errors"

var (
	ErrValueMustBeNonNilPointer    = errors.New("value must be a non-nil pointer")
	ErrUnknownConfigType           = errors.New("unknown config type")
	ErrDisabledConfigClient        = errors.New("disabled config client")
	ErrDisabledDiscoveryClient     = errors.New("disabled discover client")
	ErrConfigAndDiscoveryDisabled  = errors.New("config and discover modules are disabled")
	ErrBadNacosConfig              = errors.New("bad nacos config")
	ErrNacosStarterAlreadyStarted  = errors.New("nacos starter already started")
	ErrNacosStarterNotStarted      = errors.New("nacos starter not started")
	ErrNacosStopTimeout            = errors.New("nacos starter stop timeout")
	ErrDuplicatedConfigWatch       = errors.New("duplicated config watch")
	ErrBadWatchId                  = errors.New("bad watchId")
	ErrEmptyInstance               = errors.New("empty instance")
	ErrBadInstanceId               = errors.New("bad instanceId")
	ErrDuplicatedNamingWatch       = errors.New("duplicated naming watch")
	ErrRegisterInstanceFailed      = errors.New("register instance failed")
)
