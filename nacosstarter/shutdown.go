package nacosstarter

import "github.com/acexy/golang-toolkit/logger"

// cancelManagedWatches detaches local records before calling the SDK without wrapper locks.
// Shutdown owns the withdrawn runtime; callers must already have finished client operations.
func cancelManagedWatches(runtime *nacosRuntime) {
	if runtime.manager == nil {
		return
	}
	if runtime.naming != nil {
		for _, client := range snapshotNamingClients(runtime.manager) {
			client.mu.Lock()
			watches := client.watched
			client.watched = nil
			client.mu.Unlock()
			for _, param := range watches {
				// The SDK removes callbacks by the identity of the original parameter field.
				if err := runtime.naming.Unsubscribe(param); err != nil {
					logger.Logrus().WithError(err).Error("cancel naming subscription during shutdown failed")
				}
			}
		}
	}
	if runtime.config != nil {
		runtime.manager.configLocker.Lock()
		clients := make([]*ConfigClient, 0, len(runtime.manager.configClient))
		for _, client := range runtime.manager.configClient {
			clients = append(clients, client)
		}
		runtime.manager.configLocker.Unlock()
		for _, client := range clients {
			client.mu.Lock()
			watches := client.watched
			client.watched = nil
			client.mu.Unlock()
			for _, param := range watches {
				if err := runtime.config.CancelListenConfig(*param); err != nil {
					logger.Logrus().WithError(err).Error("cancel configuration subscription during shutdown failed")
				}
			}
		}
	}
}
