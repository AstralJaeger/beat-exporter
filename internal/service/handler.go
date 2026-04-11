//go:build linux || darwin

package service

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// SetupServiceListener sets up a signal handler that sends to stopCh on SIGINT/SIGTERM/SIGHUP.
func SetupServiceListener(stopCh chan<- bool, serviceName string) error {
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
		sig := <-sigs
		slog.Info("Signal received, shutting down", "signal", sig, "service", serviceName)
		stopCh <- true
	}()

	return nil
}
