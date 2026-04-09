//go:build windows

package service

import (
	"fmt"
	"log/slog"

	"golang.org/x/sys/windows/svc"
)

type beatExporterService struct {
	stopCh chan<- bool
}

func (s *beatExporterService) Execute(args []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (ssec bool, errno uint32) {
	const cmdsAccepted = svc.AcceptStop | svc.AcceptShutdown
	changes <- svc.Status{State: svc.StartPending}
	changes <- svc.Status{State: svc.Running, Accepts: cmdsAccepted}
loop:
	for {
		select {
		case c := <-r:
			switch c.Cmd {
			case svc.Interrogate:
				changes <- c.CurrentStatus
			case svc.Stop, svc.Shutdown:
				s.stopCh <- true
				break loop
			default:
				slog.Error("Unexpected service control request", "cmd", fmt.Sprintf("#%d", c))
			}
		}
	}
	changes <- svc.Status{State: svc.StopPending}
	return
}

// SetupServiceListener sets up a Windows service handler or signal handler for interactive sessions.
func SetupServiceListener(stopCh chan<- bool, serviceName string) error {
	isInteractive, err := svc.IsAnInteractiveSession()
	if err != nil {
		return err
	}

	if !isInteractive {
		go func() {
			if err := svc.Run(serviceName, &beatExporterService{stopCh: stopCh}); err != nil {
				slog.Error("Failed to start Windows service", "err", err)
			}
		}()
	}

	return nil
}
