package shutdown

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrPerInstanceTimeout = errors.New("per instance timeout exceeded")
var ErrGlobalTimeout = errors.New("global timeout exceeded")

type Instance interface {
	Shutdown(context.Context) error
}

type Manager struct {
	instances          []Instance
	timeoutPerInstance time.Duration
	timeoutGlobal      time.Duration
}

func New(
	instances []Instance,
	timeoutPerInstance time.Duration,
	timeoutGlobal time.Duration,
) (*Manager, error) {
	if timeoutGlobal <= 0 {
		return nil, errors.New("timeout global is 0")
	}
	if timeoutPerInstance <= 0 {
		return nil, errors.New("timeout pre instance is 0")
	}
	if len(instances) == 0 {
		return nil, errors.New("given instance slice is empty")
	}
	return &Manager{
		instances:          instances,
		timeoutGlobal:      timeoutGlobal,
		timeoutPerInstance: timeoutPerInstance,
	}, nil
}

func (g *Manager) Shutdown(ctx context.Context) error {
	globalTimeoutCtx, cancel := context.WithTimeoutCause(
		ctx,
		g.timeoutGlobal,
		ErrGlobalTimeout,
	)
	defer cancel()
	var errs = make([]error, 0, len(g.instances))

	for _, instance := range g.instances {
		err := g.shutdownOnce(globalTimeoutCtx, instance)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (g *Manager) shutdownOnce(
	ctx context.Context,
	instance Instance,
) error {
	timeoutPerInstanceCtx, cancel := context.WithTimeoutCause(
		ctx,
		g.timeoutPerInstance,
		ErrPerInstanceTimeout,
	)
	defer cancel()

	err := instance.Shutdown(timeoutPerInstanceCtx)
	cause := context.Cause(timeoutPerInstanceCtx)

	switch {
	case err != nil && cause != nil:
		return fmt.Errorf("%T: %w", instance, errors.Join(err, cause))
	case err != nil:
		return fmt.Errorf("%T: %w", instance, err)
	case cause != nil:
		return fmt.Errorf("%T: %w", instance, cause)
	default:
		return nil
	}
}
