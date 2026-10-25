package shutdown_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ethernetuser/shutdown"
)

type mockInstance struct {
	name         string
	shutdownFunc func(context.Context) error
}

func (m *mockInstance) Shutdown(ctx context.Context) error {
	if m.shutdownFunc != nil {
		return m.shutdownFunc(ctx)
	}
	return nil
}

func blockingShutdown(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func Test_New(t *testing.T) {
	inst := &mockInstance{name: "a"}
	inst2 := &mockInstance{name: "b"}

	tests := []struct {
		name               string
		instances          []shutdown.Instance
		timeoutPerInstance time.Duration
		timeoutGlobal      time.Duration
		wantErr            bool
	}{
		{
			name:               "nil instances",
			instances:          nil,
			timeoutPerInstance: time.Second,
			timeoutGlobal:      time.Second,
			wantErr:            true,
		},
		{
			name:               "empty instances slice",
			instances:          []shutdown.Instance{},
			timeoutPerInstance: time.Second,
			timeoutGlobal:      time.Second,
			wantErr:            true,
		},
		{
			name:               "zero global timeout",
			instances:          []shutdown.Instance{inst},
			timeoutPerInstance: time.Second,
			timeoutGlobal:      0,
			wantErr:            true,
		},
		{
			name:               "negative global timeout",
			instances:          []shutdown.Instance{inst},
			timeoutPerInstance: time.Second,
			timeoutGlobal:      -time.Millisecond,
			wantErr:            true,
		},
		{
			name:               "zero per-instance timeout",
			instances:          []shutdown.Instance{inst},
			timeoutPerInstance: 0,
			timeoutGlobal:      time.Second,
			wantErr:            true,
		},
		{
			name:               "negative per-instance timeout",
			instances:          []shutdown.Instance{inst},
			timeoutPerInstance: -time.Millisecond,
			timeoutGlobal:      time.Second,
			wantErr:            true,
		},
		{
			name:               "success with one instance",
			instances:          []shutdown.Instance{inst},
			timeoutPerInstance: time.Second,
			timeoutGlobal:      5 * time.Second,
			wantErr:            false,
		},
		{
			name:               "success with multiple instances",
			instances:          []shutdown.Instance{inst, inst2},
			timeoutPerInstance: time.Second,
			timeoutGlobal:      5 * time.Second,
			wantErr:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := shutdown.New(
				tt.instances,
				tt.timeoutPerInstance,
				tt.timeoutGlobal,
			)

			if tt.wantErr {
				if gotErr == nil {
					t.Fatalf("New() succeeded unexpectedly, got %v", got)
				}
				if got != nil {
					t.Errorf("New() = %v, want nil on error", got)
				}
				return
			}

			if gotErr != nil {
				t.Fatalf("New() failed unexpectedly: %v", gotErr)
			}
			if got == nil {
				t.Fatal("New() = nil, want non-nil Manager")
			}
		})
	}
}

func Test_Shutdown(t *testing.T) {
	errShutdown := errors.New("shutdown failed")

	tests := []struct {
		name               string
		instances          []shutdown.Instance
		timeoutPerInstance time.Duration
		timeoutGlobal      time.Duration
		wantErr            bool
		wantErrIs          []error
	}{
		{
			name: "all instances succeed",
			instances: []shutdown.Instance{
				&mockInstance{name: "a"},
				&mockInstance{name: "b"},
			},
			timeoutPerInstance: time.Second,
			timeoutGlobal:      5 * time.Second,
			wantErr:            false,
		},
		{
			name: "instance returns error",
			instances: []shutdown.Instance{
				&mockInstance{
					name: "failing",
					shutdownFunc: func(context.Context) error {
						return errShutdown
					},
				},
			},
			timeoutPerInstance: time.Second,
			timeoutGlobal:      5 * time.Second,
			wantErr:            true,
			wantErrIs:          []error{errShutdown},
		},
		{
			name: "multiple instances return errors",
			instances: []shutdown.Instance{
				&mockInstance{
					name: "failing",
					shutdownFunc: func(context.Context) error {
						return errShutdown
					},
				},
				&mockInstance{name: "ok"},
			},
			timeoutPerInstance: time.Second,
			timeoutGlobal:      5 * time.Second,
			wantErr:            true,
			wantErrIs:          []error{errShutdown},
		},
		{
			name: "per-instance timeout exceeded",
			instances: []shutdown.Instance{
				&mockInstance{name: "blocker", shutdownFunc: blockingShutdown},
			},
			timeoutPerInstance: 10 * time.Millisecond,
			timeoutGlobal:      time.Second,
			wantErr:            true,
			wantErrIs:          []error{shutdown.ErrPerInstanceTimeout},
		},
		{
			name: "global timeout exceeded",
			instances: []shutdown.Instance{
				&mockInstance{name: "blocker", shutdownFunc: blockingShutdown},
			},
			timeoutPerInstance: time.Second,
			timeoutGlobal:      10 * time.Millisecond,
			wantErr:            true,
			wantErrIs:          []error{shutdown.ErrGlobalTimeout},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := shutdown.New(
				tt.instances,
				tt.timeoutPerInstance,
				tt.timeoutGlobal,
			)
			if err != nil {
				t.Fatalf("New() failed: %v", err)
			}

			gotErr := m.Shutdown(context.Background())

			if !tt.wantErr {
				if gotErr != nil {
					t.Fatalf("Shutdown() failed unexpectedly: %v", gotErr)
				}
				return
			}

			if gotErr == nil {
				t.Fatal("Shutdown() succeeded unexpectedly")
			}
			for _, target := range tt.wantErrIs {
				if !errors.Is(gotErr, target) {
					t.Errorf(
						"Shutdown() error = %v, want errors.Is(..., %v)",
						gotErr, target,
					)
				}
			}
		})
	}
}

func Test_Shutdown_Sequential(t *testing.T) {
	tests := []struct {
		name      string
		instances []struct{ fail bool }
	}{
		{
			name:      "all succeed",
			instances: []struct{ fail bool }{{}, {}, {}},
		},
		{
			name:      "middle fails",
			instances: []struct{ fail bool }{{}, {fail: true}, {}},
		},
		{
			name:      "first fails",
			instances: []struct{ fail bool }{{fail: true}, {}, {}},
		},
		{
			name:      "last fails",
			instances: []struct{ fail bool }{{}, {}, {fail: true}},
		},
		{
			name:      "single instance",
			instances: []struct{ fail bool }{{}},
		},
		{
			name:      "single fails",
			instances: []struct{ fail bool }{{fail: true}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			var events []string

			instances := make([]shutdown.Instance, len(tt.instances))
			for i, spec := range tt.instances {
				idx, fail := i, spec.fail
				instances[idx] = &mockInstance{
					name: fmt.Sprintf("i%d", idx),
					shutdownFunc: func(context.Context) error {
						mu.Lock()
						events = append(events, fmt.Sprintf("start:%d", idx))
						mu.Unlock()

						time.Sleep(5 * time.Millisecond)

						mu.Lock()
						events = append(events, fmt.Sprintf("end:%d", idx))
						mu.Unlock()

						if fail {
							return errors.New("fail")
						}
						return nil
					},
				}
			}

			m, err := shutdown.New(instances, time.Second, 5*time.Second)
			if err != nil {
				t.Fatalf("New() failed: %v", err)
			}
			_ = m.Shutdown(context.Background())

			want := make([]string, 0, len(instances)*2)
			for i := range instances {
				want = append(want,
					fmt.Sprintf("start:%d", i),
					fmt.Sprintf("end:%d", i),
				)
			}

			if !reflect.DeepEqual(events, want) {
				t.Errorf(
					"execution order mismatch:\n got = %v\nwant = %v",
					events,
					want,
				)
			}
		})
	}
}
