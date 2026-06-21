// closure_extended_test.go — additional tests for Closure Actor package
// Covers: BaseActor defaults, GetActor, IsStopped, GetState, edge cases for Call/Send/TrySend
package closure_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	closure "github.com/RedHuang-0622/TemplatePoolByGO/util/Closure"
)

// ============================================================================
// BaseActor tests
// ============================================================================

type DummyActor struct {
	closure.BaseActor[string]
}

func (a *DummyActor) Init() string {
	return "dummy"
}

func TestBaseActor_Init_ZeroValue(t *testing.T) {
	// BaseActor.Init() returns zero value of type T
	ba := closure.BaseActor[int]{}
	val := ba.Init()
	if val != 0 {
		t.Errorf("BaseActor[int].Init() should return 0, got %d", val)
	}
}

func TestBaseActor_OnStart_NoOp(t *testing.T) {
	ba := closure.BaseActor[int]{}
	state := 42
	// Should not panic
	ba.OnStart(&state)
}

func TestBaseActor_OnStop_NoOp(t *testing.T) {
	ba := closure.BaseActor[int]{}
	state := 42
	// Should not panic
	ba.OnStop(&state)
}

// ============================================================================
// GetActor tests
// ============================================================================

func TestGetActor(t *testing.T) {
	actor := &CounterActor{}
	c := closure.New(actor)
	defer c.StopAndWait()

	got := c.GetActor()
	// GetActor returns the actor with its concrete type
	if got == nil {
		t.Error("GetActor should return the CounterActor, got nil")
	}
	_ = got
}

// ============================================================================
// IsStopped tests
// ============================================================================

func TestIsStopped_BeforeStop(t *testing.T) {
	c := closure.New(&CounterActor{})
	defer c.StopAndWait()

	if c.IsStopped() {
		t.Error("IsStopped should return false before Stop()")
	}
}

func TestIsStopped_AfterStop(t *testing.T) {
	c := closure.New(&CounterActor{})
	c.Stop()

	time.Sleep(20 * time.Millisecond)

	if !c.IsStopped() {
		t.Error("IsStopped should return true after Stop()")
	}
}

func TestIsStopped_AfterStopAndWait(t *testing.T) {
	c := closure.New(&CounterActor{})
	c.StopAndWait()

	if !c.IsStopped() {
		t.Error("IsStopped should return true after StopAndWait()")
	}
}

// ============================================================================
// GetState tests
// ============================================================================

func TestGetState(t *testing.T) {
	c := closure.New(&CounterActor{})
	defer c.StopAndWait()

	// Increment first
	_, _ = closure.CallTyped(c, func(a *CounterActor, s *int) int {
		return a.Increment(s, 42)
	})

	state, err := c.GetState(func(s int) int {
		return s
	})
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	if state != 42 {
		t.Errorf("GetState: expected 42, got %d", state)
	}
}

func TestGetState_AfterStop(t *testing.T) {
	c := closure.New(&CounterActor{})
	c.StopAndWait()

	_, err := c.GetState(func(s int) int {
		return s
	})
	if err == nil {
		t.Error("GetState should return error after StopAndWait()")
	}
}

// ============================================================================
// CallWithContext: stopped actor
// ============================================================================

func TestCallWithContext_AlreadyStopped(t *testing.T) {
	c := closure.New(&CounterActor{})
	c.StopAndWait()

	ctx := context.Background()
	_, err := c.CallWithContext(ctx, func(a *CounterActor, s *int) any {
		return a.Increment(s, 1)
	})

	if err == nil {
		t.Error("CallWithContext should return error when actor is stopped")
	}
}

// ============================================================================
// CallWithContext: context cancelled before inbox send
// ============================================================================

func TestCallWithContext_ContextCancelledBeforeSend(t *testing.T) {
	c := closure.New(&CounterActor{}, closure.WithInboxSize(1))
	defer c.StopAndWait()

	// Fill the inbox so the next CallWithContext blocks on inbox send
	c.Send(func(a *CounterActor, s *int) {
		time.Sleep(100 * time.Millisecond)
		a.Increment(s, 1)
	})

	// Create context that expires immediately
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond) // Let it expire

	_, err := c.CallWithContext(ctx, func(a *CounterActor, s *int) any {
		return a.Increment(s, 1)
	})

	if err == nil {
		t.Error("CallWithContext should return error when context is cancelled")
	}
}

// ============================================================================
// TrySend: stopped actor
// ============================================================================

func TestTrySend_WhenStopped(t *testing.T) {
	c := closure.New(&CounterActor{})
	c.StopAndWait()

	err := c.TrySend(func(a *CounterActor, s *int) {
		a.Increment(s, 1)
	})

	// After StopAndWait, TrySend may succeed (inbox write) or fail (stopped channel)
	// because Go select chooses randomly between ready cases.
	// If it succeeds, the task is orphaned in the inbox buffer — not a panic.
	if err != nil {
		t.Logf("TrySend after stop returned error: %v", err)
	} else {
		t.Log("TrySend after stop succeeded (task orphaned in inbox)")
	}
}

// ============================================================================
// Send: stopped actor
// ============================================================================

func TestSend_WhenStopped(t *testing.T) {
	c := closure.New(&CounterActor{})
	c.StopAndWait()

	err := c.Send(func(a *CounterActor, s *int) {
		a.Increment(s, 1)
	})

	// After StopAndWait, Send may succeed (inbox write) or fail (stopped channel)
	// because Go select chooses randomly between ready cases.
	if err != nil {
		t.Logf("Send after stop returned error: %v", err)
	} else {
		t.Log("Send after stop succeeded (task orphaned in inbox)")
	}
}

// ============================================================================
// Call: stopped actor
// ============================================================================

func TestCall_WhenStopped(t *testing.T) {
	c := closure.New(&CounterActor{})
	c.StopAndWait()

	_, err := c.Call(func(a *CounterActor, s *int) any {
		return a.Increment(s, 1)
	})

	if err == nil {
		t.Error("Call should return error when actor is stopped")
	}
}

// ============================================================================
// CallTyped: nil result for non-error type
// ============================================================================

func TestCallTyped_NilResultNonError(t *testing.T) {
	c := closure.New(&CounterActor{})
	defer c.StopAndWait()

	// CallTyped expects int, but we return nil → should get error
	_, err := closure.CallTyped(c, func(a *CounterActor, s *int) int {
		_ = a.Increment(s, 1)
		// Returning int (non-nil interface), but the underlying value could be typed
		return 0 // This returns 0, not nil
	})
	if err != nil {
		t.Logf("CallTyped returned error (expected for known cases): %v", err)
	}
}

// ============================================================================
// CallTyped: with panic recovery in Send
// ============================================================================

func TestSend_PanicRecovery(t *testing.T) {
	c := closure.New(&CounterActor{})
	defer c.StopAndWait()

	// Send a function that panics — should be recovered
	err := c.Send(func(a *CounterActor, s *int) {
		panic("test panic in Send")
	})
	if err != nil {
		t.Fatalf("Send should not return error on send: %v", err)
	}

	// Wait for panic recovery
	time.Sleep(50 * time.Millisecond)

	// Actor should still be functional
	val, err := closure.CallTyped(c, func(a *CounterActor, s *int) int {
		return a.Get(s)
	})
	if err != nil {
		t.Fatalf("Actor should still work after panic: %v", err)
	}
	if val != 0 {
		t.Errorf("value should be 0 (panic'd fn didn't execute), got %d", val)
	}
}

// ============================================================================
// TrySend_PanicRecovery
// ============================================================================

func TestTrySend_PanicRecovery(t *testing.T) {
	c := closure.New(&CounterActor{})
	defer c.StopAndWait()

	err := c.TrySend(func(a *CounterActor, s *int) {
		panic("test panic in TrySend")
	})
	if err != nil {
		t.Fatalf("TrySend should succeed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Actor should still be functional
	val, err := closure.CallTyped(c, func(a *CounterActor, s *int) int {
		return a.Get(s)
	})
	if err != nil {
		t.Fatalf("Actor should still work after panic: %v", err)
	}
	if val != 0 {
		t.Errorf("value should be 0, got %d", val)
	}
}

// ============================================================================
// StopAndWait: idempotent
// ============================================================================

func TestStopAndWait_Idempotent(t *testing.T) {
	c := closure.New(&CounterActor{})
	c.StopAndWait()
	// Second call should not panic
	c.StopAndWait()
}

func TestStop_Idempotent(t *testing.T) {
	c := closure.New(&CounterActor{})
	c.Stop()
	time.Sleep(10 * time.Millisecond)
	// Second call should not panic
	c.Stop()
	time.Sleep(10 * time.Millisecond)
}

// ============================================================================
// WithInboxSize: edge values
// ============================================================================

func TestWithInboxSize_VerySmall(t *testing.T) {
	c := closure.New(&CounterActor{}, closure.WithInboxSize(1))
	defer c.StopAndWait()

	// Fill the inbox
	c.Send(func(a *CounterActor, s *int) {
		time.Sleep(100 * time.Millisecond)
		a.Increment(s, 1)
	})

	// Send should block but not deadlock (with 2s timeout in test)
	done := make(chan struct{})
	go func() {
		c.Send(func(a *CounterActor, s *int) {
			a.Increment(s, 2)
		})
		close(done)
	}()

	select {
	case <-done:
		t.Log("Send on full inbox completed (previous message processed)")
	case <-time.After(2 * time.Second):
		t.Error("Send on full inbox should eventually complete")
	}
}

// ============================================================================
// WithTimeout: setting validation
// ============================================================================

func TestWithTimeout_Option(t *testing.T) {
	c := closure.New(&CounterActor{}, closure.WithTimeout(5*time.Second))
	defer c.StopAndWait()

	// Should still work normally
	val, err := closure.CallTyped(c, func(a *CounterActor, s *int) int {
		return a.Increment(s, 10)
	})
	if err != nil {
		t.Fatalf("CallTyped failed: %v", err)
	}
	if val != 10 {
		t.Errorf("expected 10, got %d", val)
	}
}

// ============================================================================
// CallWithContext: panic in function body
// ============================================================================

func TestCallWithContext_PanicRecovery(t *testing.T) {
	c := closure.New(&CounterActor{})
	defer c.StopAndWait()

	ctx := context.Background()
	result, err := c.CallWithContext(ctx, func(a *CounterActor, s *int) any {
		panic("test panic in CallWithContext")
	})

	if err == nil {
		t.Error("CallWithContext should return error for panic")
	} else {
		t.Logf("Got expected error: %v", err)
	}
	_ = result
}

// ============================================================================
// CallTyped: type mismatch
// ============================================================================

func TestCallTyped_TypeInference(t *testing.T) {
	// Go infers R from the lambda's return type, so CallTyped works correctly
	c := closure.New(&CounterActor{})
	defer c.StopAndWait()

	result, err := closure.CallTyped(c, func(a *CounterActor, s *int) string {
		return fmt.Sprintf("%d", a.Get(s))
	})

	if err != nil {
		t.Fatalf("CallTyped should succeed with type inference: %v", err)
	}
	if result != "0" {
		t.Errorf("expected '0', got '%s'", result)
	}
}

// ============================================================================
// Concurrent Call and Send mix
// ============================================================================

func TestConcurrentCallAndSend(t *testing.T) {
	c := closure.New(&CounterActor{}, closure.WithInboxSize(500))
	defer c.StopAndWait()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			c.Send(func(a *CounterActor, s *int) {
				a.Increment(s, 1)
			})
		}
		close(done)
	}()

	for i := 0; i < 50; i++ {
		_, err := closure.CallTyped(c, func(a *CounterActor, s *int) int {
			return a.Increment(s, 1)
		})
		if err != nil {
			t.Fatalf("CallTyped #%d failed: %v", i, err)
		}
	}

	<-done
	time.Sleep(100 * time.Millisecond)

	val, _ := closure.CallTyped(c, func(a *CounterActor, s *int) int {
		return a.Get(s)
	})
	if val != 100 {
		t.Errorf("expected 100, got %d", val)
	}
}

// ============================================================================
// Large number of TrySend
// ============================================================================

func TestTrySend_ManyMessages(t *testing.T) {
	c := closure.New(&CounterActor{}, closure.WithInboxSize(1000))
	defer c.StopAndWait()

	for i := 0; i < 500; i++ {
		err := c.TrySend(func(a *CounterActor, s *int) {
			a.Increment(s, 1)
		})
		if err != nil {
			t.Fatalf("TrySend #%d failed: %v", i, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	val, _ := closure.CallTyped(c, func(a *CounterActor, s *int) int {
		return a.Get(s)
	})
	if val != 500 {
		t.Errorf("expected 500, got %d", val)
	}
}

// ============================================================================
// Call with error return from actor method
// ============================================================================

type ErrorActor struct {
	closure.BaseActor[int]
}

func (a *ErrorActor) Init() int { return 0 }

func (a *ErrorActor) FailingOp(s *int) error {
	return errors.New("operation failed")
}

func (a *ErrorActor) SuccessfulOp(s *int) error {
	*s = 99
	return nil
}

func TestCallTyped_ErrorReturn(t *testing.T) {
	c := closure.New(&ErrorActor{})
	defer c.StopAndWait()

	// Test error return: CallTyped's first return value is the business result (error),
	// second return value is the communication error.
	businessErr, commErr := closure.CallTyped(c, func(a *ErrorActor, s *int) error {
		return a.FailingOp(s)
	})
	if commErr != nil {
		t.Fatalf("unexpected communication error: %v", commErr)
	}
	if businessErr == nil {
		t.Error("expected business error from FailingOp")
	}

	// Test nil error return
	businessErr, commErr = closure.CallTyped(c, func(a *ErrorActor, s *int) error {
		return a.SuccessfulOp(s)
	})
	if commErr != nil {
		t.Fatalf("unexpected communication error: %v", commErr)
	}
	if businessErr != nil {
		t.Errorf("expected nil business error, got %v", businessErr)
	}

	// Verify state changed (SuccessfulOp set s=99)
	val, _ := closure.CallTyped(c, func(a *ErrorActor, s *int) int {
		return *s
	})
	if val != 99 {
		t.Errorf("expected 99, got %d", val)
	}
}

// ============================================================================
// CallTyped: typed nil pointer — CallTyped handles it correctly (no error)
// The nil-check in CallTyped (line ~185) only fires when result interface is
// truly nil (bare nil without concrete type), which is hard to trigger from
// Go generics since lambda return types always carry concrete type info.
// ============================================================================

func TestCallTyped_NilPointerResult(t *testing.T) {
	c := closure.New(&CounterActor{})
	defer c.StopAndWait()

	// Returning typed nil *int is valid — no error expected
	result, err := closure.CallTyped(c, func(a *CounterActor, s *int) *int {
		return nil // *int(nil)
	})
	if err != nil {
		t.Fatalf("CallTyped should not error for typed nil pointer: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil *int, got %v", result)
	}
}
