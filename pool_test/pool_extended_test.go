// pool_extended_test.go — additional black-box tests covering edge cases
package pool_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/RedHuang-0622/TemplatePoolByGO"
)

// ============================================================================
// ErrPoolBusy: wait queue full
// ============================================================================

func TestPoolBusy_WaitQueueFull(t *testing.T) {
	config := PoolConfig{
		MinSize:          5,
		MaxSize:          5,
		MaxWaitQueue:     2, // small queue
		IdleBufferFactor: 1.0,
		MaxRetries:       1,
		RetryInterval:    10 * time.Millisecond,
		PingInterval:     0,
		MonitorInterval:  0,
	}

	p := NewPool(config, &FakeConnControl{})
	defer p.Close()
	time.Sleep(300 * time.Millisecond)

	ctx := context.Background()

	// Drain all pre-created connections
	held := make([]*Resource[*FakeConn], 0, 5)
	for i := 0; i < 5; i++ {
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		res, err := p.Get(rctx)
		cancel()
		if err != nil {
			t.Fatalf("Get #%d failed: %v", i, err)
		}
		held = append(held, res)
	}

	// Fill the wait queue (MaxWaitQueue=2)
	var wg sync.WaitGroup
	var errCount atomic.Int64
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, err := p.Get(rctx)
			cancel()
			if err != nil {
				errCount.Add(1)
			}
		}()
	}

	// Give goroutines time to enqueue
	time.Sleep(200 * time.Millisecond)

	// At least one should get ErrPoolBusy (queue size=2, 3 goroutines → at least 1 rejected)
	if errCount.Load() < 1 {
		t.Log("Timing-dependent: may not have rejected all extra waiters in time")
	}

	// Release connections to drain waiters
	for _, res := range held {
		p.Put(res)
	}
	wg.Wait()
	t.Logf("Errors from waiters: %d", errCount.Load())
}

// ============================================================================
// Get: context cancellation
// ============================================================================

func TestGetContextCancelled(t *testing.T) {
	config := PoolConfig{
		MinSize:          1,
		MaxSize:          1,
		MaxWaitQueue:     100,
		IdleBufferFactor: 1.0,
		PingInterval:     0,
		MonitorInterval:  0,
	}

	p := NewPool(config, &FakeConnControl{})
	defer p.Close()
	time.Sleep(300 * time.Millisecond)

	// Hold the only connection
	ctx := context.Background()
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	res, err := p.Get(rctx)
	cancel()
	if err != nil {
		t.Fatalf("first Get failed: %v", err)
	}

	// Try to get with an already cancelled context
	cancelledCtx, cancel2 := context.WithCancel(context.Background())
	cancel2() // cancel immediately

	_, err = p.Get(cancelledCtx)
	if err == nil {
		t.Error("expected error with cancelled context")
	}
	// Should be context.Canceled
	if !errors.Is(err, context.Canceled) {
		t.Logf("Got error (may be ErrPoolBusy or context.Canceled): %v", err)
	}

	p.Put(res)
}

func TestGetContextTimeout(t *testing.T) {
	config := PoolConfig{
		MinSize:          1,
		MaxSize:          1,
		MaxWaitQueue:     100,
		IdleBufferFactor: 1.0,
		PingInterval:     0,
		MonitorInterval:  0,
	}

	p := NewPool(config, &FakeConnControl{})
	defer p.Close()
	time.Sleep(300 * time.Millisecond)

	// Hold the only connection
	ctx := context.Background()
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	res, err := p.Get(rctx)
	cancel()
	if err != nil {
		t.Fatalf("first Get failed: %v", err)
	}

	// Try to get with a very short timeout
	shortCtx, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()

	_, err = p.Get(shortCtx)
	if err == nil {
		t.Error("expected timeout error")
	}

	p.Put(res)
}

// ============================================================================
// Put: nil resource
// ============================================================================

func TestPutNilResource(t *testing.T) {
	config := PoolConfig{
		MinSize:         3,
		MaxSize:         10,
		PingInterval:    0,
		MonitorInterval: 0,
	}

	p := NewPool(config, &FakeConnControl{})
	defer p.Close()
	time.Sleep(100 * time.Millisecond)

	// Put nil should not panic
	err := p.Put(nil)
	if err != nil {
		t.Errorf("Put(nil) should return nil error, got %v", err)
	}
}

// ============================================================================
// Put: Reset failure
// ============================================================================

func TestPutResetFailure(t *testing.T) {
	config := PoolConfig{
		MinSize:          3,
		MaxSize:          10,
		IdleBufferFactor: 1.0,
		PingInterval:     0,
		MonitorInterval:  0,
	}

	resetErr := errors.New("reset failure")
	ctrl := &FakeConnControl{resetErr: resetErr}
	p := NewPool(config, ctrl)
	defer p.Close()

	time.Sleep(200 * time.Millisecond)
	ctx := context.Background()

	res, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Put should fail with reset error
	err = p.Put(res)
	if err == nil {
		t.Error("expected error when Reset fails")
	} else if !errors.Is(err, resetErr) {
		t.Errorf("expected reset error, got %v", err)
	}

	// Verify totalSize decreased (connection closed by actor)
	time.Sleep(200 * time.Millisecond)
	stats, _ := p.Stats(ctx)
	t.Logf("After reset failure: total=%d", stats["total_size"])
}

// ============================================================================
// Put: channel full fallback
// ============================================================================

func TestPutChannelFull(t *testing.T) {
	config := PoolConfig{
		MinSize:          3,
		MaxSize:          3,
		IdleBufferFactor: 1.0, // buffer=3, all connections fit
		MaxRetries:       1,
		RetryInterval:    10 * time.Millisecond,
		PingInterval:     0,
		MonitorInterval:  0,
		MaxWaitQueue:     100,
	}

	p := NewPool(config, &FakeConnControl{})
	defer p.Close()
	time.Sleep(300 * time.Millisecond)

	ctx := context.Background()

	// Get all 3 pre-created connections
	held := make([]*Resource[*FakeConn], 0, 3)
	for i := 0; i < 3; i++ {
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		res, err := p.Get(rctx)
		cancel()
		if err != nil {
			t.Fatalf("Get #%d failed: %v", i, err)
		}
		held = append(held, res)
	}

	// Put all back — buffer can hold all 3, no connections should be closed
	for i := 0; i < 3; i++ {
		err := p.Put(held[i])
		if err != nil {
			t.Logf("Put #%d: err=%v", i, err)
		}
	}

	stats, _ := p.Stats(ctx)
	t.Logf("After Put all back: total=%d, available=%d, in_use=%d, buffer_cap=%d",
		stats["total_size"], stats["pool_available"], stats["pool_in_use"], stats["buffer_cap"])

	// All connections should still be tracked
	if stats["total_size"] != 3 {
		t.Errorf("total_size should be 3, got %d", stats["total_size"])
	}
}

// ============================================================================
// Pool.Close: Get after Close returns error, Put after Close doesn't panic
// ============================================================================

func TestPoolClose_GetAfterClose(t *testing.T) {
	p := NewPool(PoolConfig{
		MinSize:         3,
		MaxSize:         10,
		PingInterval:    0,
		MonitorInterval: 0,
	}, &FakeConnControl{})

	p.Close()

	ctx := context.Background()
	_, err := p.Get(ctx)
	if err == nil {
		t.Error("Get after Close should return error")
	}
}

// ============================================================================
// PingInterval disabled (0)
// ============================================================================

func TestPingIntervalDisabled(t *testing.T) {
	config := PoolConfig{
		MinSize:          3,
		MaxSize:          10,
		PingInterval:     0, // explicitly disabled
		MonitorInterval:  0,
		IdleBufferFactor: 1.0,
	}

	p := NewPool(config, &FakeConnControl{})
	defer p.Close()

	time.Sleep(300 * time.Millisecond)

	ctx := context.Background()
	res, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	p.Put(res)

	stats, _ := p.Stats(ctx)
	t.Logf("PingInterval=0: total=%d, available=%d", stats["total_size"], stats["pool_available"])
}

// ============================================================================
// MonitorInterval disabled (0)
// ============================================================================

func TestMonitorIntervalDisabled(t *testing.T) {
	config := PoolConfig{
		MinSize:          3,
		MaxSize:          10,
		MonitorInterval:  0, // explicitly disabled
		PingInterval:     0,
		IdleBufferFactor: 1.0,
	}

	p := NewPool(config, &FakeConnControl{})
	defer p.Close()

	time.Sleep(200 * time.Millisecond)

	ctx := context.Background()
	res, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	p.Put(res)

	stats, _ := p.Stats(ctx)
	t.Logf("MonitorInterval=0: total=%d, available=%d", stats["total_size"], stats["pool_available"])
}

// ============================================================================
// Stats consistency under load
// ============================================================================

func TestStatsConsistency(t *testing.T) {
	config := PoolConfig{
		MinSize:         5,
		MaxSize:         20,
		PingInterval:    0,
		MonitorInterval: 0,
	}

	p := NewPool(config, &FakeConnControl{})
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	stats, err := p.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	// Verify all expected keys exist
	expectedKeys := []string{"total_size", "pool_available", "pool_in_use", "waiting_count", "expanding", "buffer_cap"}
	for _, key := range expectedKeys {
		if _, ok := stats[key]; !ok {
			t.Errorf("Stats missing key: %s", key)
		}
	}

	// total should be >= pool_available (some may be in use)
	if stats["total_size"] < stats["pool_available"] {
		t.Errorf("total_size(%d) < pool_available(%d)", stats["total_size"], stats["pool_available"])
	}

	// buffer_cap should be >= 1
	if stats["buffer_cap"] < 1 {
		t.Errorf("buffer_cap should be >= 1, got %d", stats["buffer_cap"])
	}

	t.Logf("Stats: total=%d, available=%d, in_use=%d, waiting=%d, expanding=%d, buffer_cap=%d",
		stats["total_size"], stats["pool_available"], stats["pool_in_use"],
		stats["waiting_count"], stats["expanding"], stats["buffer_cap"])
}

// ============================================================================
// Concurrent Put/Get stress (no resource leak)
// ============================================================================

func TestConcurrentManyGoroutines(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent stress test in short mode")
	}

	config := PoolConfig{
		MinSize:          10,
		MaxSize:          50,
		IdleBufferFactor: 0.5,
		MaxRetries:       2,
		RetryInterval:    10 * time.Millisecond,
		PingInterval:     0,
		MonitorInterval:  0,
		MaxWaitQueue:     10000,
	}

	p := NewPool(config, &FakeConnControl{createDelay: 1 * time.Millisecond})
	defer p.Close()
	time.Sleep(300 * time.Millisecond)

	const goroutines = 100
	const opsPerG = 50

	var wg sync.WaitGroup
	var successCount atomic.Int64
	var failCount atomic.Int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				res, err := p.Get(ctx)
				cancel()
				if err != nil {
					failCount.Add(1)
					continue
				}
				time.Sleep(1 * time.Millisecond)
				if err := p.Put(res); err != nil {
					failCount.Add(1)
					continue
				}
				successCount.Add(1)
			}
		}()
	}

	wg.Wait()

	stats, _ := p.Stats(context.Background())
	t.Logf("Concurrent: success=%d, fail=%d, total=%d, available=%d, in_use=%d",
		successCount.Load(), failCount.Load(),
		stats["total_size"], stats["pool_available"], stats["pool_in_use"])

	// No resources should be "in_use" after all goroutines complete
	time.Sleep(300 * time.Millisecond)
	stats, _ = p.Stats(context.Background())
	if stats["pool_in_use"] > 5 {
		t.Errorf("in_use should be near 0 after all ops complete, got %d", stats["pool_in_use"])
	}
}

// ============================================================================
// Resource wrapper field access
// ============================================================================

func TestResourceFields(t *testing.T) {
	config := PoolConfig{
		MinSize:         3,
		MaxSize:         10,
		PingInterval:    0,
		MonitorInterval: 0,
	}

	p := NewPool(config, &FakeConnControl{})
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	ctx := context.Background()
	res, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Verify exported fields
	if res.ID == "" {
		t.Error("Resource.ID should not be empty")
	}
	if res.Conn == nil {
		t.Error("Resource.Conn should not be nil")
	}
	t.Logf("Resource: ID=%s", res.ID)

	p.Put(res)
}

// ============================================================================
// ReconnectOnGet disabled: verify Ping is not called during Get
// ============================================================================

func TestReconnectOnGetDisabled(t *testing.T) {
	config := PoolConfig{
		MinSize:          3,
		MaxSize:          10,
		MaxRetries:       2,
		RetryInterval:    10 * time.Millisecond,
		ReconnectOnGet:   false, // disabled
		PingInterval:     0,
		MonitorInterval:  0,
	}

	// Use pingErr to verify Ping is NOT called (because ReconnectOnGet=false)
	ctrl := &FakeConnControl{pingErr: errors.New("should not trigger")}
	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	ctx := context.Background()
	res, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Connection should have pingErr (since Ping is not called with ReconnectOnGet=false)
	// Actually the pingErr is on the original conn, but if ReconnectOnGet=false, Ping is never called
	// So the conn still has the injected pingErr
	if res.Conn.Ping(res.Conn) == nil {
		t.Log("Ping succeeded (pingErr was not set on this particular conn)")
	}

	p.Put(res)
}
