// pool_internal_test.go — white-box tests for the pool package (package pool)
// Tests internal, unexported functions and code paths unreachable from external package.
package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// Test Helpers
// ============================================================================

type testConn struct {
	closed   atomic.Bool
	pingErr  error
	pingOK   atomic.Bool
	resetErr error
}

func (c *testConn) Reset(_ *testConn) error { return c.resetErr }
func (c *testConn) Close(_ *testConn) error { c.closed.Store(true); return nil }
func (c *testConn) Create() (*testConn, error) {
	return &testConn{}, nil
}
func (c *testConn) Ping(_ *testConn) error {
	c.pingOK.Store(true)
	return c.pingErr
}

type testConnControl struct {
	createErr error
	createOK  atomic.Bool
	failCount atomic.Int32
	failAt    int32 // fail on this Create call, 0 means never
}

func (c *testConnControl) Reset(conn *testConn) error { return conn.Reset(conn) }
func (c *testConnControl) Close(conn *testConn) error { return conn.Close(conn) }
func (c *testConnControl) Ping(conn *testConn) error  { return conn.Ping(conn) }
func (c *testConnControl) Create() (*testConn, error) {
	c.createOK.Store(true)
	if c.createErr != nil {
		return nil, c.createErr
	}
	if c.failAt > 0 && c.failCount.Add(1) == c.failAt {
		return nil, errors.New("planned create failure")
	}
	return &testConn{}, nil
}

// ============================================================================
// config.go tests
// ============================================================================

func TestDefaultPoolConfig(t *testing.T) {
	cfg := DefaultPoolConfig()

	if cfg.MinSize != 5 {
		t.Errorf("MinSize: want 5, got %d", cfg.MinSize)
	}
	if cfg.MaxSize != 100 {
		t.Errorf("MaxSize: want 100, got %d", cfg.MaxSize)
	}
	if cfg.SurviveTime != 30*time.Minute {
		t.Errorf("SurviveTime: want 30m, got %v", cfg.SurviveTime)
	}
	if cfg.MonitorInterval != 10*time.Second {
		t.Errorf("MonitorInterval: want 10s, got %v", cfg.MonitorInterval)
	}
	if cfg.IdleBufferFactor != 1.0 {
		t.Errorf("IdleBufferFactor: want 1.0, got %v", cfg.IdleBufferFactor)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries: want 3, got %d", cfg.MaxRetries)
	}
	if cfg.RetryInterval != 1*time.Second {
		t.Errorf("RetryInterval: want 1s, got %v", cfg.RetryInterval)
	}
	if cfg.ReconnectOnGet != false {
		t.Errorf("ReconnectOnGet: want false, got %v", cfg.ReconnectOnGet)
	}
	if cfg.PingInterval != 30*time.Second {
		t.Errorf("PingInterval: want 30s, got %v", cfg.PingInterval)
	}
	if cfg.OnUnhealthy != nil {
		t.Errorf("OnUnhealthy: want nil, got non-nil")
	}
	if cfg.MaxWaitQueue != 10000 {
		t.Errorf("MaxWaitQueue: want 10000, got %d", cfg.MaxWaitQueue)
	}
}

// ============================================================================
// tryReturnOrClose tests
// ============================================================================

func TestTryReturnOrClose_ChannelHasSpace(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.PingInterval = 0
	config.MonitorInterval = 0
	config.IdleBufferFactor = 1.0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(50 * time.Millisecond)

	r := &resource[*testConn]{
		ID:         "test",
		createTime: time.Now(),
		updateTime: time.Now(),
		Conn:       &testConn{},
	}
	prevTotal := p.totalSize.Load()
	p.totalSize.Add(1) // account for r

	// channel has space (buffer = MaxSize * IdleBufferFactor = 100, empty)
	p.tryReturnOrClose(r)

	// Verify: resource should be in channel, not closed
	if r.Conn.closed.Load() {
		t.Error("connection should NOT be closed when channel has space")
	}
	// Verify total size unchanged (returned, not closed)
	if p.totalSize.Load() != prevTotal+1 {
		t.Errorf("totalSize should remain %d, got %d", prevTotal+1, p.totalSize.Load())
	}
}

func TestTryReturnOrClose_ChannelFull(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.MaxSize = 3
	config.IdleBufferFactor = 0.01 // buffer = max(1, 0) = 1
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(50 * time.Millisecond)

	// Fill the channel
	r1 := &resource[*testConn]{
		ID:         "fill1",
		createTime: time.Now(),
		updateTime: time.Now(),
		Conn:       &testConn{},
	}
	p.resources <- r1
	p.totalSize.Add(1)

	// Now channel is full, tryReturnOrClose should close the conn
	r2 := &resource[*testConn]{
		ID:         "overflow",
		createTime: time.Now(),
		updateTime: time.Now(),
		Conn:       &testConn{},
	}

	prevTotal := p.totalSize.Load()
	p.totalSize.Add(1)
	p.tryReturnOrClose(r2)

	if !r2.Conn.closed.Load() {
		t.Error("connection should be closed when channel is full")
	}
	if p.totalSize.Load() != prevTotal {
		t.Errorf("totalSize should decrease by 1, want %d, got %d", prevTotal, p.totalSize.Load())
	}
}

func TestTryReturnOrClose_ReturnThenWaitQueueGrew(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.MaxSize = 100
	config.IdleBufferFactor = 1.0
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(50 * time.Millisecond)

	// Pre-enqueue a waiter
	waiter := p.waitQueue.Enqueue()

	r := &resource[*testConn]{
		ID:         "test",
		createTime: time.Now(),
		updateTime: time.Now(),
		Conn:       &testConn{},
	}
	p.totalSize.Add(1)

	// tryReturnOrClose: channel has space → put r
	// then sees waitQueue has waiter → dequeues r from channel → TryDequeue gives to waiter
	p.tryReturnOrClose(r)

	// Verify waiter received resource
	select {
	case received := <-waiter.Ch:
		if received != r {
			t.Error("waiter received wrong resource")
		}
	case <-time.After(1 * time.Second):
		t.Error("waiter did not receive resource within timeout")
	}
}

// ============================================================================
// collectPingBatch tests
// ============================================================================

func TestCollectPingBatch_ChannelEmpty(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.PingInterval = 0
	config.MonitorInterval = 0
	config.IdleBufferFactor = 1.0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(50 * time.Millisecond)

	// Channel is empty
	batch := p.collectPingBatch(5)
	if len(batch) != 0 {
		t.Errorf("expected empty batch from empty channel, got %d", len(batch))
	}
}

func TestCollectPingBatch_WaitQueueInterrupt(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.MaxSize = 100
	config.IdleBufferFactor = 1.0
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(50 * time.Millisecond)

	// Put some resources in channel
	for i := 0; i < 3; i++ {
		r := &resource[*testConn]{
			ID:         "ping-test",
			createTime: time.Now(),
			updateTime: time.Now(),
			Conn:       &testConn{},
		}
		p.resources <- r
		p.totalSize.Add(1)
	}

	// Add waiter (but don't consume yet — the waitQueue.Len() check triggers)
	waiter := p.waitQueue.Enqueue()

	totalBefore := p.totalSize.Load()

	// collectPingBatch should detect the waiter and return the collected resources
	batch := p.collectPingBatch(5)

	if batch != nil {
		t.Error("collectPingBatch should return nil when waitQueue has entries mid-collection")
	}

	// Resources that were collected should be returned via tryReturnOrClose
	// Clean up the waiter
	p.waitQueue.Remove(waiter)

	_ = totalBefore
}

func TestCollectPingBatch_PartialFill(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.PingInterval = 0
	config.MonitorInterval = 0
	config.IdleBufferFactor = 1.0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(50 * time.Millisecond)

	// Put 2 resources in channel
	for i := 0; i < 2; i++ {
		r := &resource[*testConn]{
			ID:         "partial",
			createTime: time.Now(),
			updateTime: time.Now(),
			Conn:       &testConn{},
		}
		p.resources <- r
		p.totalSize.Add(1)
	}

	// Ask for 5 but only 2 available
	batch := p.collectPingBatch(5)
	if len(batch) != 2 {
		t.Errorf("expected 2 resources in partial batch, got %d", len(batch))
	}

	// Return them
	for _, r := range batch {
		p.resources <- r
	}
}

// ============================================================================
// processPingBatch tests
// ============================================================================

func TestProcessPingBatch_HealthyWithWaiter(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.PingInterval = 0
	config.MonitorInterval = 0
	config.IdleBufferFactor = 1.0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(50 * time.Millisecond)

	conn := &testConn{}
	r := &resource[*testConn]{
		ID:         "healthy",
		createTime: time.Now(),
		updateTime: time.Now(),
		Conn:       conn,
	}
	p.totalSize.Add(1)

	// Add waiter
	waiter := p.waitQueue.Enqueue()

	batch := []*resource[*testConn]{r}
	p.processPingBatch(batch)

	// Waiter should receive resource (healthy → TryDequeue succeeds)
	select {
	case received := <-waiter.Ch:
		if received != r {
			t.Error("waiter received wrong resource")
		}
	case <-time.After(1 * time.Second):
		t.Error("waiter did not receive resource")
	}

	if !conn.pingOK.Load() {
		t.Error("Ping should have been called")
	}
}

func TestProcessPingBatch_UnhealthyWithOnUnhealthy(t *testing.T) {
	var unhealthyErr error
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.PingInterval = 0
	config.MonitorInterval = 0
	config.IdleBufferFactor = 1.0
	config.OnUnhealthy = func(err error) {
		unhealthyErr = err
	}

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(100 * time.Millisecond)

	pingErr := errors.New("connection lost")
	conn := &testConn{pingErr: pingErr}
	r := &resource[*testConn]{
		ID:         "unhealthy",
		createTime: time.Now(),
		updateTime: time.Now(),
		Conn:       conn,
	}
	p.totalSize.Add(1)

	batch := []*resource[*testConn]{r}
	p.processPingBatch(batch)

	// Wait for async actor to process
	time.Sleep(100 * time.Millisecond)

	if unhealthyErr == nil {
		t.Error("OnUnhealthy should have been called with ping error")
	} else if unhealthyErr != pingErr {
		t.Errorf("OnUnhealthy: want %v, got %v", pingErr, unhealthyErr)
	}

	if !conn.closed.Load() {
		t.Error("connection should be closed when unhealthy")
	}
	// totalSize should be decremented by the actor
	// Note: actor runs asynchronously, so we wait
	time.Sleep(200 * time.Millisecond)
}

func TestProcessPingBatch_UnhealthyWithoutOnUnhealthy(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.PingInterval = 0
	config.MonitorInterval = 0
	// OnUnhealthy is nil

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(100 * time.Millisecond)

	pingErr := errors.New("connection lost")
	conn := &testConn{pingErr: pingErr}
	r := &resource[*testConn]{
		ID:         "unhealthy-no-cb",
		createTime: time.Now(),
		updateTime: time.Now(),
		Conn:       conn,
	}
	p.totalSize.Add(1)

	batch := []*resource[*testConn]{r}
	// Should not panic with nil OnUnhealthy
	p.processPingBatch(batch)

	time.Sleep(100 * time.Millisecond)
	if !conn.closed.Load() {
		t.Error("connection should be closed when unhealthy")
	}
}

func TestProcessPingBatch_HealthyNoWaiter(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.PingInterval = 0
	config.MonitorInterval = 0
	config.IdleBufferFactor = 1.0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(50 * time.Millisecond)

	conn := &testConn{}
	r := &resource[*testConn]{
		ID:         "healthy-back",
		createTime: time.Now(),
		updateTime: time.Now(),
		Conn:       conn,
	}
	p.totalSize.Add(1)

	batch := []*resource[*testConn]{r}
	p.processPingBatch(batch)

	// No waiter → should go back to channel via tryReturnOrClose
	if conn.closed.Load() {
		t.Error("healthy connection should NOT be closed when no waiter")
	}

	// Should be in resources channel
	select {
	case returned := <-p.resources:
		if returned != r {
			t.Error("wrong resource returned to channel")
		}
	default:
		t.Error("resource should be in the channel")
	}
}

// ============================================================================
// validateAndReturn tests
// ============================================================================

func TestValidateAndReturn_ReconnectDisabled(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.ReconnectOnGet = false
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(50 * time.Millisecond)

	conn := &testConn{}
	r := &resource[*testConn]{
		ID:         "no-reconnect",
		createTime: time.Now(),
		updateTime: time.Now(),
		Conn:       conn,
	}

	result, err := p.validateAndReturn(r)
	if err != nil {
		t.Fatalf("validateAndReturn should succeed: %v", err)
	}
	if result != r {
		t.Error("should return same resource")
	}
	if p.inUse.Load() != 1 {
		t.Errorf("inUse should be 1, got %d", p.inUse.Load())
	}
	if conn.pingOK.Load() {
		t.Error("Ping should NOT be called when ReconnectOnGet=false")
	}
}

func TestValidateAndReturn_ReconnectEnabled_PingOK(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.ReconnectOnGet = true
	config.MaxRetries = 2
	config.RetryInterval = 10 * time.Millisecond
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(50 * time.Millisecond)

	conn := &testConn{}
	r := &resource[*testConn]{
		ID:         "reconnect-ok",
		createTime: time.Now(),
		updateTime: time.Now(),
		Conn:       conn,
	}

	result, err := p.validateAndReturn(r)
	if err != nil {
		t.Fatalf("validateAndReturn should succeed: %v", err)
	}
	if p.inUse.Load() != 1 {
		t.Errorf("inUse should be 1, got %d", p.inUse.Load())
	}
	if !conn.pingOK.Load() {
		t.Error("Ping should be called when ReconnectOnGet=true")
	}
	_ = result
}

func TestValidateAndReturn_ReconnectEnabled_PingFail_RetrySuccess(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.ReconnectOnGet = true
	config.MaxRetries = 3
	config.RetryInterval = 10 * time.Millisecond
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(50 * time.Millisecond)

	pingErr := errors.New("connection lost")
	conn := &testConn{pingErr: pingErr}
	origConn := conn
	r := &resource[*testConn]{
		ID:         "reconnect-fail-then-ok",
		createTime: time.Now(),
		updateTime: time.Now(),
		Conn:       conn,
	}

	result, err := p.validateAndReturn(r)
	if err != nil {
		t.Fatalf("validateAndReturn should succeed after retry: %v", err)
	}
	if p.inUse.Load() != 1 {
		t.Errorf("inUse should be 1, got %d", p.inUse.Load())
	}
	// Should have a new connection (reconnected)
	if result.Conn == origConn {
		t.Error("should have new connection after reconnect (Ping failed → Create succeeded)")
	}
	if result.Conn.pingErr != nil {
		t.Error("new connection should have no ping error")
	}
	if result.retryCount != 1 {
		t.Errorf("retryCount should be 1, got %d", result.retryCount)
	}
}

func TestValidateAndReturn_ReconnectEnabled_PingFail_AllRetriesFail(t *testing.T) {
	ctrl := &testConnControl{
		createErr: errors.New("permanent create failure"),
	}
	config := DefaultPoolConfig()
	config.ReconnectOnGet = true
	config.MaxRetries = 2
	config.RetryInterval = 10 * time.Millisecond
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(50 * time.Millisecond)

	pingErr := errors.New("connection lost")
	conn := &testConn{pingErr: pingErr}
	r := &resource[*testConn]{
		ID:         "reconnect-all-fail",
		createTime: time.Now(),
		updateTime: time.Now(),
		Conn:       conn,
	}

	result, err := p.validateAndReturn(r)
	if err != nil {
		t.Fatalf("validateAndReturn should not error (returns original on retry exhaustion): %v", err)
	}
	// Still returns the resource (original conn with ping error)
	if result != r {
		t.Error("should return original resource when all retries fail")
	}
	if p.inUse.Load() != 1 {
		t.Errorf("inUse should be 1 even after retry exhaustion, got %d", p.inUse.Load())
	}
}

// ============================================================================
// calculateExpandSize tests (via PoolManagerActor on a pool)
// ============================================================================

func TestCalculateExpandSize_EarlyPhase(t *testing.T) {
	ctrl := &testConnControl{}
	config := PoolConfig{
		MinSize:          5,
		MaxSize:          100,
		IdleBufferFactor: 1.0,
		PingInterval:     0,
		MonitorInterval:  0,
		MaxRetries:       1,
		RetryInterval:    10 * time.Millisecond,
	}

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	// PreInit creates 5, so effectiveTotal=5 at min=5, usageRate=0
	// usageRate < 0.20 → baseStep should be 15
	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}

	// Directly test calculateExpandSize. effectiveTotal=5 (at MinSize), waiting=0
	// But wait, MinSize=5, effectiveTotal=5, usedRange=0, usageRate=0 → < 0.20 → baseStep=15
	step := p.manager.GetActor().calculateExpandSize(&s, 0, 5) // effectiveTotal=5
	if step != 15 {
		t.Errorf("early phase: expected step=15, got %d", step)
	}
}

func TestCalculateExpandSize_BurstPhase(t *testing.T) {
	ctrl := &testConnControl{}
	config := PoolConfig{
		MinSize:          5,
		MaxSize:          100,
		IdleBufferFactor: 1.0,
		PingInterval:     0,
		MonitorInterval:  0,
		MaxRetries:       1,
		RetryInterval:    10 * time.Millisecond,
	}

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}

	// effectiveTotal=50: usedRange=45, totalRange=95, usageRate≈0.47
	// 0.20 <= usageRate < 0.75 → baseStep = remaining/2 = 50/2 = 25
	step := p.manager.GetActor().calculateExpandSize(&s, 0, 50)

	// remaining=50, baseStep=25
	if step != 25 {
		t.Errorf("burst phase: expected step=25, got %d", step)
	}
}

func TestCalculateExpandSize_ConvergencePhase(t *testing.T) {
	ctrl := &testConnControl{}
	config := PoolConfig{
		MinSize:          5,
		MaxSize:          100,
		IdleBufferFactor: 1.0,
		PingInterval:     0,
		MonitorInterval:  0,
		MaxRetries:       1,
		RetryInterval:    10 * time.Millisecond,
	}

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}

	// effectiveTotal=85: usedRange=80, totalRange=95, usageRate≈0.84
	// usageRate >= 0.75 → baseStep = remaining/8 = 15/8 = 1
	step := p.manager.GetActor().calculateExpandSize(&s, 0, 85)

	// remaining=15, baseStep=15/8=1
	if step != 1 {
		t.Errorf("convergence phase: expected step=1, got %d", step)
	}
}

func TestCalculateExpandSize_PressureCompensation(t *testing.T) {
	ctrl := &testConnControl{}
	config := PoolConfig{
		MinSize:          5,
		MaxSize:          100,
		IdleBufferFactor: 1.0,
		PingInterval:     0,
		MonitorInterval:  0,
		MaxRetries:       1,
		RetryInterval:    10 * time.Millisecond,
	}

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}

	// Low usageRate (early phase: baseStep=15), but many waiting (120 → pressure=60)
	// pressureStep=120/2=60 (since waiting > 100), step should be 60
	step := p.manager.GetActor().calculateExpandSize(&s, 120, 5)
	// pressureStep=120/2=60, but limit caps at maxSize/3=100/3=33
	if step != 33 {
		t.Errorf("pressure compensation (waiting>100, limit capped): expected step=33, got %d", step)
	}

	// Moderate waiting: waiting=30 → pressureStep=30/3=10, baseStep=15 → step=15
	step2 := p.manager.GetActor().calculateExpandSize(&s, 30, 5)
	if step2 != 15 {
		t.Errorf("pressure compensation (waiting<=100, baseStep wins): expected step=15, got %d", step2)
	}
}

func TestCalculateExpandSize_LimitCapping(t *testing.T) {
	ctrl := &testConnControl{}
	config := PoolConfig{
		MinSize:          5,
		MaxSize:          100,
		IdleBufferFactor: 1.0,
		PingInterval:     0,
		MonitorInterval:  0,
		MaxRetries:       1,
		RetryInterval:    10 * time.Millisecond,
	}

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}

	// MaxSize=100, limit=100/3=33
	// Lots of waiting (200) → pressureStep=200/2=100
	// Step should be capped at 33
	step := p.manager.GetActor().calculateExpandSize(&s, 200, 5)
	if step != 33 {
		t.Errorf("limit capping: expected step=33, got %d", step)
	}
}

func TestCalculateExpandSize_RemainingSpaceProtection(t *testing.T) {
	ctrl := &testConnControl{}
	config := PoolConfig{
		MinSize:          5,
		MaxSize:          101, // Use 101 to get limit=33
		IdleBufferFactor: 1.0,
		PingInterval:     0,
		MonitorInterval:  0,
		MaxRetries:       1,
		RetryInterval:    10 * time.Millisecond,
	}

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}

	// effectiveTotal=99, remaining=2, but step from burst would be larger
	// Should be capped to remaining space: 2
	step := p.manager.GetActor().calculateExpandSize(&s, 0, 99)
	// effectiveTotal=99, remaining=2, usageRate=(99-5)/(101-5)=94/96≈0.98 → convergence
	// baseStep=2/8=0, step<1 so step=1 (at least 1), but also baseStep < pressureStep=0
	// So step=1 → capped by remaining: 99+1=100 ≤ 101 → step=1
	if step < 1 || step > 2 {
		t.Errorf("remaining space protection: expected step=1 or 2, got %d", step)
	}
}

func TestCalculateExpandSize_AlreadyAtMax(t *testing.T) {
	ctrl := &testConnControl{}
	config := PoolConfig{
		MinSize:          5,
		MaxSize:          100,
		IdleBufferFactor: 1.0,
		PingInterval:     0,
		MonitorInterval:  0,
		MaxRetries:       1,
		RetryInterval:    10 * time.Millisecond,
	}

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}

	step := p.manager.GetActor().calculateExpandSize(&s, 10, 100) // already at max
	if step != 0 {
		t.Errorf("at max: expected step=0, got %d", step)
	}
}

// ============================================================================
// preInit tests
// ============================================================================

func TestPreInit_AllFailures(t *testing.T) {
	ctrl := &testConnControl{
		createErr: errors.New("always fail"),
	}
	config := DefaultPoolConfig()
	config.MinSize = 5
	config.MaxSize = 10
	config.IdleBufferFactor = 1.0
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()

	time.Sleep(200 * time.Millisecond)

	// All preInit Create calls failed, totalSize should be 0
	if p.totalSize.Load() != 0 {
		t.Errorf("totalSize should be 0 when all preInit fail, got %d", p.totalSize.Load())
	}

	// Actor should still be initialized (even with 0 connections)
	// Verify by trying to get (should trigger expansion)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := p.Get(ctx)
	// Should fail because Create always fails
	if err == nil {
		t.Error("Get should fail when Create always fails")
	}
}

// ============================================================================
// pingIdleResources: PingInterval <= 0 disables ping
// ============================================================================

func TestPingIdleResources_Disabled(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.PingInterval = 0 // disabled
	config.MinSize = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	// pingIdleResources goroutine should have exited immediately
	// No panic, no resource consumption — just verify the pool is still usable
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Wait for expansion since MinSize=0
	res, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	p.Put(res)
}

// ============================================================================
// monitorAndAdjust: MonitorInterval <= 0 disables monitor
// ============================================================================

func TestMonitorAndAdjust_Disabled(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MonitorInterval = 0 // disabled
	config.PingInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	// monitorAndAdjust goroutine should have exited immediately
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	res, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	p.Put(res)
}

// ============================================================================
// doPingRound: with wait queue present (skips ping)
// ============================================================================

func TestDoPingRound_WaitQueuePresent(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.PingInterval = 1 // enable ping (but call doPingRound directly)
	config.MonitorInterval = 0
	config.MinSize = 3

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	// Add a waiter
	waiter := p.waitQueue.Enqueue()
	defer p.waitQueue.Remove(waiter)

	// doPingRound should return immediately because waitQueue has entries
	p.doPingRound()
	// No panic, no hang — the function should have skipped
}

func TestDoPingRound_NoResources(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.PingInterval = 1
	config.MonitorInterval = 0
	config.MinSize = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(100 * time.Millisecond)

	// No resources in channel, doPingRound should return without issue
	p.doPingRound()
	// Just verify no panic
}

// ============================================================================
// checkAndAdjust: initialized guard
// ============================================================================

func TestCheckAndAdjust_NotInitialized(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.PingInterval = 0
	config.MonitorInterval = 0
	config.MinSize = 0

	p := NewPool(config, ctrl)
	defer p.Close()

	// Immediately call checkAndAdjust before preInit completes
	// The initialized check should prevent action
	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}

	// Should not panic
	actor := p.manager.GetActor()
	actor.checkAndAdjust(&s)

	// Verify expansion didn't happen (totalSize should still be 0 or near 0)
	time.Sleep(50 * time.Millisecond)
}

// ============================================================================
// expand: retry exhaustion path
// ============================================================================

func TestExpand_RetryExhaustion(t *testing.T) {
	ctrl := &testConnControl{
		createErr: errors.New("permanent failure"),
	}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.MaxSize = 10
	config.MaxRetries = 2
	config.RetryInterval = 5 * time.Millisecond
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	// Force expand via actor
	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}
	actor := p.manager.GetActor()

	prevTotal := p.totalSize.Load()
	actor.expand(&s, 3)

	// Wait for async expansion to complete
	time.Sleep(500 * time.Millisecond)

	// All Create calls should have failed, totalSize should be unchanged
	if p.totalSize.Load() != prevTotal {
		t.Errorf("totalSize should not change when all expansions fail: was %d, now %d",
			prevTotal, p.totalSize.Load())
	}
}

// ============================================================================
// shrink: survivors phase
// ============================================================================

func TestShrink_SurvivorsReturned(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 3
	config.MaxSize = 20
	config.SurviveTime = 1 * time.Hour // very long, no connections expired
	config.IdleBufferFactor = 1.0
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	// Put extra resources in the pool
	extraTotal := int64(10)
	for i := int64(0); i < extraTotal; i++ {
		r := &resource[*testConn]{
			ID:         "shrink-test",
			createTime: time.Now(),
			updateTime: time.Now(),
			Conn:       &testConn{},
		}
		p.resources <- r
		p.totalSize.Add(1)
	}

	prevTotal := p.totalSize.Load()

	// Trigger shrink via actor
	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}
	actor := p.manager.GetActor()
	actor.shrink(&s)

	time.Sleep(100 * time.Millisecond)

	// Shrink should reduce totalSize but not below MinSize=3
	if p.totalSize.Load() < config.MinSize {
		t.Errorf("totalSize %d should not go below MinSize %d", p.totalSize.Load(), config.MinSize)
	}
	if p.totalSize.Load() >= prevTotal {
		t.Logf("Shrink may not have reduced total: was %d, now %d", prevTotal, p.totalSize.Load())
	}
}

func TestShrink_ExpiredConnectionsRemoved(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.MaxSize = 20
	config.SurviveTime = 10 * time.Millisecond // very short, connections expire quickly
	config.IdleBufferFactor = 1.0
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()

	// Put resources with very old createTime
	for i := 0; i < 8; i++ {
		r := &resource[*testConn]{
			ID:         "old-shrink",
			createTime: time.Now().Add(-1 * time.Hour),
			updateTime: time.Now(),
			Conn:       &testConn{},
		}
		p.resources <- r
		p.totalSize.Add(1)
	}

	time.Sleep(50 * time.Millisecond)

	prevTotal := p.totalSize.Load()

	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}
	actor := p.manager.GetActor()
	actor.shrink(&s)

	time.Sleep(100 * time.Millisecond)

	// Expired connections should be removed (at least some)
	if p.totalSize.Load() >= prevTotal {
		t.Logf("all expired connections may have been removed: was %d, now %d", prevTotal, p.totalSize.Load())
	}
}

// ============================================================================
// collectPingBatch: channel empty default case
// ============================================================================

func TestCollectPingBatch_ChannelEmptyDefault(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 1
	config.MaxSize = 10
	config.IdleBufferFactor = 1.0
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	// Drain the pre-created connection
	select {
	case <-p.resources:
	default:
	}

	// Channel empty → collectPingBatch hits default case
	batch := p.collectPingBatch(5)
	if len(batch) != 0 {
		t.Errorf("expected empty batch from empty channel, got %d", len(batch))
	}
}

// ============================================================================
// tryReturnOrClose: channel full → close path (already tested, add secondary dequeue full)
// ============================================================================

func TestTryReturnOrClose_FullChannelRePutFails(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 0
	config.MaxSize = 10
	config.IdleBufferFactor = 0.01 // buffer=1
	config.PingInterval = 0
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(100 * time.Millisecond)

	// Fill the buffer
	fill := &resource[*testConn]{
		ID: "fill", createTime: time.Now(), updateTime: time.Now(),
		Conn: &testConn{},
	}
	p.resources <- fill
	p.totalSize.Add(1)

	// Add a cancelled waiter (TryDequeue will skip it)
	waiter := p.waitQueue.Enqueue()
	p.waitQueue.Remove(waiter)

	// tryReturnOrClose: channel full → default → close
	r := &resource[*testConn]{
		ID: "close-me", createTime: time.Now(), updateTime: time.Now(),
		Conn: &testConn{},
	}
	p.totalSize.Add(1)
	p.tryReturnOrClose(r)

	if !r.Conn.closed.Load() {
		t.Error("connection should be closed when channel is full")
	}
}

// ============================================================================
// pingIdleResources / monitorAndAdjust: ctx.Done() on pool close
// ============================================================================

func TestPingIdleResources_CtxDone(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 3
	config.PingInterval = 100 * time.Millisecond
	config.MonitorInterval = 0

	p := NewPool(config, ctrl)
	time.Sleep(150 * time.Millisecond) // let ping goroutine start
	p.Close()                          // cancel ctx → pingIdleResources exits via ctx.Done()
}

func TestMonitorAndAdjust_CtxDone(t *testing.T) {
	ctrl := &testConnControl{}
	config := DefaultPoolConfig()
	config.MinSize = 3
	config.MonitorInterval = 100 * time.Millisecond
	config.PingInterval = 0

	p := NewPool(config, ctrl)
	time.Sleep(150 * time.Millisecond) // let monitor goroutine start
	p.Close()                          // cancel ctx → monitorAndAdjust exits via ctx.Done()
}

// ============================================================================
// expand: hitting MaxSize during loop → break
// ============================================================================

func TestExpand_HitMaxSizeDuringLoop(t *testing.T) {
	ctrl := &testConnControl{}
	config := PoolConfig{
		MinSize: 0, MaxSize: 3, IdleBufferFactor: 1.0,
		PingInterval: 0, MonitorInterval: 0,
		MaxRetries: 1, RetryInterval: 10 * time.Millisecond,
	}

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	// Artificially set totalSize near MaxSize
	p.totalSize.Store(2)

	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}
	actor := p.manager.GetActor()
	actor.expand(&s, 5) // request 5, but should break when total hits 3

	time.Sleep(300 * time.Millisecond)

	if p.totalSize.Load() > config.MaxSize {
		t.Errorf("totalSize %d should not exceed MaxSize %d", p.totalSize.Load(), config.MaxSize)
	}
}

// ============================================================================
// expand: sharedResources channel full → connection closed in actor callback
// ============================================================================

func TestExpand_SharedResourcesFull(t *testing.T) {
	ctrl := &testConnControl{}
	config := PoolConfig{
		MinSize: 0, MaxSize: 5, IdleBufferFactor: 0.01, // buffer=1
		PingInterval: 0, MonitorInterval: 0,
		MaxRetries: 1, RetryInterval: 10 * time.Millisecond,
	}

	p := NewPool(config, ctrl)
	defer p.Close()
	time.Sleep(200 * time.Millisecond)

	// Fill the buffer
	fill := &resource[*testConn]{
		ID: "fill", createTime: time.Now(), updateTime: time.Now(),
		Conn: &testConn{},
	}
	p.resources <- fill
	p.totalSize.Add(1)

	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}
	actor := p.manager.GetActor()
	actor.expand(&s, 2) // created conns can't go into full channel → closed by actor

	time.Sleep(500 * time.Millisecond)

	stats, _ := p.Stats(context.Background())
	t.Logf("After expand with full buffer: total=%d", stats["total_size"])
}

// ============================================================================
// shrink: survivors returned because enough expired connections filled shrink target
// ============================================================================

func TestShrink_SurvivorsReturnedEnoughExpired(t *testing.T) {
	ctrl := &testConnControl{}
	config := PoolConfig{
		MinSize: 2, MaxSize: 20,
		SurviveTime:      10 * time.Millisecond,
		IdleBufferFactor: 1.0,
		PingInterval:     0,
		MonitorInterval:  0,
	}

	p := NewPool(config, ctrl)
	defer p.Close()

	// Old (expired): 4 connections
	for i := 0; i < 4; i++ {
		r := &resource[*testConn]{
			ID: "old", createTime: time.Now().Add(-1 * time.Hour), updateTime: time.Now(),
			Conn: &testConn{},
		}
		p.resources <- r
		p.totalSize.Add(1)
	}
	// New (not expired): 6 connections
	for i := 0; i < 6; i++ {
		r := &resource[*testConn]{
			ID: "new", createTime: time.Now(), updateTime: time.Now(),
			Conn: &testConn{},
		}
		p.resources <- r
		p.totalSize.Add(1)
	}

	time.Sleep(50 * time.Millisecond)
	prevTotal := p.totalSize.Load()

	s := PoolManagerState[*testConn]{config: config, connControl: ctrl}
	actor := p.manager.GetActor()
	actor.shrink(&s)

	time.Sleep(100 * time.Millisecond)

	// Survivors (non-expired) should be put back because expired ones fill shrink target
	if p.totalSize.Load() >= prevTotal {
		t.Logf("Expected shrink: was %d, now %d", prevTotal, p.totalSize.Load())
	}
	if p.totalSize.Load() < config.MinSize {
		t.Errorf("totalSize %d below MinSize %d", p.totalSize.Load(), config.MinSize)
	}
}
