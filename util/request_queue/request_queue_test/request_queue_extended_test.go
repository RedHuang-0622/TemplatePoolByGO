// request_queue_extended_test.go — additional tests for LockFreeQueue
// Covers: TryDequeue edge cases (maxAttempts exhaustion, head==tail path, channel full)
package request_queue_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/RedHuang-0622/TemplatePoolByGO/util/request_queue"
)

// ============================================================================
// TryDequeue: head == tail (empty after helping advance tail)
// ============================================================================

func TestTryDequeue_HeadEqualsTail(t *testing.T) {
	q := NewLockFreeQueue[int]()

	// Empty queue: head == tail (both point to sentinel)
	success := q.TryDequeue(42)
	if success {
		t.Error("TryDequeue should fail on empty queue")
	}

	if q.Len() != 0 {
		t.Errorf("queue length should remain 0, got %d", q.Len())
	}
}

// ============================================================================
// TryDequeue: Cancelled node cleanup
// ============================================================================

func TestTryDequeue_SkipsMultipleCancelledNodes(t *testing.T) {
	q := NewLockFreeQueue[int]()

	// Enqueue several waiters, cancel some of them
	w1 := q.Enqueue() // will be cancelled
	w2 := q.Enqueue() // will be cancelled
	w3 := q.Enqueue() // valid

	q.Remove(w1)
	q.Remove(w2)

	// TryDequeue should skip w1 and w2, deliver to w3
	success := q.TryDequeue(999)
	if !success {
		t.Fatal("TryDequeue should succeed after skipping cancelled nodes")
	}

	// w3 should receive the value
	select {
	case val := <-w3.Ch:
		if val != 999 {
			t.Errorf("Expected 999, got %d", val)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("w3 failed to receive value")
	}

	// Queue should be empty now
	if q.Len() != 0 {
		t.Errorf("Queue should be empty, got %d", q.Len())
	}
}

// ============================================================================
// TryDequeue: maxAttempts exhaustion with very high contention
// ============================================================================

func TestTryDequeue_HighContentionManyTryDequeue(t *testing.T) {
	q := NewLockFreeQueue[int]()

	const numWaiters = 500
	const numProducers = 100

	// Create waiters
	for i := 0; i < numWaiters; i++ {
		q.Enqueue()
	}

	var received atomic.Int64
	var wg sync.WaitGroup

	// Consumer goroutines (each consumes one)
	for i := 0; i < numWaiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := q.Enqueue()
			select {
			case <-w.Ch:
				received.Add(1)
			case <-time.After(3 * time.Second):
				// timeout
			}
		}()
	}

	// Producers try to dequeue (each sends one value)
	go func() {
		values := make([]int, numProducers)
		for i := 0; i < numProducers; i++ {
			values[i] = i
		}
		for i := 0; i < numProducers; i++ {
			q.TryDequeue(values[i])
			if i%50 == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Don't wait for all — just verify no deadlocks
	time.Sleep(500 * time.Millisecond)
}

// ============================================================================
// TryDequeue: with concurrent Enqueue and Remove
// ============================================================================

func TestTryDequeue_ConcurrentEnqueueAndRemove(t *testing.T) {
	q := NewLockFreeQueue[int]()

	var readyWg, startWg sync.WaitGroup
	const goroutines = 50

	var enqueueSuccess atomic.Int64
	var removeSuccess atomic.Int64
	var dequeueSuccess atomic.Int64

	startWg.Add(1) // gate
	for i := 0; i < goroutines; i++ {
		readyWg.Add(3)

		// Enqueuers
		go func() {
			readyWg.Done()
			startWg.Wait()
			w := q.Enqueue()
			// Immediately try to read (it may or may not succeed)
			select {
			case <-w.Ch:
				enqueueSuccess.Add(1)
			case <-time.After(200 * time.Millisecond):
			}
		}()

		// Removers (cancel what was just enqueued)
		go func() {
			readyWg.Done()
			startWg.Wait()
			w := q.Enqueue()
			q.Remove(w) // self-cancel
			if q.TryDequeue(0) {
				// we may have gotten someone else's node
				dequeueSuccess.Add(1)
			}
			removeSuccess.Add(1)
		}()

		// Pure dequeuers
		go func() {
			readyWg.Done()
			startWg.Wait()
			if q.TryDequeue(42) {
				dequeueSuccess.Add(1)
			}
		}()
	}

	readyWg.Wait()
	startWg.Done()

	time.Sleep(500 * time.Millisecond)

	t.Logf("Concurrent: enqueue_success=%d, remove=%d, dequeue=%d, len=%d",
		enqueueSuccess.Load(), removeSuccess.Load(), dequeueSuccess.Load(), q.Len())
}

// ============================================================================
// Len: consistency check
// ============================================================================

func TestLen_Consistency(t *testing.T) {
	q := NewLockFreeQueue[int]()

	if q.Len() != 0 {
		t.Errorf("new queue len should be 0, got %d", q.Len())
	}

	const count = 100
	for i := 0; i < count; i++ {
		q.Enqueue()
	}

	// Len() is approximate due to lock-free nature, but should be close
	l := q.Len()
	if l < count/2 || l > count*2 {
		t.Errorf("Len should be approximately %d, got %d", count, l)
	}

	q.Clear()
	if q.Len() != 0 {
		t.Errorf("after Clear, Len should be 0, got %d", q.Len())
	}
}

// ============================================================================
// Remove: idempotent (double remove)
// ============================================================================

func TestRemove_DoubleRemove(t *testing.T) {
	q := NewLockFreeQueue[int]()

	w := q.Enqueue()
	q.Remove(w)
	// Second remove should be no-op
	q.Remove(w)

	// Enqueue a valid waiter and verify TryDequeue skips the cancelled one
	w2 := q.Enqueue()
	success := q.TryDequeue(77)
	if !success {
		t.Fatal("TryDequeue should succeed")
	}

	select {
	case val := <-w2.Ch:
		if val != 77 {
			t.Errorf("Expected 77, got %d", val)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("w2 failed to receive")
	}
}

// ============================================================================
// Clear: already empty queue
// ============================================================================

func TestClear_AlreadyEmpty(t *testing.T) {
	q := NewLockFreeQueue[int]()

	q.Clear() // should not panic
	if q.Len() != 0 {
		t.Errorf("len should be 0, got %d", q.Len())
	}
}

// ============================================================================
// Enqueue/Dequeue: single waiter, single resource
// ============================================================================

func TestEnqueueDequeue_SinglePair(t *testing.T) {
	q := NewLockFreeQueue[string]()

	w := q.Enqueue()
	if q.Len() != 1 {
		t.Errorf("len should be 1, got %d", q.Len())
	}

	success := q.TryDequeue("hello")
	if !success {
		t.Fatal("TryDequeue should succeed")
	}

	select {
	case val := <-w.Ch:
		if val != "hello" {
			t.Errorf("expected 'hello', got '%s'", val)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("waiter timed out")
	}

	if q.Len() != 0 {
		t.Errorf("len should be 0, got %d", q.Len())
	}
}

// ============================================================================
// Recycle: channel returned to pool
// ============================================================================

func TestRecycle_ChannelReused(t *testing.T) {
	q := NewLockFreeQueue[int]()

	// Enqueue many, all cancelled, then dequeued (channels recycled)
	for round := 0; round < 5; round++ {
		waiters := make([]*LockFreeWaiter[int], 10)
		for i := 0; i < 10; i++ {
			waiters[i] = q.Enqueue()
		}

		// Cancel half
		for i := 0; i < 5; i++ {
			q.Remove(waiters[i])
		}

		// Dequeue all (should skip cancelled, deliver to valid)
		delivered := 0
		for i := 0; i < 10; i++ {
			if q.TryDequeue(round*100 + i) {
				delivered++
			}
		}

		t.Logf("Round %d: delivered=%d, len=%d", round, delivered, q.Len())
	}

	// Should still be functional
	w := q.Enqueue()
	if q.TryDequeue(42) {
		select {
		case val := <-w.Ch:
			if val != 42 {
				t.Errorf("expected 42, got %d", val)
			}
		case <-time.After(500 * time.Millisecond):
			t.Error("final waiter timed out")
		}
	}
}

// ============================================================================
// TryDequeue: maxAttempts exhaustion under extreme pressure
// ============================================================================

func TestTryDequeue_MaxAttemptsExhaustion(t *testing.T) {
	q := NewLockFreeQueue[int]()

	// Fill queue with many waiters
	const numWaiters = 500
	waiters := make([]*LockFreeWaiter[int], numWaiters)
	for i := 0; i < numWaiters; i++ {
		waiters[i] = q.Enqueue()
	}

	// Spawn many goroutines that all try to dequeue simultaneously
	// This creates CAS contention that can exhaust maxAttempts
	var wg sync.WaitGroup
	var successCount atomic.Int64
	var failCount atomic.Int64

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if q.TryDequeue(idx) {
				successCount.Add(1)
			} else {
				failCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	// Some should succeed (at least one), some may fail due to contention
	total := successCount.Load() + failCount.Load()
	t.Logf("TryDequeue under pressure: success=%d, fail=%d, total=%d, len=%d",
		successCount.Load(), failCount.Load(), total, q.Len())

	if successCount.Load() == 0 {
		t.Error("at least some TryDequeue should succeed")
	}

	// Clean up remaining waiters
	q.Clear()
	for _, w := range waiters {
		select {
		case <-w.Ch:
		default:
		}
	}
}
