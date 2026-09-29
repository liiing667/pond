package pond

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alitto/pond/v2/internal/assert"
)

// executionLog records the order in which tasks start executing.
type executionLog struct {
	mutex   sync.Mutex
	entries []string
}

func (l *executionLog) record(entry string) {
	l.mutex.Lock()
	l.entries = append(l.entries, entry)
	l.mutex.Unlock()
}

func (l *executionLog) snapshot() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return append([]string(nil), l.entries...)
}

func waitForCondition(t *testing.T, message string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("Timed out waiting for condition: %s", message)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestGroupPoolFIFOWithinSamePriority(t *testing.T) {

	pool := NewGroupPool(1)
	defer pool.StopAndWait()

	log := &executionLog{}

	var futures []Task
	for i := 0; i < 50; i++ {
		for _, group := range []string{"a", "b"} {
			entry := fmt.Sprintf("%s-%d", group, i)
			futures = append(futures, pool.Submit(group, func() {
				log.record(entry)
			}))
		}
	}

	for _, future := range futures {
		assert.Equal(t, nil, future.Wait())
	}

	entries := log.snapshot()
	assert.Equal(t, 100, len(entries))

	// Tasks with the same priority must execute in submission order
	for i := 0; i < 50; i++ {
		assert.Equal(t, fmt.Sprintf("a-%d", i), entries[i*2])
		assert.Equal(t, fmt.Sprintf("b-%d", i), entries[i*2+1])
	}
}

func TestGroupPoolPriorityPreventsStarvation(t *testing.T) {

	pool := NewGroupPool(1)
	defer pool.StopAndWait()

	log := &executionLog{}

	gate := make(chan struct{})
	gateTask := pool.Submit("bulk", func() {
		<-gate
	})

	// Submit a large batch of low priority tasks
	bulkFutures := make([]Task, 100)
	for i := 0; i < 100; i++ {
		entry := fmt.Sprintf("bulk-%d", i)
		bulkFutures[i] = pool.Submit("bulk", func() {
			log.record(entry)
		})
	}

	// Submit a few high priority tasks after the bulk batch
	notifyFutures := make([]Task, 5)
	for i := 0; i < 5; i++ {
		entry := fmt.Sprintf("notify-%d", i)
		notifyFutures[i] = pool.Submit("notify", func() {
			log.record(entry)
		}, WithPriority(10))
	}

	// Wait until the dispatcher has dequeued exactly one bulk task
	// (it is blocked waiting for the worker slot held by the gate task)
	waitForCondition(t, "one bulk task to be dequeued", func() bool {
		return pool.Stats("bulk").Queued == 99
	})

	close(gate)

	assert.Equal(t, nil, gateTask.Wait())
	for _, future := range notifyFutures {
		assert.Equal(t, nil, future.Wait())
	}
	for _, future := range bulkFutures {
		assert.Equal(t, nil, future.Wait())
	}

	entries := log.snapshot()
	assert.Equal(t, 105, len(entries))

	// The single bulk task that was already being handed off runs first,
	// then all high priority tasks in submission order, then the rest of
	// the bulk batch in submission order.
	expected := make([]string, 0, 105)
	expected = append(expected, "bulk-0")
	for i := 0; i < 5; i++ {
		expected = append(expected, fmt.Sprintf("notify-%d", i))
	}
	for i := 1; i < 100; i++ {
		expected = append(expected, fmt.Sprintf("bulk-%d", i))
	}
	for i, entry := range expected {
		assert.Equal(t, entry, entries[i])
	}
}

func TestGroupPoolPauseResume(t *testing.T) {

	pool := NewGroupPool(1)
	defer pool.StopAndWait()

	// Pausing a group that does not exist yet creates it
	pool.Pause("reconcile")
	assert.True(t, pool.Paused("reconcile"))

	gate := make(chan struct{})
	gateTask := pool.Submit("notify", func() {
		<-gate
	})

	var reconciled atomic.Int64
	reconcileFutures := make([]Task, 10)
	for i := 0; i < 10; i++ {
		reconcileFutures[i] = pool.Submit("reconcile", func() {
			reconciled.Add(1)
		})
	}

	var notified atomic.Int64
	notifyFutures := make([]Task, 5)
	for i := 0; i < 5; i++ {
		notifyFutures[i] = pool.Submit("notify", func() {
			notified.Add(1)
		})
	}

	close(gate)

	assert.Equal(t, nil, gateTask.Wait())
	for _, future := range notifyFutures {
		assert.Equal(t, nil, future.Wait())
	}

	// Paused group tasks must not have been picked up
	assert.Equal(t, int64(0), reconciled.Load())
	assert.Equal(t, int64(5), notified.Load())

	stats := pool.Stats("reconcile")
	assert.Equal(t, uint64(10), stats.Submitted)
	assert.Equal(t, uint64(10), stats.Queued)
	assert.Equal(t, uint64(0), stats.Completed)

	// Resuming the group allows its tasks to run
	pool.Resume("reconcile")
	assert.True(t, !pool.Paused("reconcile"))

	for _, future := range reconcileFutures {
		assert.Equal(t, nil, future.Wait())
	}
	assert.Equal(t, int64(10), reconciled.Load())

	stats = pool.Stats("reconcile")
	assert.Equal(t, uint64(10), stats.Completed)
	assert.Equal(t, uint64(0), stats.Queued)
}

func TestGroupPoolStats(t *testing.T) {

	pool := NewGroupPool(5)
	defer pool.StopAndWait()

	sampleErr := errors.New("sample error")

	futures := make([]Task, 0, 14)
	for i := 0; i < 10; i++ {
		futures = append(futures, pool.Submit("report", func() {}))
	}
	for i := 0; i < 3; i++ {
		futures = append(futures, pool.SubmitErr("report", func() error {
			return sampleErr
		}))
	}
	futures = append(futures, pool.Submit("report", func() {
		panic(sampleErr)
	}))

	for _, future := range futures {
		future.Wait()
	}

	stats := pool.Stats("report")
	assert.Equal(t, uint64(14), stats.Submitted)
	assert.Equal(t, uint64(10), stats.Completed)
	assert.Equal(t, uint64(4), stats.Failed)
	assert.Equal(t, uint64(0), stats.Queued)

	// Unknown groups report zeroed stats
	assert.Equal(t, GroupStats{}, pool.Stats("unknown"))
}

func TestGroupPoolGroups(t *testing.T) {

	pool := NewGroupPool(1)
	defer pool.StopAndWait()

	pool.Submit("b", func() {}).Wait()
	pool.Submit("a", func() {}).Wait()
	pool.Pause("c")

	groups := pool.Groups()
	assert.Equal(t, 3, len(groups))
	assert.Equal(t, "a", groups[0])
	assert.Equal(t, "b", groups[1])
	assert.Equal(t, "c", groups[2])
}

func TestGroupPoolStopDropsQueuedTasks(t *testing.T) {

	pool := NewGroupPool(1)

	started := make(chan struct{})
	gate := make(chan struct{})
	running := pool.Submit("reconcile", func() {
		close(started)
		<-gate
	})

	var executed atomic.Int64
	queued := make([]Task, 10)
	for i := 0; i < 10; i++ {
		queued[i] = pool.Submit("reconcile", func() {
			executed.Add(1)
		})
	}

	<-started

	stop := pool.Stop()
	close(gate)
	stop.Wait()

	// The running task completed successfully
	assert.Equal(t, nil, running.Wait())

	// Queued tasks were dropped without executing
	assert.Equal(t, int64(0), executed.Load())
	for _, future := range queued {
		assert.True(t, errors.Is(future.Wait(), ErrPoolStopped))
	}

	assert.True(t, pool.Stopped())

	// Statistics remain readable after the pool is stopped
	stats := pool.Stats("reconcile")
	assert.Equal(t, uint64(11), stats.Submitted)
	assert.Equal(t, uint64(1), stats.Completed)
	assert.Equal(t, uint64(0), stats.Queued)

	// Stopping again is a no-op
	pool.StopAndWait()
}

func TestGroupPoolSubmitAfterStop(t *testing.T) {

	pool := NewGroupPool(1)
	pool.StopAndWait()

	assert.Equal(t, ErrPoolStopped, pool.Go("g", func() {}))
	assert.True(t, errors.Is(pool.Submit("g", func() {}).Wait(), ErrPoolStopped))
	assert.True(t, errors.Is(pool.SubmitErr("g", func() error { return nil }).Wait(), ErrPoolStopped))
}

func TestGroupPoolContextCancel(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	pool := NewGroupPool(1, WithContext(ctx))

	started := make(chan struct{})
	gate := make(chan struct{})
	running := pool.Submit("g", func() {
		close(started)
		<-gate
	})
	queued := pool.Submit("g", func() {})

	<-started
	cancel()
	close(gate)

	// The running task is allowed to complete, but its future is resolved
	// with the context error, just like regular pools.
	assert.True(t, errors.Is(running.Wait(), context.Canceled))
	assert.True(t, queued.Wait() != nil)

	waitForCondition(t, "pool to stop after context cancellation", pool.Stopped)

	// Statistics remain readable after the context is cancelled
	assert.Equal(t, uint64(2), pool.Stats("g").Submitted)
}

func TestGroupPoolConcurrencyLimit(t *testing.T) {

	pool := NewGroupPool(3)
	defer pool.StopAndWait()

	var current, max atomic.Int64

	futures := make([]Task, 0, 60)
	for i := 0; i < 20; i++ {
		for _, group := range []string{"low", "high"} {
			var opts []GroupOption
			if group == "high" {
				opts = append(opts, WithPriority(1))
			}
			futures = append(futures, pool.Submit(group, func() {
				running := current.Add(1)
				for {
					observed := max.Load()
					if running <= observed || max.CompareAndSwap(observed, running) {
						break
					}
				}
				time.Sleep(2 * time.Millisecond)
				current.Add(-1)
			}, opts...))
		}
	}

	for _, future := range futures {
		assert.Equal(t, nil, future.Wait())
	}

	assert.True(t, max.Load() > 0)
	assert.True(t, max.Load() <= 3)
	assert.Equal(t, 3, pool.MaxConcurrency())
}

func TestGroupPoolTaskErrorsArePropagated(t *testing.T) {

	pool := NewGroupPool(2)
	defer pool.StopAndWait()

	sampleErr := errors.New("sample error")

	errTask := pool.SubmitErr("g", func() error {
		return sampleErr
	})
	assert.Equal(t, sampleErr, errTask.Wait())

	panicTask := pool.Submit("g", func() {
		panic(sampleErr)
	})
	assert.True(t, errors.Is(panicTask.Wait(), ErrPanic))
}
