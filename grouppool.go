package pond

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/alitto/pond/v2/internal/future"
)

// GroupStats contains statistics about the tasks submitted to a single group.
type GroupStats struct {
	// Total number of tasks submitted to the group.
	Submitted uint64

	// Number of tasks that finished successfully.
	Completed uint64

	// Number of tasks that finished with an error, a panic or were canceled
	// after being dispatched to a worker.
	Failed uint64

	// Number of tasks currently waiting in the group's queue to be dispatched.
	Queued uint64
}

// GroupOption customizes how a task is submitted to a group.
type GroupOption func(*groupSubmitConfig)

// WithPriority sets the priority of the submitted task.
// Tasks with a higher priority value are dispatched before tasks with a lower
// priority value. Tasks with the same priority are dispatched in submission order.
// The default priority is 0.
func WithPriority(priority int) GroupOption {
	return func(config *groupSubmitConfig) {
		config.priority = priority
	}
}

type groupSubmitConfig struct {
	priority int
}

// GroupPool is a pool of goroutines that executes tasks submitted to named groups.
// Groups only affect the order in which queued tasks are picked for execution,
// the overall concurrency limit of the pool is unchanged.
//
// Dispatching order is deterministic: among all queued tasks belonging to groups
// that are not paused, the task with the highest priority is picked first, and
// ties are broken by submission order (FIFO). Tasks within the same group are
// always dispatched in submission order.
type GroupPool interface {

	// Submits a task to a group without waiting for it to complete.
	// If the pool has been stopped, this method will return ErrPoolStopped.
	Go(group string, task func(), opts ...GroupOption) error

	// Submits a task to a group and returns a future that can be used to wait for the task to complete.
	// If the pool has been stopped, the returned future will resolve to ErrPoolStopped.
	Submit(group string, task func(), opts ...GroupOption) Task

	// Submits a task to a group and returns a future that can be used to wait for the task to complete.
	// The task function must return an error.
	// If the pool has been stopped, the returned future will resolve to ErrPoolStopped.
	SubmitErr(group string, task func() error, opts ...GroupOption) Task

	// Pauses a group. Queued tasks belonging to a paused group are not picked
	// for execution until the group is resumed. Tasks that are already running
	// are not affected. Pausing a group that does not exist yet creates it,
	// so tasks submitted afterwards are queued but not dispatched.
	Pause(group string)

	// Resumes a paused group. Queued tasks become eligible for dispatch again.
	Resume(group string)

	// Returns true if the group is currently paused.
	Paused(group string) bool

	// Returns the statistics of a group. Statistics remain readable after the
	// pool has been stopped.
	Stats(group string) GroupStats

	// Returns the names of all known groups, sorted alphabetically.
	Groups() []string

	// Returns the maximum concurrency of the pool.
	MaxConcurrency() int

	// Returns the number of worker goroutines that are currently active (executing a task) in the pool.
	RunningWorkers() int64

	// Returns the context associated with this pool.
	Context() context.Context

	// Stops the pool and returns a future that can be used to wait for all running tasks to complete.
	// The pool will not accept new tasks after it has been stopped.
	// Tasks that are still queued will not be executed and their futures will resolve to ErrPoolStopped.
	Stop() Task

	// Stops the pool and waits for all running tasks to complete.
	StopAndWait()

	// Returns true if the pool has been stopped or its context has been cancelled.
	Stopped() bool
}

// groupTask is a task waiting to be dispatched to the underlying pool.
type groupTask struct {
	seq      uint64
	priority int
	state    *groupState
	run      func() error
	resolve  future.FutureResolver
}

// groupQueue is a FIFO queue of group tasks.
type groupQueue struct {
	items []*groupTask
	head  int
}

func (q *groupQueue) push(task *groupTask) {
	q.items = append(q.items, task)
}

func (q *groupQueue) pushFront(task *groupTask) {
	if q.head > 0 {
		q.head--
		q.items[q.head] = task
		return
	}
	q.items = append([]*groupTask{task}, q.items...)
}

func (q *groupQueue) peek() *groupTask {
	if q.head < len(q.items) {
		return q.items[q.head]
	}
	return nil
}

func (q *groupQueue) pop() *groupTask {
	task := q.peek()
	if task == nil {
		return nil
	}
	q.items[q.head] = nil
	q.head++
	// Compact the queue once the consumed prefix is large enough
	if q.head >= 64 && q.head*2 >= len(q.items) {
		q.items = append([]*groupTask(nil), q.items[q.head:]...)
		q.head = 0
	}
	return task
}

func (q *groupQueue) len() int {
	return len(q.items) - q.head
}

// groupState holds the queue and statistics of a single group.
type groupState struct {
	paused    bool
	queue     groupQueue
	submitted atomic.Uint64
	completed atomic.Uint64
	failed    atomic.Uint64
}

type groupPool struct {
	pool         *pool
	ctx          context.Context
	cancel       context.CancelCauseFunc
	mutex        sync.Mutex
	cond         sync.Cond
	groups       map[string]*groupState
	seq          uint64
	stopped      atomic.Bool
	stopOnce     sync.Once
	stopFuture   *future.Future
	stopResolve  future.FutureResolver
	dispatchDone chan struct{}
}

// NewGroupPool creates a new pool that dispatches tasks submitted to named groups
// in a deterministic order: higher priority tasks are dispatched first and tasks
// with the same priority are dispatched in submission order.
//
// The pool's overall concurrency limit works exactly like NewPool. Groups only
// affect the order in which queued tasks are picked for execution.
//
// The WithQueueSize and WithNonBlocking options are ignored because the group
// pool manages its own unbounded queue.
func NewGroupPool(maxConcurrency int, options ...Option) GroupPool {
	inner := newPool(maxConcurrency, nil, options...)

	// The group pool keeps queued tasks in its own group queues and hands them
	// to the underlying pool only when a worker slot is free, so the underlying
	// pool must not queue tasks itself.
	inner.queueSize = 0
	inner.nonBlocking = false

	pool := &groupPool{
		pool:         inner,
		groups:       make(map[string]*groupState),
		dispatchDone: make(chan struct{}),
	}
	pool.ctx, pool.cancel = context.WithCancelCause(inner.Context())
	pool.cond = sync.Cond{L: &pool.mutex}
	pool.stopFuture, pool.stopResolve = future.NewFuture(context.Background())

	go pool.dispatchLoop()

	// Shut the pool down when its context is cancelled.
	go func() {
		<-pool.ctx.Done()
		pool.stopOnce.Do(func() {
			go pool.shutdown(errors.Join(ErrContextCanceled, pool.ctx.Err()))
		})
	}()

	return pool
}

func (p *groupPool) Context() context.Context {
	return p.ctx
}

func (p *groupPool) Stopped() bool {
	return p.stopped.Load() || p.ctx.Err() != nil
}

func (p *groupPool) MaxConcurrency() int {
	return p.pool.MaxConcurrency()
}

func (p *groupPool) RunningWorkers() int64 {
	return p.pool.RunningWorkers()
}

func (p *groupPool) Go(group string, task func(), opts ...GroupOption) error {
	if _, ok := p.wrapAndSubmit(group, task, opts); !ok {
		return ErrPoolStopped
	}
	return nil
}

func (p *groupPool) Submit(group string, task func(), opts ...GroupOption) Task {
	future, _ := p.wrapAndSubmit(group, task, opts)
	return future
}

func (p *groupPool) SubmitErr(group string, task func() error, opts ...GroupOption) Task {
	future, _ := p.wrapAndSubmit(group, task, opts)
	return future
}

func (p *groupPool) wrapAndSubmit(group string, task any, opts []GroupOption) (Task, bool) {
	if p.Stopped() {
		return poolStoppedFuture, false
	}

	config := groupSubmitConfig{}
	for _, opt := range opts {
		opt(&config)
	}

	future, resolve := future.NewFuture(p.ctx)

	p.mutex.Lock()

	// Check if the pool has been stopped while holding the lock to avoid
	// enqueueing tasks that would never be dispatched nor resolved.
	if p.Stopped() {
		p.mutex.Unlock()
		resolve(ErrPoolStopped)
		return future, false
	}

	state := p.groupStateLocked(group)
	state.submitted.Add(1)

	run := wrapTask[struct{}, func(error)](task, func(err error) {
		if err != nil {
			state.failed.Add(1)
		} else {
			state.completed.Add(1)
		}
		resolve(err)
	}, p.ctx, p.pool.panicRecovery)

	p.seq++
	state.queue.push(&groupTask{
		seq:      p.seq,
		priority: config.priority,
		state:    state,
		run:      run,
		resolve:  resolve,
	})

	p.mutex.Unlock()

	// Wake up the dispatcher, there is a new task to dispatch
	p.cond.Signal()

	return future, true
}

func (p *groupPool) Pause(group string) {
	p.mutex.Lock()
	p.groupStateLocked(group).paused = true
	p.mutex.Unlock()
}

func (p *groupPool) Resume(group string) {
	p.mutex.Lock()
	if state, ok := p.groups[group]; ok {
		state.paused = false
	}
	p.mutex.Unlock()

	// Wake up the dispatcher, a paused group may have become runnable
	p.cond.Signal()
}

func (p *groupPool) Paused(group string) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	state, ok := p.groups[group]
	return ok && state.paused
}

func (p *groupPool) Stats(group string) GroupStats {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	state, ok := p.groups[group]
	if !ok {
		return GroupStats{}
	}

	return GroupStats{
		Submitted: state.submitted.Load(),
		Completed: state.completed.Load(),
		Failed:    state.failed.Load(),
		Queued:    uint64(state.queue.len()),
	}
}

func (p *groupPool) Groups() []string {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	groups := make([]string, 0, len(p.groups))
	for name := range p.groups {
		groups = append(groups, name)
	}
	sort.Strings(groups)

	return groups
}

func (p *groupPool) Stop() Task {
	p.stopOnce.Do(func() {
		// Stop accepting and dispatching new tasks synchronously, so that no
		// queued task can be handed off to a worker after Stop is called.
		p.mutex.Lock()
		p.stopped.Store(true)
		p.cond.Broadcast()
		p.mutex.Unlock()

		// Prevent the underlying pool from accepting a task handoff that is
		// already in flight.
		p.pool.closed.Store(true)

		go p.shutdown(ErrPoolStopped)
	})
	return p.stopFuture
}

func (p *groupPool) StopAndWait() {
	p.Stop().Wait()
}

// groupStateLocked returns the state of a group, creating it if necessary.
// The mutex must be held by the caller.
func (p *groupPool) groupStateLocked(group string) *groupState {
	state, ok := p.groups[group]
	if !ok {
		state = &groupState{}
		p.groups[group] = state
	}
	return state
}

// pickLocked returns the next task to dispatch: among all groups that are not
// paused and have queued tasks, the one whose oldest queued task has the highest
// priority wins, and ties are broken by submission order.
// The mutex must be held by the caller.
func (p *groupPool) pickLocked() *groupTask {
	var selected *groupState
	var head *groupTask

	for _, state := range p.groups {
		if state.paused {
			continue
		}
		candidate := state.queue.peek()
		if candidate == nil {
			continue
		}
		if head == nil || candidate.priority > head.priority ||
			(candidate.priority == head.priority && candidate.seq < head.seq) {
			selected = state
			head = candidate
		}
	}

	if selected == nil {
		return nil
	}

	return selected.queue.pop()
}

// next blocks until there is a task to dispatch or the pool is stopped.
func (p *groupPool) next() *groupTask {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for {
		if p.Stopped() {
			return nil
		}
		if task := p.pickLocked(); task != nil {
			return task
		}
		p.cond.Wait()
	}
}

// dispatchLoop dequeues tasks in scheduling order and hands them to the
// underlying pool. Handing a task blocks until a worker slot is free, so at
// most one task is ever waiting to be picked up by a worker and the dispatch
// order is preserved.
func (p *groupPool) dispatchLoop() {
	defer close(p.dispatchDone)

	for {
		task := p.next()
		if task == nil {
			return
		}

		if err := p.pool.Go(func() { task.run() }); err != nil {
			// The underlying pool no longer accepts tasks, put the task back
			// at the front of its group queue and exit. The shutdown procedure
			// resolves the futures of queued tasks once this loop exits.
			p.mutex.Lock()
			task.state.queue.pushFront(task)
			p.mutex.Unlock()
			return
		}
	}
}

// shutdown stops the pool: running tasks are allowed to complete, queued tasks
// are dropped and their futures are resolved with the given error.
func (p *groupPool) shutdown(err error) {
	// Stop accepting and dispatching new tasks while holding the lock to avoid
	// race conditions with submissions. This is a no-op if Stop was called.
	p.mutex.Lock()
	p.stopped.Store(true)
	p.cond.Broadcast()
	p.mutex.Unlock()

	// Wait for all running tasks to complete. This also unblocks the dispatcher
	// if it is waiting for a worker slot to hand off a task.
	p.pool.StopAndWait()

	// Wait for the dispatcher to exit before draining the queues
	<-p.dispatchDone

	// Resolve the futures of tasks that were still queued
	p.mutex.Lock()
	for _, state := range p.groups {
		for {
			task := state.queue.pop()
			if task == nil {
				break
			}
			task.resolve(err)
		}
	}
	p.mutex.Unlock()

	// Cancel the context to signal that the pool has been stopped
	p.cancel(err)

	p.stopResolve(nil)
}
