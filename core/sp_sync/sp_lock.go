package sp_sync

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Synchronization primitive,
// which can unlock multiple threads by single action,
// works like starter pistol

type SpLock struct {
	mutex     sync.Mutex
	sleep     time.Duration
	waitCount atomic.Int32
	version   atomic.Value
}

var ErrorTimeout = errors.New("Timeout reached")

func NewSpLock(sleep time.Duration) *SpLock {
	lock := new(SpLock)

	lock.sleep = sleep

	return lock
}

func IntCompare(target any) func(any) bool {
	return func(value any) bool {
		if value == nil {
			return false
		}
		switch target.(type) {
		case int:
			return value.(int) >= target.(int)
		case int32:
			return value.(int32) >= target.(int32)
		case int64:
			return value.(int64) >= target.(int64)
		default:
			return false
		}
	}
}

func (l *SpLock) LockUntilInt(target any, timeout time.Duration) error {
	return l.LockUntil(IntCompare(target), timeout)
}

func (l *SpLock) LockUntilIntChange(timeout time.Duration) error {
	return l.LockUntil(IntCompare(l.version.Load()), timeout)
}

func (l *SpLock) LockUntil(fn func(version any) bool, timeout time.Duration) error {
	l.waitCount.Add(1)
	defer l.waitCount.Add(-1)
	startAt := time.Now()
	for {
		if fn(l.version.Load()) {
			return nil
		}

		if time.Since(startAt) > timeout {
			return ErrorTimeout
		}

		runtime.Gosched()
		if l.sleep > 0 {
			time.Sleep(l.sleep)
		}
	}
}

func (l *SpLock) UnlockReached(target any) {
	l.version.Store(target)
	runtime.Gosched()
}
