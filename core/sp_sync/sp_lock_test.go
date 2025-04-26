package sp_sync

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIntCompare(t *testing.T) {
	fn := IntCompare(int32(1000))
	fn64 := IntCompare(10000000000000)

	actual := []bool{
		fn(999),
		fn(1000),
		fn(1001),
		fn(0),
		fn(-1),
		fn64(10000000000000 - 1),
		fn64(10000000000000),
		fn64(10000000000000 + 1),
		fn64(0),
		fn64(-1),
	}

	expected := []bool{
		false,
		true,
		true,
		false,
		false,
		false,
		true,
		true,
		false,
		false,
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("IntCompare failed, expected %v, actual %v", expected, actual)
	}
}

func TestLockUntil(t *testing.T) {
	lock := NewSpLock(0)
	waiters := atomic.Int32{}
	timeouts := atomic.Int32{}
	wg := sync.WaitGroup{}
	wg.Add(3 + 5 + 7)

	for i := 0; i < 3; i++ {
		go func() {
			waiters.Add(1)
			defer waiters.Add(-1)
			wg.Done()
			if lock.LockUntilInt(42, time.Second) != nil {
				timeouts.Add(1)
			}
		}()
	}

	for i := 0; i < 5; i++ {
		go func() {
			waiters.Add(1)
			defer waiters.Add(-1)
			wg.Done()
			if lock.LockUntilInt(43, time.Second) != nil {
				timeouts.Add(1)
			}
		}()
	}

	for i := 0; i < 7; i++ {
		go func() {
			waiters.Add(1)
			defer waiters.Add(-1)
			wg.Done()
			if lock.LockUntilInt(44, 100*time.Millisecond) != nil {
				timeouts.Add(1)
			}
		}()
	}

	wg.Wait()

	if waiters.Load() != 3+5+7 {
		t.Errorf("TestLockUntil case 1 failed, expected %v, actual %v", 3+5+7, waiters.Load())
	}

	lock.UnlockReached(41)

	if waiters.Load() != 3+5+7 {
		t.Errorf("TestLockUntil case 2 failed, expected %v, actual %v", 3+5+7, waiters.Load())
	}

	lock.UnlockReached(42)

	if waiters.Load() != 5+7 {
		t.Errorf("TestLockUntil case 3 failed, expected %v, actual %v", 5+7, waiters.Load())
	}

	lock.UnlockReached(43)

	if waiters.Load() != 7 {
		t.Errorf("TestLockUntil case 3 failed, expected %v, actual %v", 7, waiters.Load())
	}

	time.Sleep(200 * time.Millisecond)

	if waiters.Load() != 0 {
		t.Errorf("TestLockUntil case 4 failed, expected %v, actual %v", 0, waiters.Load())
	}

	if timeouts.Load() != 7 {
		t.Errorf("TestLockUntil case 5 failed, expected %v, actual %v", 7, timeouts.Load())
	}
}
