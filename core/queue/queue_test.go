package queue

import (
	"fmt"
	"log"
	"reflect"
	"sync"
	"testing"
	"time"
	"tinymq/config"
	"tinymq/core"
)

func TestEnqueue(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test%d", time.Now().Unix())
	config.InitConfig(config.Settings{
		StoragePath: &path,
		MaxPartSize: 47,
	})

	q := GetQueue("queue1")

	expected := []core.Message{
		{
			ContentType: core.TypeText,
			Id:          1,
			Data:        "Message 1",
		},
		{
			ContentType: core.TypeText,
			Id:          2,
			Data:        "Message 2",
		},
		{
			ContentType: core.TypeText,
			Id:          3,
			Data:        "Message 3",
		},
		{
			ContentType: core.TypeText,
			Id:          4,
			Data:        "Message 4",
		},
	}
	q.Enqueue(&core.Operation{
		Op:       core.OpPublish,
		Target:   "exchange1",
		Messages: expected,
	})

	actual := q.Consume(42, 2, 1*time.Second)
	actual = append(actual, q.Consume(42, 1, 1*time.Second)...)
	actual = append(actual, q.Consume(42, 1, 1*time.Second)...)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("queue failed, expected %v, actual %v", expected, actual)
	}
}

func TestEnqueueLarge(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/%d", time.Now().Unix())
	config.InitConfig(config.Settings{
		StoragePath: &path,
		MaxPartSize: 100000,
	})

	q := GetQueue("queue1")

	threads := 1000
	ops := 10
	messages := 1

	var wg sync.WaitGroup
	wg.Add(threads)

	for i := 0; i < threads; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				expectedOp := make([]core.Message, 0)
				for k := 0; k < messages; k++ {
					expectedOp = append(expectedOp, core.Message{
						ContentType: core.TypeText,
						Id:          int64(i*threads*ops + j*ops + k),
						Data:        fmt.Sprintf("Message %d %d %d", i, j, k),
					})
				}

				q.Enqueue(&core.Operation{
					Op:       core.OpPublish,
					Target:   "exchange1",
					Messages: expectedOp,
				})
			}
		}()
	}
	wg.Wait()

	log.Printf("Consuming %d messages", threads*messages*ops)

	actual := q.Consume(42, 200000000, 1*time.Second)

	if len(actual) != threads*messages*ops {
		t.Errorf("queue failed, expected %v, actual %v", threads*messages*ops, len(actual))
	}
}

func TestConsumeThreads(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/%d", time.Now().Unix())
	config.InitConfig(config.Settings{
		StoragePath: &path,
		MaxPartSize: 10000,
	})

	q := GetQueue("queue1")

	threads := 150
	ops := 50
	messages := 1

	var wg sync.WaitGroup
	wg.Add(threads)

	for i := 0; i < threads; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				expectedOp := make([]core.Message, 0)
				for k := 0; k < messages; k++ {
					expectedOp = append(expectedOp, core.Message{
						ContentType: core.TypeText,
						Id:          int64(i*threads*ops + j*ops + k),
						Data:        fmt.Sprintf("Message %d %d %d", i, j, k),
					})
				}

				q.Enqueue(&core.Operation{
					Op:       core.OpPublish,
					Target:   "exchange1",
					Messages: expectedOp,
				})
			}
		}()
	}
	wg.Wait()

	log.Printf("Consuming %d messages", threads*messages*ops)

	var mutex sync.Mutex
	actual := make([]core.Message, 0)

	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func() {
			consumed := q.Consume(int64(i), 50, 0)

			ids := make([]int64, 0)

			for _, m := range consumed {
				ids = append(ids, m.Id)
			}

			q.Ack(int64(i), ids)

			mutex.Lock()
			actual = append(actual, consumed...)
			mutex.Unlock()

			wg.Done()
		}()
	}

	wg.Wait()

	if len(actual) != threads*messages*ops {
		t.Errorf("queue failed, expected %v, actual %v", threads*messages*ops, len(actual))
	}
}
