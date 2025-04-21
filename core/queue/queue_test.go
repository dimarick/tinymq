package queue

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
	"tinymq/config"
	"tinymq/core"
)

func TestEnqueue(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/%d", time.Now().Unix())
	config.InitConfig(config.Settings{
		StoragePath: &path,
		MaxPartSize: 100,
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
		{
			ContentType: core.TypeText,
			Id:          5,
			Data:        "Message 5",
		},
	}
	q.Enqueue(&core.Operation{
		Op:       core.OpPublish,
		Target:   "exchange1",
		Messages: expected[0:3],
	})

	q.Enqueue(&core.Operation{
		Op:       core.OpPublish,
		Target:   "exchange1",
		Messages: expected[3:4],
	})

	q.Enqueue(&core.Operation{
		Op:       core.OpPublish,
		Target:   "exchange1",
		Messages: expected[4:],
	})

	actual := q.Consume(42, 2, 0)
	actual = append(actual, q.Consume(42, 1, 0)...)
	actual = append(actual, q.Consume(42, 1, 0)...)
	actual = append(actual, q.Consume(42, 2, 0)...)

	ids := make([]int64, 0)

	for _, m := range actual {
		ids = append(ids, m.Id)
	}

	q.Ack(42, ids)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("queue failed, expected %v, actual %v", expected, actual)
	}

	q2 := GetQueue("queue1")

	actual = q2.Consume(42, 100, 0)

	if len(actual) > 0 {
		t.Errorf("consume failed, expected %v, actual %v", 0, len(actual))
	}

	files, err := fs.Glob(os.DirFS(path), "*/*/*")

	if err != nil {
		t.Error(err)
	}

	expectedFiles := []string{
		"queue/queue1/consume.ref",
		"queue/queue1/publish.ref",
		"queue/queue1/queue.1",
		"queue/queue1/queue.1.status",
	}
	if !reflect.DeepEqual(files, expectedFiles) {
		t.Errorf("cleanup failed, expected %v, actual %v", expectedFiles, files)
	}
}

func TestEnqueueLarge(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/%d", time.Now().Unix())
	config.InitConfig(config.Settings{
		StoragePath: &path,
		MaxPartSize: 100000,
	})

	q := GetQueue("queue1")

	threads := 100000
	ops := 100
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

	actual := q.Consume(42, 200000000, 0)

	ids := make([]int64, 0)

	for _, m := range actual {
		ids = append(ids, m.Id)
	}

	q.Ack(42, ids)

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
