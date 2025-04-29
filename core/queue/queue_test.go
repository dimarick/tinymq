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
	path := fmt.Sprintf("/tmp/queue_test/1%d", time.Now().UnixNano())
	config.InitConfig(config.Settings{
		StoragePath: path,
		MaxPartSize: 100,
	})

	q := GetQueue("queue1")
	defer q.Close()

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

func TestEnqueueWait(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/2%d", time.Now().UnixNano())
	config.InitConfig(config.Settings{
		StoragePath: path,
		MaxPartSize: 100,
	})

	q := GetQueue("queue2")
	defer q.Close()

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

	var actual []core.Message
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		defer wg.Done()
		actual = q.Consume(42, 6, 2*time.Second)
	}()

	q.Enqueue(&core.Operation{
		Op:       core.OpPublish,
		Target:   "exchange1",
		Messages: expected,
	})

	wg.Wait()

	if len(actual) == 0 {
		t.Errorf("queue failed, expected %v, actual %v", 0, len(actual))
	}

	actual = append(actual, q.Consume(42, 6, 200*time.Millisecond)...)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("queue failed, expected %v, actual %v", expected, actual)
	}
}

func TestReject(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/3%d", time.Now().UnixNano())
	config.InitConfig(config.Settings{
		StoragePath: path,
		MaxPartSize: 100,
	})

	q := GetQueue("queue2")
	defer q.Close()

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
		Messages: expected,
	})

	actual := q.Consume(42, 2, 0)
	actual = append(actual, q.Consume(42, 1, 0)...)
	actual = append(actual, q.Consume(42, 1, 0)...)
	actual = append(actual, q.Consume(42, 5, 0)...)

	ids := make([]int64, 0)

	for _, m := range actual[0:2] {
		ids = append(ids, m.Id)
	}

	q.Reject(42, ids)

	ids = make([]int64, 0)

	for _, m := range actual[2:] {
		ids = append(ids, m.Id)
	}

	q.Ack(42, ids)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("queue failed, expected %v, actual %v", expected, actual)
	}

	actual = q.Consume(42, 100, 0)

	if len(actual) != 0 {
		t.Errorf("consume failed, expected %v, actual %v", 0, len(actual))
	}
}

func TestRequeue(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/6%d", time.Now().UnixNano())
	config.InitConfig(config.Settings{
		StoragePath: path,
		MaxPartSize: 100,
	})

	q := GetQueue("queue4")
	defer q.Close()

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
		Messages: expected,
	})

	actual := q.Consume(42, 2, 0)
	actual = append(actual, q.Consume(42, 1, 0)...)
	actual = append(actual, q.Consume(42, 1, 0)...)
	actual = append(actual, q.Consume(42, 5, 0)...)

	ids := make([]int64, 0)

	for _, m := range actual[0:2] {
		ids = append(ids, m.Id)
	}

	q.Requeue(42, ids)

	ids = make([]int64, 0)

	for _, m := range actual[2:] {
		ids = append(ids, m.Id)
	}

	q.Ack(42, ids)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("queue failed, expected %v, actual %v", expected, actual)
	}

	actual = q.Consume(42, 100, 0)

	if len(actual) != 2 {
		t.Errorf("consume failed, expected %v, actual %v", 2, len(actual))
	}
}

func TestDetachConsumer(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/7%d", time.Now().UnixNano())
	config.InitConfig(config.Settings{
		StoragePath: path,
		MaxPartSize: 100,
	})

	q := GetQueue("queue5")
	defer q.Close()

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
		Messages: expected,
	})

	actual := q.Consume(42, 2, 0)
	actual2 := q.Consume(43, 5, 0)

	if !reflect.DeepEqual(actual, expected[0:2]) {
		t.Errorf("queue failed, expected %v, actual %v", expected, actual)
	}

	if !reflect.DeepEqual(actual2, expected[2:]) {
		t.Errorf("queue failed, expected %v, actual %v", expected, actual2)
	}

	wg := sync.WaitGroup{}
	var actual3 []core.Message
	go func() {
		wg.Add(1)
		defer wg.Done()
		actual3 = q.Consume(42, 100, 1*time.Second)
	}()

	q.DetachConsumer(42)

	wg.Wait()

	if !reflect.DeepEqual(actual3, expected[0:2]) {
		t.Errorf("queue failed, expected %v, actual %v", expected, actual3)
	}
}

func TestEnqueueLarge(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/7%d", time.Now().UnixNano())
	config.InitConfig(config.Settings{
		StoragePath: path,
		MaxPartSize: 100000,
	})

	q := GetQueue("queue3")
	defer q.Close()

	threads := 100000
	ops := 1
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
	path := fmt.Sprintf("/tmp/queue_test/8%d", time.Now().UnixNano())
	config.InitConfig(config.Settings{
		StoragePath: path,
		MaxPartSize: 10000,
	})

	q := GetQueue("queue5")
	defer q.Close()

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

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			consumed := q.Consume(int64(i), 10, 0)

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
