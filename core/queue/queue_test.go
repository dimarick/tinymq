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
		MaxPartSize: 1000,
	})

	q := GetQueue("queue1")

	expected := []core.Message{
		{
			ContentType: core.TypeText,
			Id:          "1",
			Data:        "Message 1",
		},
		{
			ContentType: core.TypeText,
			Id:          "2",
			Data:        "Message 2",
		},
		{
			ContentType: core.TypeText,
			Id:          "3",
			Data:        "Message 3",
		},
		{
			ContentType: core.TypeText,
			Id:          "4",
			Data:        "Message 4",
		},
	}
	q.Enqueue(&core.Operation{
		Op:       core.OpPublish,
		Target:   "exchange1",
		Messages: expected,
	})

	actual := q.Consume(2, 0)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("queue failed, expected %v, actual %v", expected, actual)
	}
}

func TestEnqueueLarge(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/%d", time.Now().Unix())
	config.InitConfig(config.Settings{
		StoragePath: &path,
		MaxPartSize: 1000000,
	})

	q := GetQueue("queue1")

	threads := 10000
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
						Id:          fmt.Sprintf("%d-%d-%d", i, j, k),
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

	actual := q.Consume(200000000, 0)

	if len(actual) != threads*messages*ops {
		t.Errorf("queue failed, expected %v, actual %v", threads*messages*ops, len(actual))
	}
}
