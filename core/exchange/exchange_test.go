package exchange

import (
	"fmt"
	"reflect"
	"testing"
	"time"
	"tinymq/config"
	"tinymq/core"
	"tinymq/core/queue"
)

func TestPublish(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/1%d", time.Now().Unix())
	config.InitConfig(config.Settings{
		StoragePath: &path,
		MaxPartSize: 100,
	})

	e := GetExchange("e1")
	defer e.Close()

	e.Bind("q1")
	e.Bind("q2")

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
	e.Publish(&core.Operation{
		Op:       core.OpPublish,
		Target:   "e1",
		Messages: expected,
	})
	//e.Publish(&core.Operation{
	//	Op:       core.OpPublish,
	//	Target:   "exchange1",
	//	Messages: expected[0:3],
	//})
	//
	//e.Publish(&core.Operation{
	//	Op:       core.OpPublish,
	//	Target:   "exchange1",
	//	Messages: expected[3:4],
	//})
	//
	//e.Publish(&core.Operation{
	//	Op:       core.OpPublish,
	//	Target:   "exchange1",
	//	Messages: expected[4:],
	//})

	for _, q := range []*queue.QueueDescriptor{
		queue.GetQueue("q1"),
		queue.GetQueue("q2"),
	} {

		actual := q.Consume(42, 6, 0)
		actual = append(actual, q.Consume(42, 6, 1*time.Second)...)

		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("queue failed, expected %v, actual %v", expected, actual)
		}
	}

	q2 := queue.GetQueue("q3")
	actual := q2.Consume(42, 6, 0)

	if len(actual) != 0 {
		t.Errorf("queue failed, expected %v, actual %v", 0, len(actual))
	}
}
