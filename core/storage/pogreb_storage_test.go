package storage

import (
	"fmt"
	"github.com/akrylysov/pogreb"
	"reflect"
	"testing"
	"time"
	"tinymq/config"
	"tinymq/core"
)

func TestFilterMessages(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/1%d", time.Now().Unix())

	db, err := pogreb.Open(fmt.Sprintf("%s/db", path), nil)

	if err != nil {
		t.Error(err)
	}

	config.InitConfig(config.Settings{
		StoragePath:              path,
		MaxPartSize:              100,
		StorageMaxItems:          10000000,
		StatCollectorInterval:    10 * time.Hour,
		GarbageCollectorInterval: 10 * time.Hour,
	})

	s := NewMessageStorage(db)
	defer func() {
		_ = s.Close()
	}()

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

	actual, err := s.FilterMessages(expected)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("FilterMessages failed, expected %v, actual %v", expected, actual)
	}

	err = s.SetMessages(expected[0:2])

	actual, _ = s.FilterMessages(expected)

	if !reflect.DeepEqual(actual, expected[2:]) {
		t.Errorf("SetMessages failed, expected %v, actual %v", expected, actual)
	}
}

func TestGC(t *testing.T) {
	path := fmt.Sprintf("/tmp/queue_test/1%d", time.Now().Unix())

	db, err := pogreb.Open(fmt.Sprintf("%s/db", path), nil)

	if err != nil {
		t.Error(err)
	}

	config.InitConfig(config.Settings{
		StoragePath:              path,
		MaxPartSize:              100,
		StorageMaxItems:          3,
		StatCollectorInterval:    10 * time.Hour,
		GarbageCollectorInterval: 10 * time.Hour,
	})

	s := NewMessageStorage(db)
	defer func() {
		_ = s.Close()
	}()
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

	err = s.SetMessages(expected[0:1])
	if err != nil {
		t.Error(err)
	}

	time.Sleep(100 * time.Millisecond)

	err = s.SetMessages(expected[1:2])
	if err != nil {
		t.Error(err)
	}

	time.Sleep(100 * time.Millisecond)
	err = s.SetMessages(expected[2:3])
	if err != nil {
		t.Error(err)
	}
	time.Sleep(100 * time.Millisecond)
	err = s.SetMessages(expected[3:4])
	if err != nil {
		t.Error(err)
	}
	time.Sleep(100 * time.Millisecond)
	err = s.SetMessages(expected[4:5])
	if err != nil {
		t.Error(err)
	}
	err = s.CollectStat()
	if err != nil {
		t.Error(err)
	}

	err = s.GC(2)
	if err != nil {
		t.Error(err)
	}

	actual, _ := s.FilterMessages(expected)

	if len(actual) != 3 {
		t.Errorf("GC failed, expected %v, actual %v", 3, len(actual))
	}
}
