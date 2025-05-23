package serializer

import (
	"bytes"
	"reflect"
	"testing"
	"tinymq/core"
)

func TestSerializeMessage(t *testing.T) {
	expected := []byte{0, 233, 2, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 123, 34, 116, 101, 115, 116, 34, 58, 32, 34, 100, 97, 116, 97, 34, 125}
	actual, err := SerializeMessage(nil, core.Message{
		ContentType: core.TypeJson,
		Id:          745,
		Data:        "{\"test\": \"data\"}",
	})

	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(expected, actual) != 0 {
		t.Errorf("serialize failed, expected %v, actual %v", expected, actual)
	}
}

func TestSerializeMessages(t *testing.T) {
	expected := []byte{1, 0, 0, 0, 0, 83, 153, 69, 0, 0, 0, 0, 0, 16, 0, 0, 0, 123, 34, 116, 101, 115, 116, 34, 58, 32, 34, 100, 97, 116, 97, 34, 125}
	actual, err := SerializeMessages(nil, []core.Message{
		{
			ContentType: core.TypeJson,
			Id:          4561235,
			Data:        "{\"test\": \"data\"}",
		},
	})

	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(expected, actual) != 0 {
		t.Errorf("serialize failed, expected %v, actual %v", expected, actual)
	}
}

func TestSerializeMessages2(t *testing.T) {
	expected := []byte{3, 0, 0, 0, 0, 233, 2, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 123, 34, 116, 101, 115, 116, 34, 58, 32, 34, 100, 97, 116, 97, 34, 125, 2, 141, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 142, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 84, 101, 120, 116}
	actual, err := SerializeMessages(nil, []core.Message{
		{
			ContentType: core.TypeJson,
			Id:          745,
			Data:        "{\"test\": \"data\"}",
		},
		{
			ContentType: core.TypeBinary,
			Id:          141,
			Data:        string([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
		},
		{
			ContentType: core.TypeText,
			Id:          142,
			Data:        "Text",
		},
	})

	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(expected, actual) != 0 {
		t.Errorf("serialize failed, expected %v, actual %v", expected, actual)
	}
}

func TestDeserializeMessage(t *testing.T) {
	buffer := []byte{0, 233, 2, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 123, 34, 116, 101, 115, 116, 34, 58, 32, 34, 100, 97, 116, 97, 34, 125}
	actual, err := DeserializeMessage(bytes.NewReader(buffer))

	if err != nil {
		t.Error(err)
	}

	expected := core.Message{
		ContentType: core.TypeJson,
		Id:          745,
		Data:        "{\"test\": \"data\"}",
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("serialize failed, expected %v, actual %v", expected, actual)
	}
}

func TestDeserializeMessages(t *testing.T) {
	buffer := []byte{1, 0, 0, 0, 0, 83, 153, 69, 0, 0, 0, 0, 0, 16, 0, 0, 0, 123, 34, 116, 101, 115, 116, 34, 58, 32, 34, 100, 97, 116, 97, 34, 125}
	actual, err := DeserializeMessages(bytes.NewReader(buffer))

	if err != nil {
		t.Error(err)
	}

	expected := []core.Message{
		{
			ContentType: core.TypeJson,
			Id:          4561235,
			Data:        "{\"test\": \"data\"}",
		},
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("serialize failed, expected %v, actual %v", expected, actual)
	}
}

func TestDeserializeMessages2(t *testing.T) {
	buffer := []byte{3, 0, 0, 0, 0, 233, 2, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 123, 34, 116, 101, 115, 116, 34, 58, 32, 34, 100, 97, 116, 97, 34, 125, 2, 141, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 142, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 84, 101, 120, 116}
	actual, err := DeserializeMessages(bytes.NewReader(buffer))

	if err != nil {
		t.Error(err)
	}

	expected := []core.Message{
		{
			ContentType: core.TypeJson,
			Id:          745,
			Data:        "{\"test\": \"data\"}",
		},
		{
			ContentType: core.TypeBinary,
			Id:          141,
			Data:        string([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
		},
		{
			ContentType: core.TypeText,
			Id:          142,
			Data:        "Text",
		},
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("serialize failed, expected %v, actual %v", expected, actual)
	}
}
