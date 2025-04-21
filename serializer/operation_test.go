package serializer

import (
	"bytes"
	"reflect"
	"testing"
	"tinymq/core"
)

func TestSerializeOperation(t *testing.T) {
	expected := []byte{116, 110, 109, 113, 130, 1, 0, 0, 0, 0, 233, 2, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 123, 34, 116, 101, 115, 116, 34, 58, 32, 34, 100, 97, 116, 97, 34, 125}
	actual, err := SerializeOperation(nil, core.Operation{
		Op: core.OpAck,
		Messages: []core.Message{
			{
				ContentType: core.TypeJson,
				Id:          745,
				Data:        "{\"test\": \"data\"}",
			},
		},
	})

	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(expected, actual) != 0 {
		t.Errorf("serialize failed, expected %v, actual %v", expected, actual)
	}
}

func TestDeserializeOperation(t *testing.T) {
	buffer := []byte{116, 110, 109, 113, 130, 1, 0, 0, 0, 0, 233, 2, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 123, 34, 116, 101, 115, 116, 34, 58, 32, 34, 100, 97, 116, 97, 34, 125}
	actual, err := DeserializeOperation(bytes.NewReader(buffer))

	if err != nil {
		t.Error(err)
	}

	expected := core.Operation{
		Op: core.OpAck,
		Messages: []core.Message{
			{
				ContentType: core.TypeJson,
				Id:          745,
				Data:        "{\"test\": \"data\"}",
			},
		},
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("serialize failed, expected %v, actual %v", expected, actual)
	}
}
