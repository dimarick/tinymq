package serializer

import (
	"bytes"
	"reflect"
	"testing"
	"tinymq/core"
)

func TestSerializeRef(t *testing.T) {
	expected := []byte{0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x1}
	actual, err := SerializeRef(nil, core.Ref{0x123456789ABCDEF})

	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(expected, actual) != 0 {
		t.Errorf("serialize failed, expected %v, actual %v", expected, actual)
	}
}

func TestDeserializeRef(t *testing.T) {
	buffer := []byte{0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x1}
	actual, err := DeserializeRef(bytes.NewReader(buffer))

	if err != nil {
		t.Error(err)
	}

	expected := core.Ref{0x123456789ABCDEF}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("serialize failed, expected %v, actual %v", expected, actual)
	}
}
