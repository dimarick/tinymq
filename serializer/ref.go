package serializer

import (
	"bytes"
	"encoding/binary"
	"tinymq/core"
)

func SerializeRef(buffer []byte, object core.Ref) ([]byte, error) {
	var err error = nil
	buffer, err = binary.Append(buffer, binary.LittleEndian, object.Id)
	if err != nil {
		return nil, err
	}
	buffer, err = binary.Append(buffer, binary.LittleEndian, object.Ptr)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func DeserializeRef(reader *bytes.Reader) (core.Ref, error) {
	object := core.Ref{}
	err := binary.Read(reader, binary.LittleEndian, &object.Id)
	if err != nil {
		return object, err
	}
	err = binary.Read(reader, binary.LittleEndian, &object.Ptr)
	if err != nil {
		return object, err
	}

	return object, nil
}
