package serializer

import (
	"encoding/binary"
	"errors"
	"io"
	"tinymq/core"
)

func SerializeOperation(buffer []byte, object core.Operation) ([]byte, error) {
	var err error = nil
	buffer, err = binary.Append(buffer, binary.LittleEndian, []byte(core.OperationSignature))
	if err != nil {
		return nil, err
	}

	buffer, err = binary.Append(buffer, binary.LittleEndian, object.Op)
	if err != nil {
		return nil, err
	}

	buffer, err = SerializeMessage(buffer, object.Messages[0])
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func DeserializeOperation(reader io.Reader) (core.Operation, error) {

	object := core.Operation{}
	var signature = []byte{0, 0, 0, 0}
	err := binary.Read(reader, binary.LittleEndian, &signature)
	if err != nil {
		return object, err
	}

	if core.OperationSignature != string(signature) {
		return object, errors.New("invalid binary data signature")
	}

	err = binary.Read(reader, binary.LittleEndian, &object.Op)
	if err != nil {
		return object, err
	}

	messages, err := DeserializeMessage(reader)
	if err != nil {
		return object, err
	}

	object.Messages = append(object.Messages, messages)

	return object, nil
}
