package serializer

import (
	"encoding/binary"
	"io"
	"tinymq/core"
)

func SerializeMessage(buffer []byte, object core.Message) ([]byte, error) {
	var err error = nil
	buffer, err = binary.Append(buffer, binary.LittleEndian, object.ContentType)
	if err != nil {
		return nil, err
	}

	buffer, err = binary.Append(buffer, binary.LittleEndian, object.Id)
	if err != nil {
		return nil, err
	}

	buffer, err = binary.Append(buffer, binary.LittleEndian, int32(len(object.Data)))
	if err != nil {
		return nil, err
	}

	buffer, err = binary.Append(buffer, binary.LittleEndian, []byte(object.Data))
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func SerializeMessages(buffer []byte, objects []core.Message) ([]byte, error) {
	var err error = nil
	buffer, err = binary.Append(buffer, binary.LittleEndian, int32(len(objects)))
	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		buffer, err = SerializeMessage(buffer, object)
		if err != nil {
			return nil, err
		}
	}

	return buffer, nil
}

func DeserializeMessage(reader io.Reader) (core.Message, error) {
	object := core.Message{}
	err := binary.Read(reader, binary.LittleEndian, &object.ContentType)
	if err != nil {
		return object, err
	}

	err = binary.Read(reader, binary.LittleEndian, &object.Id)
	if err != nil {
		return object, err
	}

	var dataSize int32 = 0
	err = binary.Read(reader, binary.LittleEndian, &dataSize)
	if err != nil {
		return object, err
	}

	data := make([]byte, dataSize)
	err = binary.Read(reader, binary.LittleEndian, data)
	if err != nil {
		return object, err
	}

	object.Data = string(data)

	return object, nil
}

func DeserializeMessages(reader io.Reader) ([]core.Message, error) {
	var messageCount int32 = 0
	err := binary.Read(reader, binary.LittleEndian, &messageCount)
	if err != nil {
		return []core.Message{}, err
	}

	objects := make([]core.Message, messageCount)

	for i, _ := range objects {
		object, err := DeserializeMessage(reader)
		if err != nil {
			return nil, err
		}

		objects[i] = object
	}

	return objects, nil
}
