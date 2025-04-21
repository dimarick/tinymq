package serializer

import (
	"encoding/binary"
	"io"
	"tinymq/core"
)

func SerializeMessageStatus(buffer []byte, messageId int64, object core.MessageStatus) ([]byte, error) {
	var err error = nil
	buffer, err = binary.Append(buffer, binary.LittleEndian, messageId)
	if err != nil {
		return nil, err
	}

	buffer, err = binary.Append(buffer, binary.LittleEndian, object.Ptr)
	if err != nil {
		return nil, err
	}

	buffer, err = binary.Append(buffer, binary.LittleEndian, object.Size)
	if err != nil {
		return nil, err
	}

	buffer, err = binary.Append(buffer, binary.LittleEndian, object.ConsumerId)
	if err != nil {
		return nil, err
	}

	buffer, err = binary.Append(buffer, binary.LittleEndian, object.Status)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func SerializeMessageStatuses(buffer []byte, objects map[int64]core.MessageStatus) ([]byte, error) {
	var err error = nil
	buffer, err = binary.Append(buffer, binary.LittleEndian, int32(len(objects)))
	if err != nil {
		return nil, err
	}

	for id, object := range objects {
		buffer, err = SerializeMessageStatus(buffer, id, object)
		if err != nil {
			return nil, err
		}
	}

	return buffer, nil
}

func DeserializeMessageStatus(reader io.Reader) (int64, core.MessageStatus, error) {
	object := core.MessageStatus{}
	messageId := int64(0)

	err := binary.Read(reader, binary.LittleEndian, &messageId)
	if err != nil {
		return messageId, object, err
	}

	err = binary.Read(reader, binary.LittleEndian, &object.Ptr)
	if err != nil {
		return messageId, object, err
	}

	err = binary.Read(reader, binary.LittleEndian, &object.Size)
	if err != nil {
		return messageId, object, err
	}

	err = binary.Read(reader, binary.LittleEndian, &object.ConsumerId)
	if err != nil {
		return messageId, object, err
	}

	err = binary.Read(reader, binary.LittleEndian, &object.Status)
	if err != nil {
		return messageId, object, err
	}

	return messageId, object, nil
}

func DeserializeMessageStatuses(reader io.Reader) (map[int64]core.MessageStatus, error) {
	var messageCount int32 = 0
	err := binary.Read(reader, binary.LittleEndian, &messageCount)
	if err != nil {
		return map[int64]core.MessageStatus{}, err
	}

	objects := make(map[int64]core.MessageStatus, 0)

	for i := int32(0); i < messageCount; i++ {
		id, object, err := DeserializeMessageStatus(reader)
		if err != nil {
			return nil, err
		}

		objects[id] = object
	}

	return objects, nil
}
