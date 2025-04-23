package core

import (
	"encoding/binary"
	"io"
	"sync"
)

type DeduplicationStorage interface {
	io.Closer
	Get(key []byte) ([]byte, error)
	Has(key []byte) (bool, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Sync() error
}

type NullStorage struct {
	DeduplicationStorage
}

func NewNullStorage() *NullStorage {
	return new(NullStorage)
}

func (storage *NullStorage) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (storage *NullStorage) Put(key []byte, value []byte) error {
	return nil
}

func (storage *NullStorage) Has(key []byte) (bool, error) {
	return false, nil
}

func (storage *NullStorage) Delete(key []byte) error {
	return nil
}

func (storage *NullStorage) Sync() error {
	return nil
}

func (storage *NullStorage) Close() error {
	return nil
}

type MessageStorage struct {
	mutex sync.Mutex
	db    DeduplicationStorage
}

func NewMessageStorage(db DeduplicationStorage) *MessageStorage {
	storage := new(MessageStorage)
	storage.db = db

	return storage
}

func (storage *MessageStorage) SetMessages(messages []Message) error {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	for _, message := range messages {
		err := storage.set(message.Id)

		if err != nil {
			return err
		}
	}

	return nil
}

func (storage *MessageStorage) FilterMessages(messages []Message) ([]Message, error) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	result := make([]Message, 0)
	for _, message := range messages {
		has, err := storage.has(message.Id)

		if err != nil {
			return nil, err
		}

		if !has {
			result = append(result, message)
		}
	}

	return result, nil
}

func (storage *MessageStorage) set(key int64) error {
	bytes, err := binary.Append(nil, binary.LittleEndian, key)

	if err != nil {
		return err
	}

	return storage.db.Put(bytes, []byte{1})
}

func (storage *MessageStorage) has(key int64) (bool, error) {
	bytes, err := binary.Append(nil, binary.LittleEndian, key)

	if err != nil {
		return false, err
	}

	return storage.db.Has(bytes)
}

func (storage *MessageStorage) delete(key int64) error {
	bytes, err := binary.Append(nil, binary.LittleEndian, key)

	if err != nil {
		return err
	}

	return storage.db.Delete(bytes)
}

func (storage *MessageStorage) Sync() error {
	return storage.db.Sync()
}

func (storage *MessageStorage) Close() error {
	return storage.db.Close()
}
