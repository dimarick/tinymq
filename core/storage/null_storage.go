package storage

import (
	"errors"
	"tinymq/config"
	"tinymq/core"
)

type NullMessageStorage struct {
	config.MessageStorageInterface
}

func NewNullMessageStorage() *NullMessageStorage {
	return new(NullMessageStorage)
}

func (storage *NullMessageStorage) FilterMessages(messages []core.Message) ([]core.Message, error) {
	return messages, nil
}

func (storage *NullMessageStorage) SetMessages(_ []core.Message) error {
	return nil
}

func (storage *NullMessageStorage) Sync() error {
	return nil
}

func (storage *NullMessageStorage) Close() error {
	return nil
}

func (storage *NullMessageStorage) Items() config.ItemsIterator {
	return make([]int64, 0)
}

func (storage *NullMessageStorage) Next(config.ItemsIterator) ([]byte, []byte, error) {
	return nil, nil, errors.New("not implemented")
}
