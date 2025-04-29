package config

import (
	"log"
	"time"
	"tinymq/core"
)

type Settings struct {
	StoragePath              string
	MaxPartSize              int64
	DB                       MessageStorageInterface
	StorageMaxItems          int64
	StatCollectorInterval    time.Duration
	GarbageCollectorInterval time.Duration
}

type ItemsIterator interface{}

type MessageStorageInterface interface {
	FilterMessages(messages []core.Message) ([]core.Message, error)
	SetMessages(messages []core.Message) error
	Sync() error
	Close() error
	Items() ItemsIterator
	Next(items ItemsIterator) ([]byte, []byte, error)
	Delete(key []byte) error
	StartBackgroundWorkers()
	StopBackgroundWorkers()
}

var globalConfig Settings
var globalConfigInit bool

func InitConfig(config Settings) {
	if globalConfigInit == true {
		log.Panic("Failed to setup configuration twice")

		return
	}

	globalConfig = config
}

func GetConfig() Settings {
	return globalConfig
}
