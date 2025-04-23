package config

import (
	"log"
	"tinymq/core"
)

type Settings struct {
	StoragePath *string
	MaxPartSize int64
	DB          core.DeduplicationStorage
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
