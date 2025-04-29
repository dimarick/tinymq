package storage

import (
	"encoding/binary"
	"github.com/akrylysov/pogreb"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"tinymq/config"
	"tinymq/core"
)

type PogrebStorage struct {
	config.MessageStorageInterface
	mutex                          sync.Mutex
	db                             *pogreb.DB
	stat                           *storageStat
	statCollectorTransactionsCount uint64
	backgroundWorkers              sync.WaitGroup
	stopped                        atomic.Bool
}

func NewMessageStorage(db *pogreb.DB) *PogrebStorage {
	storage := new(PogrebStorage)
	storage.db = db
	storage.stat = new(storageStat)

	return storage
}

func (storage *PogrebStorage) CollectStat() error {
	newStat, err := storage.stat.collectStat(storage)

	if err != nil {
		return err
	}

	storage.stat = newStat

	return nil
}

func (storage *PogrebStorage) SetMessages(messages []core.Message) error {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	messages, err := storage.filterMessages(messages)
	if err != nil {
		return err
	}

	storage.stat.incrementStat(time.Now().UnixNano(), len(messages))

	for _, message := range messages {
		err := storage.set(message.Id)

		if err != nil {
			return err
		}
	}

	return nil
}

func (storage *PogrebStorage) FilterMessages(messages []core.Message) ([]core.Message, error) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.filterMessages(messages)
}
func (storage *PogrebStorage) Items() config.ItemsIterator {
	return storage.db.Items()
}
func (storage *PogrebStorage) Next(items config.ItemsIterator) ([]byte, []byte, error) {
	return items.(*pogreb.ItemIterator).Next()
}
func (storage *PogrebStorage) Delete(key []byte) error {
	return storage.db.Delete(key)
}
func (storage *PogrebStorage) filterMessages(messages []core.Message) ([]core.Message, error) {
	result := make([]core.Message, 0)
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

func (storage *PogrebStorage) set(key int64) error {
	bytesKey, err := binary.Append(nil, binary.LittleEndian, key)

	if err != nil {
		return err
	}

	value, err := binary.Append(nil, binary.LittleEndian, time.Now().UnixNano())

	if err != nil {
		return err
	}

	return storage.db.Put(bytesKey, value)
}

func (storage *PogrebStorage) has(key int64) (bool, error) {
	bytesKey, err := binary.Append(nil, binary.LittleEndian, key)

	if err != nil {
		return false, err
	}

	return storage.db.Has(bytesKey)
}

func (storage *PogrebStorage) delete(key int64) error {
	bytesKey, err := binary.Append(nil, binary.LittleEndian, key)

	if err != nil {
		return err
	}

	return storage.db.Delete(bytesKey)
}

func (storage *PogrebStorage) Sync() error {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.db.Sync()
}

func (storage *PogrebStorage) Close() error {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.db.Close()
}

func (storage *PogrebStorage) StartBackgroundWorkers() {

	go func() {
		storage.backgroundWorkers.Add(1)
		defer storage.backgroundWorkers.Done()
		for {
			if storage.stopped.Load() {
				break
			}
			time.Sleep(config.GetConfig().StatCollectorInterval)
			err := storage.CollectStat()
			runtime.Gosched()

			if err != nil {
				log.Panic(err)
			}
		}
	}()

	go func() {
		storage.backgroundWorkers.Add(1)
		defer storage.backgroundWorkers.Done()
		for {
			if storage.stopped.Load() {
				break
			}
			time.Sleep(config.GetConfig().GarbageCollectorInterval)
			err := storage.GC(config.GetConfig().StorageMaxItems)
			runtime.Gosched()
			if err != nil {
				log.Panic(err)
			}
		}
	}()

}
func (storage *PogrebStorage) StopBackgroundWorkers() {
	storage.stopped.Store(true)
	storage.backgroundWorkers.Wait()
}

func (storage *PogrebStorage) GC(items int64) error {
	return storage.stat.GC(storage, items)
}
