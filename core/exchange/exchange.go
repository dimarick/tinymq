package exchange

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
	"tinymq/config"
	"tinymq/core"
	"tinymq/core/queue"
)

type ExchangeDescriptor struct {
	name                 string
	refCount             int
	exchangeQueue        *queue.QueueDescriptor
	bindQueues           map[string]*queue.QueueDescriptor
	deduplicationStorage config.MessageStorageInterface
}

var exchanges = make(map[string]*ExchangeDescriptor)

var bindingsMutex sync.Mutex
var bindings = make(map[string][]string)

func HasBindings(exchangeName string) bool {
	_, ok := bindings[exchangeName]
	return ok
}

func GetExchange(name string) *ExchangeDescriptor {
	if _, ok := exchanges[name]; !ok {
		exchange := initExchange(name)
		exchanges[name] = exchange

		return exchange
	}

	exchange, _ := exchanges[name]
	exchange.refCount++

	return exchange
}

func (exchange *ExchangeDescriptor) Close() {
	exchange.refCount--

	if exchange.refCount == 0 {
		exchange.exchangeQueue.Close()
		exchange.exchangeQueue = nil
		for _, q := range exchange.bindQueues {
			q.Close()
		}
	}
}

func (exchange *ExchangeDescriptor) Publish(op *core.Operation) {
	exchange.exchangeQueue.Enqueue(op)
}

func (exchange *ExchangeDescriptor) Bind(qname string) {
	exchange.bindQueues[qname] = queue.GetQueue(qname)
}

func initExchange(name string) *ExchangeDescriptor {
	result := new(ExchangeDescriptor)

	*result = ExchangeDescriptor{
		name:                 name,
		refCount:             1,
		exchangeQueue:        queue.GetQueue(fmt.Sprintf("exchange.%s.queue", name)),
		bindQueues:           make(map[string]*queue.QueueDescriptor),
		deduplicationStorage: config.GetConfig().DB,
	}

	exchangeBind, ok := bindings[name]

	if ok {
		for _, qname := range exchangeBind {
			result.bindQueues[qname] = queue.GetQueue(qname)
		}
	}

	go result.consumePublishes()

	return result
}

func (exchange *ExchangeDescriptor) consumePublishes() {
	consumerId := rand.Int63()
	for {
		exchangeQueue := exchange.exchangeQueue

		if exchangeQueue == nil {
			break
		}

		messages := exchangeQueue.Consume(consumerId, 1000, 1*time.Second)

		filteredMessages, err := exchange.deduplicationStorage.FilterMessages(messages)

		if err != nil {
			log.Panic(err)
		}

		if len(filteredMessages) == 0 {
			continue
		}

		wait := make(map[string]int64)

		for qname, q := range exchange.bindQueues {
			log.Printf("Publish %d from %s to %s", len(filteredMessages), exchange.name, qname)
			wait[qname] = q.EnqueueAsync(&core.Operation{
				Op:       core.OpPublish,
				Target:   exchange.name,
				Messages: filteredMessages,
			})
		}

		for qname, target := range wait {
			q, ok := exchange.bindQueues[qname]

			if !ok {
				continue
			}

			q.EnqueueWait(target)
			log.Printf("Published %d from %s to %s", len(filteredMessages), exchange.name, qname)
		}

		ids := make([]int64, 0)

		err = exchange.deduplicationStorage.SetMessages(filteredMessages)
		if err != nil {
			log.Panic(err)
		}

		err = exchange.deduplicationStorage.Sync()
		if err != nil {
			log.Panic(err)
		}

		exchangeQueue.Ack(consumerId, ids)
	}
}

func Bind(exchange string, queue string) {
	bindingsMutex.Lock()
	defer bindingsMutex.Unlock()
	if _, ok := bindings[exchange]; !ok {
		bindings[exchange] = make([]string, 0)
	}
	bindings[exchange] = append(bindings[exchange], queue)
}

func HasQueue(name string) bool {
	for _, queues := range bindings {
		for _, q := range queues {
			if q == name {
				return true
			}
		}
	}

	return false
}
