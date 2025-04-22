package exchange

import (
	"fmt"
	"math/rand"
	"time"
	"tinymq/core"
	"tinymq/core/queue"
)

type ExchangeDescriptor struct {
	name          string
	refCount      int
	exchangeQueue *queue.QueueDescriptor
	bindQueues    map[string]*queue.QueueDescriptor
}

var exchanges = make(map[string]*ExchangeDescriptor)
var bindings = make(map[string][]string)

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
		name:          name,
		refCount:      1,
		exchangeQueue: queue.GetQueue(fmt.Sprintf("exchange.%s.queue", name)),
		bindQueues:    make(map[string]*queue.QueueDescriptor),
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

		if len(messages) == 0 {
			continue
		}

		wait := make(map[string]int64)

		for qname, q := range exchange.bindQueues {
			wait[qname] = q.EnqueueAsync(&core.Operation{
				Op:       core.OpPublish,
				Target:   exchange.name,
				Messages: messages,
			})
		}

		for qname, target := range wait {
			q, ok := exchange.bindQueues[qname]

			if !ok {
				continue
			}

			q.EnqueueWait(target)
		}

		ids := make([]int64, 0)

		for _, m := range messages {
			ids = append(ids, m.Id)
		}

		exchangeQueue.Ack(consumerId, ids)
	}
}
