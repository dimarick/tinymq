package message

import (
	"encoding/json"
	"github.com/gorilla/websocket"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
	"tinymq/core/exchange"
	"tinymq/core/queue"
	"tinymq/http_core"
)

var upgrader = websocket.Upgrader{}
var queuesMutex sync.Mutex
var queues = make(map[string]*queue.QueueDescriptor)

type Confirm struct {
	method     string
	messageIds []int64
}

func ConsumeHandler(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queue")
	n, err := strconv.Atoi(r.PathValue("count"))

	if err != nil {
		http_core.ShowResponse(w, http_core.Response{
			Status: http.StatusNotFound,
			Header: http.Header{},
			Body:   "count should be valid integer",
		})

		return
	}

	if queueName == "" || !exchange.HasQueue(queueName) {
		http_core.ShowResponse(w, http_core.Response{
			Status: http.StatusNotFound,
			Header: http.Header{},
			Body:   "Queue not defined",
		})

		return
	}

	q := getQueue(queueName)

	conn, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		log.Panic(err)

		return
	}

	defer func(conn *websocket.Conn) {
		err := conn.Close()
		if err != nil {
			log.Panic(err)
		}
	}(conn)

	consumerId := rand.Int63()

	for {
		for {
			messages := q.Consume(consumerId, n, 1*time.Second)

			if len(messages) > 0 {
				sendMessage(conn, messages)
				break
			}
		}

		messageType, message, err := conn.ReadMessage()
		if err != nil {
			log.Panic("Error during message reading:", err)
		}
		log.Printf("Received: %s", message)

		if messageType != websocket.TextMessage {
			log.Panic("messageType should be text")
		}

		var confirm Confirm

		err = json.Unmarshal(message, &confirm)
		if err != nil {
			log.Panic(err)
		}

		switch confirm.method {
		case "ack":
			q.Ack(consumerId, confirm.messageIds)
		case "requeue":
			q.Requeue(consumerId, confirm.messageIds)
		case "reject":
			q.Reject(consumerId, confirm.messageIds)
		case "discard":
			q.RejectDiscard(consumerId, confirm.messageIds)
		default:
			log.Panic(err)
		}

		sendMessage(conn, Confirm{"ok", confirm.messageIds})
	}
}

func sendMessage(conn *websocket.Conn, message any) {
	writer, err := conn.NextWriter(websocket.TextMessage)
	defer func(writer io.WriteCloser) {
		err := writer.Close()
		if err != nil {
			log.Panic(err)
		}
	}(writer)

	if err != nil {
		log.Panic(err)
	}

	err = json.NewEncoder(writer).Encode(message)

	if err != nil {
		log.Panic(err)
	}

	return
}

func getQueue(queueName string) *queue.QueueDescriptor {
	queuesMutex.Lock()
	defer queuesMutex.Unlock()
	queueObject, ok := queues[queueName]
	if !ok {
		queueObject = queue.GetQueue(queueName)
		queues[queueName] = queueObject
	}
	return queueObject
}
