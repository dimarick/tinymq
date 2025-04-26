package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
	"tinymq/core"
	"tinymq/core/exchange"
	"tinymq/core/queue"
	"tinymq/http_core"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}
var queuesMutex sync.Mutex
var queues = make(map[string]*queue.QueueDescriptor)

type Confirm struct {
	Method string `json:"method"`
}

type Confirmed struct {
	Method     string
	MessageIds []int64
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
	}

	defer func(conn *websocket.Conn) {
		err := conn.Close()
		if err != nil {
			log.Panic(err)
		}
	}(conn)

	closed := false

	consumerId := rand.Int63()

	for {
		var messages []core.Message
		for {
			messages = q.Consume(consumerId, n, 200*time.Millisecond)

			if len(messages) > 0 {
				err := sendMessage(conn, messages)
				if err != nil {
					showErrorWebsocket(conn, "Consume error", err)
				}
				break
			} else {
				if closed {
					q.DetachConsumer(consumerId)
					return
				}

				err = conn.WriteControl(websocket.PingMessage, []byte("tnmq"), time.Now().Add(time.Second))
				if err != nil {
					showErrorWebsocket(conn, "Ping error", err)
					closed = true
				}
			}
		}

		var confirm Confirm

		err := readMessages(conn, &confirm)
		if err != nil {
			showErrorWebsocket(conn, err.Error(), err)
			q.DetachConsumer(consumerId)
			return
		}

		ids := []int64{}

		for _, m := range messages {
			ids = append(ids, m.Id)
		}

		switch confirm.Method {
		case "ack":
			q.Ack(consumerId, ids)
		case "requeue":
			q.Requeue(consumerId, ids)
		case "reject":
			q.Reject(consumerId, ids)
		default:
			showErrorWebsocket(conn, fmt.Sprintf("Unsupported method: %s", confirm.Method), err)
			continue
		}

		err = sendMessage(conn, Confirmed{Method: confirm.Method, MessageIds: ids})
		if err != nil {
			showErrorWebsocket(conn, "sendMessage error", err)
			q.DetachConsumer(consumerId)
			_ = conn.Close()
		}
	}
}

func readMessages(conn *websocket.Conn, value any) error {
	messageType, message, err := conn.ReadMessage()
	if err != nil {
		showErrorWebsocket(conn, "Socket error", err)
	}
	log.Printf("Received: %s", message)

	if messageType != websocket.TextMessage {
		return errors.New("messageType should be text")
	}

	err = json.Unmarshal(message, value)
	if err != nil {

		_ = conn.Close()
		return errors.Join(
			errors.New(fmt.Sprintf("Message parse error: %s", message)),
			err,
		)
	}
	return nil
}

func showErrorWebsocket(conn *websocket.Conn, e string, err error) {
	_, _ = fmt.Fprintln(os.Stderr, e)
	_, _ = fmt.Fprintln(os.Stderr, err)
	_ = sendMessage(conn, e)
}

func sendMessage(conn *websocket.Conn, message any) error {
	writer, err := conn.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	defer func(writer io.WriteCloser) {
		err := writer.Close()
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
		}
	}(writer)

	err = json.NewEncoder(writer).Encode(message)

	if err != nil {
		return err
	}

	return nil
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
