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
	"runtime"
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

var ErrSocket = errors.New("Socket error")
var ErrTimeout = errors.New("Socket timeout")

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

	defer func() {
		_ = conn.Close()
	}()

	consumerId := rand.Int63()

	log.Printf("Consumer %d connected", consumerId)

	var messages []core.Message

	messageMutex := sync.Mutex{}

	closed := false

	defer func() {
		closed = true
	}()

	go func() {
		for {
			if closed {
				break
			}
			if len(messages) == 0 {
				m := q.Consume(consumerId, n, 200*time.Millisecond)
				messageMutex.Lock()
				messages = m
				messageMutex.Unlock()
			} else {
				runtime.Gosched()
				time.Sleep(time.Millisecond)
				continue
			}

			if len(messages) > 0 {
				err := sendMessage(conn, messages)
				if err != nil {
					showErrorWebsocket(conn, "Consume error", err)
				}
				if err != nil {
					log.Panic(err)
				}
			}
		}
	}()

	for {
		var confirm Confirm

		err := readMessages(conn, &confirm)

		if errors.Is(err, ErrSocket) {
			showErrorWebsocket(conn, "socket error", err)
			q.DetachConsumer(consumerId)
			_ = conn.Close()
			break
		}

		if err != nil {
			showErrorWebsocket(conn, err.Error(), err)
			continue
		}

		ids := []int64{}

		messageMutex.Lock()

		for _, m := range messages {
			ids = append(ids, m.Id)
		}

		messageMutex.Unlock()
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

		messages = []core.Message{}

		err = sendMessage(conn, Confirmed{Method: confirm.Method, MessageIds: ids})
		if err != nil {
			showErrorWebsocket(conn, "sendMessage error", err)
			q.DetachConsumer(consumerId)
			_ = conn.Close()
			break
		}
	}
}

func readMessages(conn *websocket.Conn, value any) error {
	messageType, message, err := conn.ReadMessage()

	if err != nil {
		return fmt.Errorf("socket error %w %w", ErrSocket, err)
	}
	log.Printf("Received: %s", message)

	if messageType != websocket.TextMessage {
		return errors.New("messageType should be text")
	}

	err = json.Unmarshal(message, value)
	if err != nil {
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
