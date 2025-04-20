package queue

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
	"tinymq/config"
	"tinymq/core"
	"tinymq/serializer"
)

type QueueDescriptor struct {
	queueDataPath          string
	publishRefFile         *os.File
	consumeRefFile         *os.File
	publishDataFile        *os.File
	publishDataWriterMutex sync.Mutex
	publishDataWriter      *bufio.Writer
	consumeDataFile        *os.File
	publishRef             core.Ref
	consumeRef             core.Ref
	consumerChannelsMutex  sync.Mutex
	consumerChannels       []chan core.Operation
	flushDataMutex         sync.Mutex
	flushDataPtr           int64
	err                    error
}

var queues = make(map[string]*QueueDescriptor)

func GetQueue(name string) *QueueDescriptor {
	if _, ok := queues[name]; !ok {
		queues[name] = initQueue(name)
	}

	queue, _ := queues[name]

	return queue
}

func (queue *QueueDescriptor) Enqueue(operation *core.Operation) {
	if queue.err != nil {
		log.Panic(queue.err)

		return
	}

	if queue.directDispatchConsumers(operation) {
		return
	}

	data, err := serializer.SerializeOperation(nil, *operation)

	if err != nil {
		log.Panic(err)

		return
	}

	queue.publishDataWriterMutex.Lock()
	_, err = queue.publishDataWriter.Write(data)
	queue.publishRef.Ptr += int64(len(data))

	if err != nil {
		queue.publishDataWriterMutex.Unlock()
		log.Panic(err)

		return
	}

	if queue.publishRef.Ptr > config.GetConfig().MaxPartSize {
		queue.rotatePublishQueuePart()
	}

	queue.publishDataWriterMutex.Unlock()

	queue.flushWait(queue.publishRef.Ptr)
}

func (queue *QueueDescriptor) Consume(n int, timeout time.Duration) []core.Message {
	if queue.err != nil {
		log.Panic(queue.err)

		return nil
	}

	queue.flushWait(queue.publishRef.Ptr)

	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()

	reader := serializer.NewCountedReader(bufio.NewReader(queue.consumeDataFile))
	messages := make([]core.Message, 0)
	_, err := queue.consumeDataFile.Seek(queue.consumeRef.Ptr, io.SeekStart)

	if err != nil {
		log.Panic(err)

		return nil
	}

	for {
		if (len(messages)) >= n {
			return messages
		}

		stat, err := queue.consumeDataFile.Stat()

		if err != nil {
			log.Panic(err)

			return nil
		}

		reader.ResetCount()
		operation, err := serializer.DeserializeOperation(reader)

		if err != nil {
			log.Panic(err)

			return nil
		}

		messages = append(messages, operation.Messages...)
		queue.consumeRef.Ptr += reader.Count()

		// At end of file
		if stat.Size() == queue.consumeRef.Ptr {
			if queue.consumeRef.Id != queue.publishRef.Id {
				queue.rotateConsumeQueuePart()
				reader = serializer.NewCountedReader(bufio.NewReader(queue.consumeDataFile))
				continue
			}

			break
		}
	}

	if len(messages) == 0 {
		return queue.waitQueueWithTimeout(timeout)
	}

	offset, err := queue.consumeDataFile.Seek(0, io.SeekCurrent)

	if err != nil {
		log.Panic(err)

		return nil
	}

	queue.consumeRef.Ptr = offset
	SetRef(queue.consumeRefFile, queue.consumeRef)

	return messages
}

func (queue *QueueDescriptor) rotateConsumeQueuePart() {
	queue.consumeRef.Id++
	queue.consumeRef.Ptr = 0

	fileNameToRemove := queue.consumeDataFile.Name()

	SetRef(queue.consumeRefFile, queue.consumeRef)
	err := queue.consumeRefFile.Sync()
	if err != nil {
		log.Panic(err)

		return
	}

	consumeDataFilePath := fmt.Sprintf("%s/queue.%d", queue.queueDataPath, queue.consumeRef.Id)

	queue.consumeDataFile, err = os.OpenFile(consumeDataFilePath, os.O_RDWR, 0666)

	if err != nil {
		log.Panic(err)
		return
	}

	_, err = queue.consumeDataFile.Seek(0, 0)

	if err != nil {
		log.Panic(err)
		return
	}

	go os.Remove(fileNameToRemove)
}

func initQueue(name string) *QueueDescriptor {
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()

	queue, ok := queues[name]

	if !ok {

		queueDataPath := fmt.Sprintf("%s/queue/%s", *config.GetConfig().StoragePath, name)

		publishRefFilePath := fmt.Sprintf("%s/publish.ref", queueDataPath)
		consumeRefFilePath := fmt.Sprintf("%s/consume.ref", queueDataPath)

		err := os.MkdirAll(queueDataPath, 0777)
		if err != nil {
			log.Panic(err)
			return nil
		}

		consumeRefFile, err := os.OpenFile(consumeRefFilePath, os.O_RDWR|os.O_CREATE, 0666)

		if err != nil {
			log.Panic(err)
			return nil
		}

		_, err = consumeRefFile.Seek(0, 0)

		if err != nil {
			log.Panic(err)
			return nil
		}

		publishRefFile, err := os.OpenFile(publishRefFilePath, os.O_RDWR|os.O_CREATE, 0666)

		if err != nil {
			log.Panic(err)
			return nil
		}

		_, err = publishRefFile.Seek(0, 0)

		if err != nil {
			log.Panic(err)
			return nil
		}

		result := new(QueueDescriptor)

		publishRef := GetRef(publishRefFile)
		consumeRef := GetRef(consumeRefFile)

		publishDataFilePath := fmt.Sprintf("%s/queue.%d", queueDataPath, publishRef.Id)
		consumeDataFilePath := fmt.Sprintf("%s/queue.%d", queueDataPath, consumeRef.Id)

		publishDataFile, err := os.OpenFile(publishDataFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

		if err != nil {
			log.Panic(err)
			return nil
		}

		consumeDataFile, err := os.OpenFile(consumeDataFilePath, os.O_RDWR|os.O_CREATE, 0666)

		if err != nil {
			log.Panic(err)
			return nil
		}

		stat, err := publishDataFile.Stat()

		if err != nil {
			log.Panic(err)
			return nil
		}

		publishRef.Ptr = stat.Size()

		*result = QueueDescriptor{
			queueDataPath:     queueDataPath,
			publishRefFile:    publishRefFile,
			consumeRefFile:    consumeRefFile,
			publishDataFile:   publishDataFile,
			publishDataWriter: bufio.NewWriterSize(publishDataFile, 2*int(config.GetConfig().MaxPartSize)),
			consumeDataFile:   consumeDataFile,
			publishRef:        publishRef,
			consumeRef:        consumeRef,
		}

		go result.flushLoop()

		return result
	}

	return queue
}

func (queue *QueueDescriptor) rotatePublishQueuePart() {
	var err error

	err = queue.publishDataWriter.Flush()

	if err != nil {
		log.Panic(err)

		return
	}

	err = queue.publishDataFile.Sync()

	if err != nil {
		log.Panic(err)

		return
	}

	queue.publishRef.Id++

	publishDataFilePath := fmt.Sprintf("%s/queue.%d", queue.queueDataPath, queue.publishRef.Id)

	publishDataFile, err := os.OpenFile(publishDataFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

	if err != nil {
		log.Panic(err)

		return
	}

	stat, err := publishDataFile.Stat()

	if err != nil {
		log.Panic(err)

		return
	}

	if stat.Size() > 0 {
		log.Panic(errors.New("Size greater than 0"))

		return
	}

	queue.publishRef.Ptr = 0

	queue.publishDataFile = publishDataFile
	queue.publishDataWriter = bufio.NewWriterSize(publishDataFile, 2*int(config.GetConfig().MaxPartSize))

	SetRef(queue.publishRefFile, queue.publishRef)
	err = queue.publishRefFile.Sync()
	if err != nil {
		log.Panic(err)

		return
	}
}

func (queue *QueueDescriptor) waitQueueWithTimeout(timeout time.Duration) []core.Message {
	if timeout == 0 {
		return make([]core.Message, 0)
	}

	channel := make(chan core.Operation)
	queue.consumerChannelsMutex.Lock()
	queue.consumerChannels = append(queue.consumerChannels, channel)
	queue.consumerChannelsMutex.Unlock()

	if timeout > 0 {
		go func() {
			time.Sleep(timeout)
			channel <- core.Operation{}
		}()
	}

	operation := <-channel

	return operation.Messages
}

func (queue *QueueDescriptor) directDispatchConsumers(operation *core.Operation) bool {
	queue.consumerChannelsMutex.Lock()

	var channel chan core.Operation
	channelValid := false

	if len(queue.consumerChannels) > 0 {
		channel = queue.consumerChannels[0]
		channelValid = true
		queue.consumerChannels = queue.consumerChannels[1:]
	}

	queue.consumerChannelsMutex.Unlock()

	if channelValid {
		channel <- *operation
		return true
	}

	return false
}

func SetRef(file *os.File, ref core.Ref) {
	serializeRef, err := serializer.SerializeRef(nil, ref)

	if err != nil {
		log.Panic(err)
	}

	_, err = file.WriteAt(serializeRef, 0)

	if err != nil {
		log.Panic(err)
	}
}

func GetRef(file *os.File) core.Ref {
	stat, err := file.Stat()
	if err != nil {
		log.Panic(err)
	}

	if stat.Size() == 0 {
		return core.Ref{Id: 0, Ptr: 0}
	}

	buffer := make([]byte, stat.Size())
	_, err = file.ReadAt(buffer, 0)

	if err != nil {
		log.Panic(err)
	}

	ref, err := serializer.DeserializeRef(bytes.NewReader(buffer))

	if err != nil {
		log.Panic(err)
	}

	return ref
}

func (queue *QueueDescriptor) flushWait(target int64) {
	for {
		if queue.err != nil {
			log.Panic(queue.err)

			return
		}

		time.Sleep(500 * time.Microsecond)
		if queue.flushDataPtr >= target {
			break
		}
	}
}

func (queue *QueueDescriptor) flushLoop() {
	for {
		if queue.publishDataWriter.Buffered() == 0 {
			time.Sleep(500 * time.Microsecond)

			stat, err := queue.publishDataFile.Stat()

			if err != nil {
				queue.err = err
				log.Panic(err)
			}

			queue.flushDataPtr = stat.Size()
			continue
		}

		var err error

		queue.flushDataMutex.Lock()

		for {
			queue.publishDataWriterMutex.Lock()
			err = queue.publishDataWriter.Flush()
			queue.publishDataWriterMutex.Unlock()

			if err == nil {
				break
			}

			if err != io.ErrShortWrite {
				break
			}
		}

		if err != nil {
			queue.err = err
			log.Panic(err)
		}
		err = queue.publishDataFile.Sync()

		if err != nil {
			queue.err = err
			log.Panic(err)
		}

		stat, err := queue.publishDataFile.Stat()

		if err != nil {
			queue.err = err
			log.Panic(err)
		}

		queue.flushDataPtr = stat.Size()

		queue.flushDataMutex.Unlock()
	}
}
