package queue

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
	"tinymq/config"
	"tinymq/core"
	"tinymq/core/sp_sync"
	"tinymq/serializer"
)

type QueueDescriptor struct {
	name                   string
	refCount               int
	queueDataPath          string
	publishRefFile         *os.File
	consumeRefFileMutex    sync.Mutex
	consumeRefFile         *os.File
	publishDataFile        *os.File
	publishDataWriterMutex sync.Mutex
	publishDataWriter      *bufio.Writer
	consumeDataFile        *os.File
	publishRef             core.Ref
	consumeRef             core.Ref
	consumerWaitLock       *sp_sync.SpLock
	flushDataLock          *sp_sync.SpLock
	messageStatus          *core.MessageStatusMap
	messageStatusDirtyTime sync.Map
	messageStatusSyncTime  sync.Map
	connectedConsumers     sync.Map
	consumeMutex           sync.Mutex
	readerMutex            sync.Mutex
	backgroundTasks        sync.WaitGroup
	err                    error
}

var queues = make(map[string]*QueueDescriptor)

func GetQueue(name string) *QueueDescriptor {
	if _, ok := queues[name]; !ok {
		queue := initQueue(name)
		queues[name] = queue

		return queue
	}

	queue, _ := queues[name]
	queue.refCount++

	return queue
}
func (queue *QueueDescriptor) Close() {
	queue.refCount--
	if queue.refCount > 0 {
		return
	}
	queue.backgroundTasks.Wait()

	queue.publishRefFile.Close()
	queue.consumeRefFile.Close()
	queue.publishDataFile.Close()
	queue.consumeDataFile.Close()

	delete(queues, queue.name)
}

func (queue *QueueDescriptor) Enqueue(operation *core.Operation) {
	queue.EnqueueWait(queue.EnqueueAsync(operation))
	queue.TriggerWaitingConsumers()
}

func (queue *QueueDescriptor) EnqueueAsync(operation *core.Operation) int64 {
	if queue.err != nil {
		log.Panic(queue.err)
	}

	var err error

	data := make([]byte, 0, 1024)
	for _, message := range operation.Messages {
		data, err = serializer.SerializeOperation(data, core.Operation{
			Op:     operation.Op,
			Target: operation.Target,
			Messages: []core.Message{
				message,
			},
		})

		if err != nil {
			log.Panic(err)
		}
	}

	queue.publishDataWriterMutex.Lock()
	_, err = queue.publishDataWriter.Write(data)
	queue.publishRef.Ptr += int64(len(data))

	if err != nil {
		queue.publishDataWriterMutex.Unlock()
		log.Panic(err)
	}

	if queue.publishRef.Ptr > config.GetConfig().MaxPartSize {
		queue.rotatePublishQueuePart(queue.publishRef.Id + 1)
	}

	queue.publishDataWriterMutex.Unlock()

	return queue.publishRef.Ptr
}

func (queue *QueueDescriptor) Consume(consumerId int64, n int, timeout time.Duration) []core.Message {
	if queue.err != nil {
		log.Panic(queue.err)
	}
	queue.connectedConsumers.Store(consumerId, true)

	queue.EnqueueWait(queue.publishRef.Ptr)

	messages := queue.readMessages(consumerId, n)

	if len(messages) == 0 {
		now := time.Now().UnixNano()
		if !queue.waitQueueWithTimeout(now, timeout) {
			return messages
		}

		runtime.Gosched()
		err := queue.consumeDataFile.Sync()
		if err != nil {
			log.Panic(err)
		}
		messages = queue.readMessages(consumerId, n)
	}

	queue.flushStatusWait(queue.consumeRef.Id)

	return messages
}

func (queue *QueueDescriptor) DetachConsumer(consumerId int64) {
	if queue.err != nil {
		log.Panic(queue.err)
	}

	var ids []int64
	for _, entry := range queue.messageStatus.Range() {
		if entry.Status.Status == core.StatusPending && entry.Status.ConsumerId == consumerId {
			ids = append(ids, entry.MessageId)
		}
	}

	queue.Reject(consumerId, ids)
	queue.connectedConsumers.Delete(consumerId)
}

func (queue *QueueDescriptor) readMessages(consumerId int64, n int) []core.Message {
	queue.consumeMutex.Lock()
	defer queue.consumeMutex.Unlock()

	_, err := queue.consumeDataFile.Seek(queue.consumeRef.Ptr, io.SeekStart)
	messages := make([]core.Message, 0)

	reader := serializer.NewCountedReader(bufio.NewReader(queue.consumeDataFile))

	if err != nil {
		log.Panic(err)
	}

	for {
		if (len(messages)) >= n {
			break
		}

		stat, err := queue.consumeDataFile.Stat()

		if err != nil {
			log.Panic(err)
		}

		// At end of file
		if stat.Size() == queue.consumeRef.Ptr {
			if queue.consumeRef.Id != queue.publishRef.Id {
				queue.flushStatusWait(queue.consumeRef.Id)
				queue.rotateConsumeQueuePart(queue.consumeRef.Id + 1)
				reader = serializer.NewCountedReader(bufio.NewReader(queue.consumeDataFile))
				continue
			}

			break
		}

		reader.ResetCount()
		operation, err := serializer.DeserializeOperation(reader)

		if err != nil {
			log.Panic(err)
		}

		if len(operation.Messages) != 1 {
			log.Panic(errors.New("More than one message is unexpected"))
		}

		for _, message := range operation.Messages {
			messageStatus, ok := queue.messageStatus.Load(queue.consumeRef.Id, message.Id)

			if ok {
				if messageStatus.Status == core.StatusAck || messageStatus.Status == core.StatusRequeue {
					continue
				}

				_, ok = queue.connectedConsumers.Load(messageStatus.ConsumerId)
				if messageStatus.Status == core.StatusPending && ok {
					continue
				}

				// else consumer dead, message not pending now
			}

			messages = append(messages, message)

			queue.messageStatus.Store(queue.consumeRef.Id, message.Id, core.MessageStatus{
				Ptr:        queue.consumeRef.Ptr,
				Size:       reader.Count(),
				ConsumerId: consumerId,
				Status:     core.StatusPending,
			})
			queue.flushStatusAsync(queue.consumeRef.Id)
		}
		queue.consumeRef.Ptr += reader.Count()
	}

	return messages
}

func (queue *QueueDescriptor) confirm(consumerId int64, messageIds []int64, confirmStatus uint8, requeue bool) {
	filesProcessed := make(map[int64]map[int64]core.MessageStatus, 0)
	for _, entry := range queue.messageStatus.Range() {
		for _, messageId := range messageIds {
			messageStatus := entry.Status

			if entry.MessageId == messageId && messageStatus.ConsumerId == consumerId {
				messageStatus.Status = confirmStatus

				status, ok := filesProcessed[entry.FileId]
				if !ok {
					status = map[int64]core.MessageStatus{}
					filesProcessed[entry.FileId] = status
				}

				status[entry.MessageId] = messageStatus
			}

		}
	}

	for id, status := range filesProcessed {
		queue.flushStatusAsync(id)
		queue.messageStatus.StoreFile(id, status)
		if requeue {
			queue.requeue(id, status)
			ptr := queue.publishRef.Ptr
			queue.flushStatusWait(id)
			queue.EnqueueWait(ptr)
		} else {
			queue.flushStatusWait(id)
		}
	}
}

func (queue *QueueDescriptor) Ack(consumerId int64, messageIds []int64) {
	queue.confirm(consumerId, messageIds, core.StatusAck, false)
}

func (queue *QueueDescriptor) Reject(consumerId int64, messageIds []int64) {
	queue.confirm(consumerId, messageIds, core.StatusReject, false)
	queue.TriggerWaitingConsumers()
}

func (queue *QueueDescriptor) Requeue(consumerId int64, messageIds []int64) {
	queue.confirm(consumerId, messageIds, core.StatusRequeue, true)
	queue.TriggerWaitingConsumers()
}

func (queue *QueueDescriptor) requeue(fileId int64, status map[int64]core.MessageStatus) {
	operations, err := queue.readMessagesByIds(fileId, status)

	if err != nil {
		log.Panic(err)
	}

	for _, operation := range operations {
		queue.EnqueueAsync(&core.Operation{
			Op:       core.OpRequeue,
			Target:   operation.Target,
			Messages: operation.Messages,
		})
	}
}

func (queue *QueueDescriptor) readMessagesByIds(fileId int64, status map[int64]core.MessageStatus) ([]core.Operation, error) {
	queue.readerMutex.Lock()
	defer queue.readerMutex.Unlock()
	consumeDataPath := fmt.Sprintf("%s/queue.%d", queue.queueDataPath, fileId)
	consumeDataFile, err := os.OpenFile(consumeDataPath, os.O_RDONLY, 0666)

	operations := make([]core.Operation, 0)

	if err != nil {
		return operations, err
	}

	defer consumeDataFile.Close()

	for _, messageStatus := range status {
		if messageStatus.Status != core.StatusRequeue {
			continue
		}

		_, err = consumeDataFile.Seek(messageStatus.Ptr, io.SeekStart)

		if err != nil {
			return operations, err
		}

		operation, err := serializer.DeserializeOperation(consumeDataFile)

		if err != nil {
			return operations, err
		}

		operations = append(operations, operation)
	}

	return operations, nil
}

func (queue *QueueDescriptor) flushStatus(fileId int64, status map[int64]core.MessageStatus) {
	consumeDataPath := fmt.Sprintf("%s/queue.%d", queue.queueDataPath, fileId)
	consumeDataStatusPath := fmt.Sprintf("%s/queue.%d.status", queue.queueDataPath, fileId)
	consumeDataStatusFile, err := os.OpenFile(consumeDataStatusPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)

	if errors.Is(err, os.ErrExist) {
		consumeDataStatusFile, err = os.OpenFile(consumeDataStatusPath, os.O_WRONLY|os.O_EXCL, 0666)
	}

	if err != nil {
		log.Panic(err)
	}

	defer consumeDataStatusFile.Close()

	ackBytes := int64(0)

	if fileId != queue.publishRef.Id {
		for _, messageStatus := range status {
			if messageStatus.Status == core.StatusPending {
				continue
			}
			if messageStatus.Status == core.StatusReject {
				continue
			}

			ackBytes += messageStatus.Size
		}
	}

	consumeDataFile, err := os.OpenFile(consumeDataPath, os.O_RDONLY, 0666)

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			consumeDataFile = nil
		} else {
			log.Panic(err)
		}
	}

	if consumeDataFile != nil {
		defer consumeDataFile.Close()
		stat, err := consumeDataFile.Stat()
		if err != nil {
			log.Panic(err)
		}

		if ackBytes > 0 && stat.Size() == ackBytes {
			queue.messageStatus.DeleteFile(fileId)
			defer func() {
				err = os.Remove(consumeDataStatusPath)

				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "Cannot cleanup file %s", consumeDataStatusPath)
				}

				err = os.Remove(consumeDataPath)
				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "Cannot cleanup file %s", consumeDataPath)
				}
			}()

			return
		}
	}

	buffer, err := serializer.SerializeMessageStatuses(nil, status)

	if err != nil {
		log.Panic(err)
	}

	_, err = consumeDataStatusFile.WriteAt(buffer, ackBytes)

	if err != nil {
		log.Panic(err)
	}
}

func (queue *QueueDescriptor) loadStatus(fileId int64) map[int64]core.MessageStatus {
	consumeDataStatusPath := fmt.Sprintf("%s/queue.%d.status", queue.queueDataPath, fileId)
	consumeDataStatusFile, err := os.OpenFile(consumeDataStatusPath, os.O_RDONLY|os.O_EXCL, 0666)

	if errors.Is(err, os.ErrNotExist) {
		return map[int64]core.MessageStatus{}
	}

	if err != nil {
		log.Panic(err)
	}

	defer consumeDataStatusFile.Close()

	stat, err := consumeDataStatusFile.Stat()

	if err != nil {
		log.Panic(err)
	}

	if stat.Size() == 0 {
		return map[int64]core.MessageStatus{}
	}

	reader := bufio.NewReader(consumeDataStatusFile)
	result, err := serializer.DeserializeMessageStatuses(reader)

	if err != nil {
		log.Panic(err)
	}

	for id, messageStatus := range result {
		if messageStatus.Status != core.StatusPending && messageStatus.Status != core.StatusReject {
			continue
		}

		// when server restarted, all pending statuses is rejected and does not matter
		delete(result, id)
	}

	return result
}

func (queue *QueueDescriptor) rotateConsumeQueuePart(fileId int64) {
	queue.consumeRef.Id = fileId
	queue.consumeRef.Ptr = 0

	SetRef(queue.consumeRefFile, queue.consumeRef)
	err := queue.consumeRefFile.Sync()
	if err != nil {
		log.Panic(err)
	}

	consumeDataPath := fmt.Sprintf("%s/queue.%d", queue.queueDataPath, queue.consumeRef.Id)

	queue.consumeDataFile, err = os.OpenFile(consumeDataPath, os.O_RDONLY, 0666)

	if err != nil {
		log.Panic(err)
		return
	}

	_, err = queue.consumeDataFile.Seek(0, 0)

	if err != nil {
		log.Panic(err)
	}

	queue.messageStatus.StoreFile(queue.consumeRef.Id, queue.loadStatus(queue.consumeRef.Id))
}

func initQueue(name string) *QueueDescriptor {
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()

	queue, ok := queues[name]

	if !ok {

		queueDataPath := fmt.Sprintf("%s/queue/%s", config.GetConfig().StoragePath, name)

		publishRefFilePath := fmt.Sprintf("%s/publish.ref", queueDataPath)
		consumeRefFilePath := fmt.Sprintf("%s/consume.ref", queueDataPath)

		err := os.MkdirAll(queueDataPath, 0777)
		if err != nil {
			log.Panic(err)
		}

		consumeRefFile, err := os.OpenFile(consumeRefFilePath, os.O_RDWR|os.O_CREATE, 0666)

		if err != nil {
			log.Panic(err)
		}

		_, err = consumeRefFile.Seek(0, 0)

		if err != nil {
			log.Panic(err)
		}

		publishRefFile, err := os.OpenFile(publishRefFilePath, os.O_RDWR|os.O_CREATE, 0666)

		if err != nil {
			log.Panic(err)
		}

		_, err = publishRefFile.Seek(0, 0)

		if err != nil {
			log.Panic(err)
		}

		result := new(QueueDescriptor)

		publishRef := GetRef(publishRefFile)
		consumeRef := GetRef(consumeRefFile)

		publishDataFilePath := fmt.Sprintf("%s/queue.%d", queueDataPath, publishRef.Id)

		publishDataFile, err := os.OpenFile(publishDataFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

		if err != nil {
			log.Panic(err)
		}

		stat, err := publishDataFile.Stat()

		if err != nil {
			log.Panic(err)
		}

		publishRef.Ptr = stat.Size()

		*result = QueueDescriptor{
			name:              name,
			queueDataPath:     queueDataPath,
			publishRefFile:    publishRefFile,
			consumeRefFile:    consumeRefFile,
			publishDataFile:   publishDataFile,
			publishDataWriter: bufio.NewWriterSize(publishDataFile, 2*int(config.GetConfig().MaxPartSize)),
			publishRef:        publishRef,
			consumeRef:        consumeRef,
			messageStatus:     core.NewMessageStatusMap(),
			flushDataLock:     sp_sync.NewSpLock(500 * time.Microsecond),
			consumerWaitLock:  sp_sync.NewSpLock(500 * time.Microsecond),
		}

		result.rotateConsumeQueuePart(consumeRef.Id)

		result.refCount++

		go result.flushEnqueueLoop()
		go result.flushConsumeStatusLoop()

		return result
	}

	return queue
}

func (queue *QueueDescriptor) rotatePublishQueuePart(fileId int64) {
	var err error

	err = queue.publishDataWriter.Flush()

	if err != nil {
		log.Panic(err)
	}

	err = queue.publishDataFile.Sync()

	if err != nil {
		log.Panic(err)
	}

	prevPublishDataFile := queue.publishDataFile

	queue.publishRef.Id = fileId

	publishDataFilePath := fmt.Sprintf("%s/queue.%d", queue.queueDataPath, queue.publishRef.Id)

	publishDataFile, err := os.OpenFile(publishDataFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

	if err != nil {
		log.Panic(err)
	}

	stat, err := publishDataFile.Stat()

	if err != nil {
		log.Panic(err)
	}

	if stat.Size() > 0 {
		log.Panic(errors.New("Size greater than 0"))
	}

	queue.publishRef.Ptr = 0

	queue.publishDataFile = publishDataFile
	queue.publishDataWriter = bufio.NewWriterSize(publishDataFile, 2*int(config.GetConfig().MaxPartSize))

	SetRef(queue.publishRefFile, queue.publishRef)
	err = queue.publishRefFile.Sync()
	if err != nil {
		log.Panic(err)
	}

	go func() {
		queue.publishDataWriterMutex.Lock()
		err = prevPublishDataFile.Close()
		queue.publishDataWriterMutex.Unlock()
	}()
}

func (queue *QueueDescriptor) waitQueueWithTimeout(target int64, timeout time.Duration) bool {
	if timeout == 0 {
		return false
	}
	err := queue.consumerWaitLock.LockUntilInt(target, timeout)

	return err == nil
}

func (queue *QueueDescriptor) TriggerWaitingConsumers() {
	now := time.Now().UnixNano()
	queue.consumerWaitLock.UnlockReached(now)
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

func (queue *QueueDescriptor) EnqueueWait(target int64) {
	initialId := queue.publishRef.Id
	err := queue.flushDataLock.LockUntil(func(version any) bool {
		switch version.(type) {
		case []int64:
			v := version.([]int64)
			fileId := v[0]
			ref := v[1]

			if fileId > initialId {
				return true
			}

			if ref >= target {
				return true
			}

			return false
		default:
			return false
		}
	}, 1*time.Second)

	if errors.Is(err, sp_sync.ErrorTimeout) {
		_, _ = fmt.Fprintf(os.Stderr, "EnqueueWait Timeout %d %v\n", target, err)
		return
	}

	if err != nil {
		log.Panic(err)
	}
}

func (queue *QueueDescriptor) consumeStatusWait(fileId int64, target int64) {
	startWait := time.Now().Unix()
	for {
		if queue.err != nil {
			log.Panic(queue.err)
		}

		time.Sleep(500 * time.Microsecond)
		value, ok := queue.messageStatusSyncTime.Load(fileId)

		if !ok {
			continue
		}

		syncTime := value.(int64)

		if syncTime >= target {
			break
		}

		if (startWait + 10) < time.Now().Unix() {
			startWait = time.Now().Unix()
			log.Printf("consumeStatusWait is waiting for %d more than 10 seconds for state %d of file %s", target, syncTime, queue.publishDataFile.Name())
		}
		runtime.Gosched()
	}
}

func (queue *QueueDescriptor) flushEnqueueLoop() {
	queue.backgroundTasks.Add(1)
	for {
		if queue.refCount == 0 {
			break
		}
		runtime.Gosched()
		if queue.publishDataWriter.Buffered() == 0 {
			time.Sleep(500 * time.Microsecond)

			stat, err := queue.publishDataFile.Stat()

			if err != nil {
				queue.err = err
				log.Panic(err)
			}

			queue.flushDataLock.UnlockReached([]int64{queue.publishRef.Id, stat.Size()})
			continue
		}

		var err error
		queue.publishDataWriterMutex.Lock()
		err = queue.publishDataWriter.Flush()
		queue.publishDataWriterMutex.Unlock()
		runtime.Gosched()

		if err != nil {
			queue.err = err
			log.Panic(err)
		}
		err = queue.publishDataFile.Sync()
		runtime.Gosched()

		if err != nil {
			queue.err = err
			log.Panic(err)
		}

		stat, err := queue.publishDataFile.Stat()

		if err != nil {
			queue.err = err
			log.Panic(err)
		}

		queue.flushDataLock.UnlockReached([]int64{queue.publishRef.Id, stat.Size()})
	}
	queue.backgroundTasks.Done()
}

func (queue *QueueDescriptor) flushConsumeStatusLoop() {
	queue.backgroundTasks.Add(1)
	for {
		if queue.refCount == 0 {
			break
		}
		runtime.Gosched()
		time.Sleep(500 * time.Microsecond)
		for fileId, status := range queue.messageStatus.RangeFiles() {
			runtime.Gosched()
			value, ok := queue.messageStatusDirtyTime.Load(fileId)

			if !ok {
				continue
			}

			dirtyTime := value.(int64)

			value, ok = queue.messageStatusSyncTime.Load(fileId)

			syncTime := int64(0)
			if ok {
				syncTime = value.(int64)
			}

			if dirtyTime > syncTime {
				queue.flushStatus(fileId, status)
				queue.messageStatusSyncTime.Store(fileId, time.Now().UnixNano())
			}
		}
	}
	queue.backgroundTasks.Done()
}

func (queue *QueueDescriptor) flushStatusAsync(fileId int64) {
	targetTime := time.Now().UnixNano()
	queue.messageStatusDirtyTime.Store(fileId, targetTime)
}

func (queue *QueueDescriptor) flushStatusWait(fileId int64) {
	targetTime, ok := queue.messageStatusDirtyTime.Load(fileId)

	if !ok {
		return
	}

	queue.consumeStatusWait(fileId, targetTime.(int64))
}
