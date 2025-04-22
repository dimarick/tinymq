package core

import "sync"

type MessageStatus struct {
	Ptr        int64
	Size       int64
	ConsumerId int64
	Status     uint8
}

type MessageStatusMap struct {
	mutex sync.Mutex
	m     map[int64]map[int64]MessageStatus
}

type MessageStatusEntry struct {
	FileId    int64
	MessageId int64
	Status    MessageStatus
}

func NewMessageStatusMap() *MessageStatusMap {
	result := new(MessageStatusMap)

	*result = MessageStatusMap{
		m: make(map[int64]map[int64]MessageStatus),
	}

	return result
}

func (m *MessageStatusMap) Load(fileId int64, messageId int64) (messageStatus MessageStatus, ok bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	fileMap, ok := m.m[fileId]

	if !ok {
		return MessageStatus{}, false
	}

	result, ok := fileMap[messageId]

	return result, ok
}

func (m *MessageStatusMap) Store(fileId int64, messageId int64, status MessageStatus) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	fileMap, ok := m.m[fileId]

	if !ok {
		fileMap = make(map[int64]MessageStatus)
		m.m[fileId] = fileMap
	}

	fileMap[messageId] = status
}

func (m *MessageStatusMap) Delete(fileId int64, messageId int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	fileMap, ok := m.m[fileId]

	if !ok {
		return
	}

	delete(fileMap, messageId)
}

func (m *MessageStatusMap) DeleteFile(fileId int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.m, fileId)
}

func (m *MessageStatusMap) Range() []MessageStatusEntry {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	rows := make([]MessageStatusEntry, 0)

	for fileId, status := range m.m {
		for messageId, messageStatus := range status {
			rows = append(rows, MessageStatusEntry{fileId, messageId, messageStatus})
		}
	}

	return rows
}

func (m *MessageStatusMap) RangeFiles() map[int64]map[int64]MessageStatus {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	rows := make(map[int64]map[int64]MessageStatus, 0)

	for fileId, status := range m.m {
		rows[fileId] = make(map[int64]MessageStatus, 0)
		for messageId, messageStatus := range status {
			rows[fileId][messageId] = messageStatus
		}
	}

	return rows
}

func (m *MessageStatusMap) StoreFile(fileId int64, status map[int64]MessageStatus) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.m[fileId] = status
}

func (m *MessageStatusMap) LoadFile(fileId int64) map[int64]MessageStatus {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	result := map[int64]MessageStatus{}

	fileMap, ok := m.m[fileId]

	if !ok {
		return result
	}

	for messageId, status := range fileMap {
		result[messageId] = status
	}

	return result
}
