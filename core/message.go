package core

const (
	TypeJson   uint8 = 0
	TypeText         = 1
	TypeBinary       = 2
)

const (
	// StatusPending message was sent to consumer and waiting confirm
	StatusPending uint8 = 0
	// StatusAck message confirmed and deleted from queue
	StatusAck = 1
	// StatusRequeue message failed and pushed to queue end
	StatusRequeue = 2
	// StatusReject message failed and removed from queue
	StatusReject = 3
)

type Message struct {
	ContentType uint8
	Id          int64
	Data        string
}
