package core

const (
	OpPublish = 0x0
	OpAck     = 0x1
	OpRequeue = 0x2
)

const OperationSignature = "tnmq"

type Operation struct {
	Op       uint8
	Target   string
	Messages []Message
}
