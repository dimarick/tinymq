package core

const (
	OpQueue         uint8 = 0x80
	OpExchange            = 0x40
	OpPublish             = 0x40
	OpPublishUnique       = 0x41
	OpConsume             = 0x81
	OpAck                 = 0x82
	OpReject              = 0x83
	OpRequeue             = 0x84
)

const OperationSignature = "tnmq"

type Operation struct {
	Op       uint8
	Target   string
	Messages []Message
}
