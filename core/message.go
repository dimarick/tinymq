package core

const (
	TypeJson   uint8 = 0
	TypeText         = 1
	TypeBinary       = 2
	TypeGzip         = 128
)

type Message struct {
	ContentType uint8
	Id          string
	Data        string
}
