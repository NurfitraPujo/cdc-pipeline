package protocol

import "time"

//go:generate msgp

type Message struct {
	SourceID  string    `msg:"sid"`
	Table     string    `msg:"tbl"`
	Op        string    `msg:"op"`
	LSN       uint64    `msg:"lsn"`
	PK        string    `msg:"pk"`
	Payload   []byte    `msg:"pay"`
	Timestamp time.Time `msg:"ts"`
}

type MessageBatch []Message

func UnmarshalMessageBatch(b []byte, batch *[]Message) ([]byte, error) {
	var m MessageBatch
	rest, err := m.UnmarshalMsg(b)
	if err != nil {
		return nil, err
	}
	*batch = []Message(m)
	return rest, nil
}
