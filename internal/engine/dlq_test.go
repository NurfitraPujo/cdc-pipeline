package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/engine/mocks"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestConsumer_DLQ(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	retryCfg := protocol.RetryConfig{
		MaxRetries:      2,
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     2 * time.Millisecond,
		EnableDLQ:       true,
	}

	c := NewConsumer("p1", mockSub, mockPub, mockSink, nil, mockKV, 1, 100*time.Millisecond, retryCfg)

	t.Run("Isolate and route to DLQ after MaxRetries", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		msg := protocol.Message{
			SourceID: "s1",
			Table:    "t1",
			Op:       "insert",
			Payload:  []byte(`{"id":1}`),
		}
		batch := protocol.MessageBatch{msg}
		data, _ := batch.MarshalMsg(nil)
		
		wmMsg1 := message.NewMessage("uuid1", data)
		wmMsg2 := message.NewMessage("uuid2", data) 

		msgChan := make(chan *message.Message, 3)
		msgChan <- wmMsg1
		msgChan <- wmMsg2
		msgChan <- wmMsg1 // Redelivery 3 of uuid1
		close(msgChan)

		mockSub.EXPECT().Subscribe(gomock.Any(), "topic1").Return(msgChan, nil)
		mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

		gomock.InOrder(
			// Attempt 1: wmMsg1. Fails. retries[uuid1]=1. Nack.
			mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).Return(errors.New("sink error")).Times(1),
			
			// Attempt 2: wmMsg2. Combined batch {wmMsg1, wmMsg2} fails.
			// retries[uuid1]=2, retries[uuid2]=1. 
			mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).Return(errors.New("sink error")).Times(1),
			
			// Attempt 3: wmMsg1 redelivery. Combined batch {wmMsg1, wmMsg2} fails.
			// retries[uuid1]=3, retries[uuid2]=2.
			// 3 > 2 is TRUE -> ISOLATE.
			mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).Return(errors.New("sink error")).Times(1),
			
			// Isolation Mode for {wmMsg1, wmMsg2}:
			// 1. wmMsg1: attempts=3 >= 2. route to DLQ.
			mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).Return(errors.New("sink error")).Times(1),
			mockPub.EXPECT().Publish("daya_pipeline_p1_dlq", gomock.Any()).Return(nil).Times(1),
			
			// 2. wmMsg2: attempts=2 >= 2 is TRUE.
			// So wmMsg2 also to DLQ.
			mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).Return(errors.New("sink error")).Times(1),
			mockPub.EXPECT().Publish("daya_pipeline_p1_dlq", gomock.Any()).Return(nil).Times(1),
		)

		err := c.Run(ctx, "topic1")
		assert.NoError(t, err)
	})
}
