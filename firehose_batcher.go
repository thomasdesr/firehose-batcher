package main

import (
	"time"

	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/pkg/errors"
)

// Batch limit taken from https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html
const (
	BATCH_ITEM_LIMIT = 500
	BATCH_SIZE_LIMIT = 4 * 1024 * 1024 // 4MB

	PER_ITEM_SIZE_LIMIT = 1024 * 1024 // 1 MB
)

type FirehoseBatcher struct {
	maxSendInterval time.Duration

	firehoseClient *firehose.Firehose

	inputBuffer chan []byte
	closed      bool
}

func NewFirehoseBatcher(fc *firehose.Firehose, sendInterval time.Duration) (*FirehoseBatcher, error) {
	fb := &FirehoseBatcher{
		maxSendInterval: sendInterval,
		firehoseClient:  fc,

		inputBuffer: make(chan []byte, BATCH_ITEM_LIMIT),
	}

	return fb, nil
}

func (fb *FirehoseBatcher) Add(msg []byte) error {
	if len(msg) > PER_ITEM_SIZE_LIMIT {
		return errors.New("item exceeds firehose's max item size")
	}

	fb.inputBuffer <- msg
	return nil
}

// AddFromChan is a convenience wrapper around Add that just keeps adding until an error is encountered
func (fb *FirehoseBatcher) AddFromChan(c chan []byte) error {
	for msg := range c {
		if err := fb.Add(msg); err != nil {
			return errors.Wrap(err, "failed to add record to batcher")
		}
	}

	return nil
}

func (fb *FirehoseBatcher) Start(streamName string) error {
	var overflow *firehose.Record
	for stop := false; !stop; {
		batch := make([]*firehose.Record, 0, BATCH_ITEM_LIMIT)
		batchSize := 0

		if overflow == nil {
			batch = append(batch, &firehose.Record{Data: <-fb.inputBuffer})
		} else {
			batch = append(batch, overflow)
			overflow = nil
		}

	BatchingLoop:
		for i := 0; i < BATCH_ITEM_LIMIT-1; i++ {
			select {
			case <-time.After(fb.maxSendInterval):
				break BatchingLoop

			case item, ok := <-fb.inputBuffer:
				if !ok {
					// Input channel is closed, we're done here exit the batching loop, send the last batch, and return
					stop = true
					break BatchingLoop
				}

				record := &firehose.Record{Data: item}
				if batchSize+len(item) > BATCH_SIZE_LIMIT {
					// We're gonna overflow a batch, save the current one and end the batch early
					overflow = record
					break BatchingLoop
				}

				batch = append(batch, record)
				batchSize += len(item)
			}
		}

		if err := fb.sendBatch(streamName, batch); err != nil {
			return errors.Wrap(err, "error sending batch")
		}
	}

	return nil
}

func (fb *FirehoseBatcher) sendBatch(streamName string, batch []*firehose.Record) error {
	prbo, err := fb.firehoseClient.PutRecordBatch(&firehose.PutRecordBatchInput{
		DeliveryStreamName: &streamName,

		Records: batch,
	})

	if err != nil {
		return err
	} else if *prbo.FailedPutCount > 0 {
		return errors.Errorf(
			"failed to send the full batch (%d failed), retries not currently handled",
			*prbo.FailedPutCount,
		)
	}

	return nil
}
