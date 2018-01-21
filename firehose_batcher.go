package firehosebatcher

import (
	"time"

	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/pkg/errors"
)

// FirehoseBatcher is a wrapper around a firehose client that makes it easier to send data to firehose using the PutRecordBatch API. It'll buffer data internally to construct batches.
type FirehoseBatcher struct {
	maxSendInterval time.Duration

	firehoseClient *firehose.Firehose

	inputBuffer     chan []byte
	batchSendBuffer chan *Batch

	closed bool
}

// New constructs a FirehoseBatcher that will send batches to Firehose whenever either a batch is full (size or length) or every interval.
func New(fc *firehose.Firehose, sendInterval time.Duration) (*FirehoseBatcher, error) {
	fb := &FirehoseBatcher{
		maxSendInterval: sendInterval,
		firehoseClient:  fc,

		inputBuffer:     make(chan []byte, BATCH_ITEM_LIMIT),
		batchSendBuffer: make(chan *Batch), // TODO(@thomas): Consider making this an actual buffer when we can run multiple senders
	}

	return fb, nil
}

// AddRaw takes a byte buffer to send to Firehose. It will return an error if the size of msg exceeds the max allowed item size (see limits.go). Will block if the send buffers are full.
func (fb *FirehoseBatcher) Add(msg []byte) error {
	if len(msg) > PER_ITEM_SIZE_LIMIT {
		return errors.New("item exceeds firehose's max item size")
	}

	fb.inputBuffer <- msg
	return nil
}

// AddFromChan is a convenience wrapper around Add that just keeps adding records until an error occurs.
func (fb *FirehoseBatcher) AddFromChan(c chan []byte) error {
	for msg := range c {
		if err := fb.Add(msg); err != nil {
			return errors.Wrap(err, "failed to add record to batcher")
		}
	}

	return nil
}

func (fb *FirehoseBatcher) startBatching() {
	defer close(fb.batchSendBuffer)

	for {
		batch := NewBatch(&firehose.Record{Data: <-fb.inputBuffer})

	BatchingLoop:
		for batch.Length() < BATCH_ITEM_LIMIT {
			select {
			case <-time.After(fb.maxSendInterval):
				break BatchingLoop
			case b, ok := <-fb.inputBuffer:
				if !ok {
					// Input channel is closed, we're done here send the last batch and return
					fb.batchSendBuffer <- batch
					return
				}

				record := &firehose.Record{Data: b}

				switch err := batch.Add(record); err {
				case nil:
					// Noop
				case ErrBatchSizeOverflow, ErrBatchLengthOverflow:
					// Send the batch along and restart with the overflowing record
					fb.batchSendBuffer <- batch
					batch = NewBatch(record)
				default:
					panic("Unknown error from batch construction")
				}
			}

			fb.batchSendBuffer <- batch
		}
	}
}

func (fb *FirehoseBatcher) sendBatches(streamName string) error {
	for batch := range fb.batchSendBuffer {
		err := batch.Send(fb.firehoseClient, streamName)
		// TODO(@thomas): retry logic here :D
		if err != nil {
			return errors.Wrap(err, "error sending batch")
		}
	}

	return nil
}

// Start creating batches and sending data to the provided Firehose Stream.
func (fb *FirehoseBatcher) Start(streamName string) error {
	go fb.startBatching()
	return fb.sendBatches(streamName)
}
