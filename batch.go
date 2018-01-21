package firehosebatcher

import (
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/pkg/errors"
)

var (
	ErrBatchSizeOverflow   = errors.New("batch size overflow")
	ErrBatchLengthOverflow = errors.New("batch length overflow")
)

// Batch is really just a wrapper around a slice of `firehose.Record`s that tracks the size and length to make sure we don't create a batch that can't be sent to Firehose.
type Batch struct {
	size     int
	contents []*firehose.Record
}

// NewBatch construct a batch with an intializing record
func NewBatch(r *firehose.Record) *Batch {
	b := new(Batch)
	b.Add(r)
	return b
}

// Add attempts to add a record to the batch. If adding the record would cause either the batch's total size or total length to exceed AWS API limits this will return an appropriate error.
func (b *Batch) Add(r *firehose.Record) error {
	if b.contents == nil {
		b.contents = make([]*firehose.Record, 0, BATCH_ITEM_LIMIT)
	}

	rSize := len(r.Data)
	if b.size+rSize > BATCH_SIZE_LIMIT {
		return ErrBatchSizeOverflow
	}

	if b.Length()+1 > BATCH_ITEM_LIMIT {
		return ErrBatchLengthOverflow
	}

	b.contents = append(b.contents, r)
	b.size += rSize
	return nil
}

// Length return the number of records in the batch
func (b *Batch) Length() int {
	return len(b.contents)
}

// Send calls firehose.PutRecordBatch with the given batch. It does not current handle or retry on any sort of failure. This can cause unrecoverable message drops.
func (b *Batch) Send(client *firehose.Firehose, streamName string) error {
	prbo, err := client.PutRecordBatch(&firehose.PutRecordBatchInput{
		DeliveryStreamName: &streamName,

		Records: b.contents,
	})
	if err != nil {
		return err
	}

	if *prbo.FailedPutCount > 0 {
		return errors.Errorf(
			"failed to send the full batch (%d failed), retries not currently handled",
			*prbo.FailedPutCount,
		)
	}

	return nil
}
