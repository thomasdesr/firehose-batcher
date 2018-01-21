package firehosebatcher

import (
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/pkg/errors"
)

var (
	ErrSizeOverflow   = errors.New("batch size overflow")
	ErrLengthOverflow = errors.New("batch length overflow")
)

type Batch struct {
	size     int
	contents []*firehose.Record
}

func NewBatch(r *firehose.Record) *Batch {
	b := &Batch{contents: make([]*firehose.Record, 0, BATCH_ITEM_LIMIT)}
	b.Add(r)
	return b
}

func (b *Batch) Add(r *firehose.Record) error {
	rSize := len(r.Data)

	if b.size+rSize > BATCH_SIZE_LIMIT {
		return ErrSizeOverflow
	}
	if b.Length()+1 > BATCH_ITEM_LIMIT {
		return ErrLengthOverflow
	}

	b.contents = append(b.contents, r)
	b.size += rSize
	return nil
}

func (b *Batch) Length() int {
	return len(b.contents)
}

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
