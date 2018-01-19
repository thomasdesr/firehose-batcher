package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/pkg/errors"
	"github.com/thomaso-mirodin/tailer"
)

var (
	path              = flag.String("path", "-", "What file to tail")
	kinesisStreamName = flag.String("stream_name", "", "What kinesis firehose stream to write to")

	input io.Reader = nil
)

func init() {
	flag.Parse()

	if *path == "-" {
		input = os.Stdin
	} else {
		var err error
		input, err = tailer.NewFile(
			*path,
			tailer.SetBufferSize(4*1024*1024),
		)
		if err != nil {
			log.Fatal(errors.Wrap(err, "failed to create tailer"))
		}
	}

	if *kinesisStreamName == "" {
		log.Println("No stream name provided")
		flag.Usage()
		os.Exit(1)
	}
}

func bufferedLineSplitter(in io.Reader) chan []byte {
	scanner := bufio.NewScanner(in)
	out := make(chan []byte)

	go func() {
		defer close(out)

		for scanner.Scan() {
			rawBytes := scanner.Bytes()

			buf := make([]byte, len(rawBytes))
			copy(buf, rawBytes)

			out <- buf
		}

		switch err := scanner.Err(); err {
		case nil, io.EOF:
			return
		default:
			log.Println(errors.Wrap(err, "scanner returned error"))
		}
	}()

	return out
}

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

func (fb *FirehoseBatcher) Start(streamName string) error {
	var overflow *firehose.Record
	for stop := false; !stop; {
		buf := make([]*firehose.Record, 0, BATCH_ITEM_LIMIT)
		bufSize := 0

		if overflow == nil {
			buf = append(buf, &firehose.Record{Data: <-fb.inputBuffer})
		} else {
			buf = append(buf, overflow)
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
				if bufSize+len(item) > BATCH_SIZE_LIMIT {
					// We're gonna overflow a batch, save the current one and end the batch early
					overflow = record
					break BatchingLoop
				}

				buf = append(buf, record)
				bufSize += len(item)
			}
		}

		if err := fb.sendBatch(streamName, buf); err != nil {
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

func main() {
	firehoseClient := firehose.New(session.Must(session.NewSession()))

	firehoseStreamName := "osquery-fleet-test"

	batcher, err := NewFirehoseBatcher(firehoseClient, time.Second*60)
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to create firehose batcher"))
	}

	go func() {
		err := batcher.Start(firehoseStreamName)
		if err != nil {
			log.Fatal(errors.Wrap(err, "batch sending exited early"))
		}
	}()

	lines := bufferedLineSplitter(input)
	for line := range lines {
		if err := batcher.Add(line); err != nil {
			log.Println(errors.Wrap(err, "failed to add record to batcher"))
		}

		fmt.Print(".")
	}
}
