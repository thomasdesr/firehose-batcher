package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

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
	BATCH_RECORD_ITEM_LIMIT = 500
	BATCH_RECORD_SIZE_LIMIT = 500
)

type recordBatch []*firehose.Record

func (rb *recordBatch) add(r *firehose.Record) {
	rb = append(rb, r)
}

func (rb *recordBatch) reset() recordBatch {
	rb = make([]*firehose.Record, 0, BATCH_RECORD_ITEM_LIMIT)
	return rb
}

func firehoseRecordBatcher(in chan []byte) chan []*firehose.Record {
	out = make(chan []*firehose.Record)

	recordBuf := make(recordBatch)
	recordBuf.reset()

	sizeCounter := 0

	go func() {
		for buf := range in {
			if sizeCounter+len(buf) > BATCH_RECORD_SIZE_LIMIT {
				out <- recordBuf
				recordBuf.reset()
			}

			recordBuf.add(&firehose.Record{
				Data: buf,
			})
		}
	}()

	return
}

func main() {
	firehoseClient := firehose.New(session.Must(session.NewSession()))

	lines := bufferedLineSplitter(input)

	for line := range lines {
		_, err := firehoseClient.PutRecord(&firehose.PutRecordInput{
			DeliveryStreamName: kinesisStreamName,
			Record: &firehose.Record{
				Data: line,
			},
		})
		if err != nil {
			log.Println(errors.Wrap(err, "failed to PutRecord"))
		}

		fmt.Print(".")
	}
}
