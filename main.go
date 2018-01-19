package main

import (
	"bufio"
	"flag"
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

func main() {
	firehoseClient := firehose.New(session.Must(session.NewSession()))

	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		buf := make([]byte, len(scanner.Bytes()))
		copy(buf, scanner.Bytes())

		fpro, err := firehoseClient.PutRecord(&firehose.PutRecordInput{
			DeliveryStreamName: kinesisStreamName,
			Record: &firehose.Record{
				Data: buf,
			},
		})
		if err != nil {
			log.Println(errors.Wrap(err, "failed to PutRecord"))
		}

		log.Println(fpro.String())
	}

	if scanner.Err() != nil {
		log.Fatal(errors.Wrap(scanner.Err(), "line scanner failed"))
	}
}
