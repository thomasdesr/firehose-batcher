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
