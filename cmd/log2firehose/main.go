package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	flags "github.com/jessevdk/go-flags"
	"github.com/pkg/errors"
	firehosebatcher "github.com/thomaso-mirodin/firehose-batcher"
	"github.com/thomaso-mirodin/tailer"
)

type Flags struct {
	Path string `short:"f" long:"filename" default:"-" description:"Path to file to tail, defaults to stdin"`
	Args struct {
		FirehoseStreamName string `required:"true" positional-arg-name:"firehose_stream_name"`
	} `positional-args:"true"`

	cfg struct {
		source   io.Reader
		firehose *firehose.Firehose
	}
}

func ParseFlags() (*Flags, error) {
	f := new(Flags)
	_, err := flags.Parse(f)
	if err != nil {
		return nil, err
	}

	switch f.Path {
	case "-":
		f.cfg.source = os.Stdin
	default:
		var err error
		f.cfg.source, err = tailer.NewFile(
			f.Path,
			tailer.SetBufferSize(4*1024*1024),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create tailer")
		}
	}

	f.cfg.firehose = firehose.New(session.Must(session.NewSession()))

	return f, nil
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
	f, err := ParseFlags()
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to parse flags"))
	}

	batcher, err := firehosebatcher.New(f.cfg.firehose, time.Second*60)
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to create firehose batcher"))
	}

	go func() {
		err := batcher.Start(f.Args.FirehoseStreamName)
		if err != nil {
			log.Fatal(errors.Wrap(err, "batch sending exited early"))
		}
	}()

	lines := bufferedLineSplitter(f.cfg.source)
	if err := batcher.AddRawFromChan(lines); err != nil {
		log.Println(errors.Wrap(err, "adding lines failed"))
	}
}
