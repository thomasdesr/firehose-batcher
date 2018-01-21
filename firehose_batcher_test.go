package firehosebatcher

import (
	"bytes"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/stretchr/testify/assert"
)

func dataToBatch(msgs [][]byte) *Batch {
	b := new(Batch)

	for _, m := range msgs {
		err := b.Add(&firehose.Record{Data: m})
		if err != nil {
			panic(err)
		}
	}

	return b
}

// Compare the results of startBatching to what a simple for loop + slice would generate
func TestBatching(t *testing.T) {
	tests := []struct {
		name string
		desc string

		data        [][]byte
		sendTimeout time.Duration

		expectedBatches []*Batch
	}{
		{
			name: "Short batch, should timeout",
			desc: `Send a small amount of data in, expect a small amount of data out. The batch sending should be triggered by the send timeout.`,

			sendTimeout: time.Millisecond * 5,
			data: [][]byte{
				[]byte("abcd"),
				[]byte("efgh"),
			},
			expectedBatches: []*Batch{
				dataToBatch([][]byte{
					[]byte("abcd"),
					[]byte("efgh"),
				}),
			},
		},
		{
			name: "Oversized batch (length)",
			desc: `This tests the case in which we get too many individual records. This should send two batches; the first with len(records == BATCH_ITEM_LIMIT, and a second with records == 1`,

			sendTimeout: time.Millisecond * 5,
			data: bytes.Split(
				bytes.Repeat([]byte{0}, BATCH_ITEM_LIMIT+1),
				[]byte{},
			),
			expectedBatches: []*Batch{
				dataToBatch(
					bytes.Split(
						bytes.Repeat([]byte{0}, BATCH_ITEM_LIMIT),
						[]byte{},
					),
				),
				dataToBatch([][]byte{[]byte{0}}),
			},
		},
		{
			name: "Oversized batch (size)",
			desc: `This tests the case in which we get too much data and need to split into multiple batches because the data volume is too large. The first record should have sum(len(record) for record in batch) == BATCH_SIZE_LIMIT, and a second with sum(len(record) for record in batch) == 1`,

			sendTimeout: time.Millisecond * 5,
			data: [][]byte{
				bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8), // 1
				bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8), // 2
				bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8), // 3
				bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8), // 4
				bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8), // 5
				bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8), // 6
				bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8), // 7
				bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8), // 8
				[]byte{0},
			},
			expectedBatches: []*Batch{
				dataToBatch([][]byte{
					bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8),
					bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8),
					bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8),
					bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8),
					bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8),
					bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8),
					bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8),
					bytes.Repeat([]byte{0}, BATCH_SIZE_LIMIT/8),
				}),
				dataToBatch([][]byte{[]byte{0}}),
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)

			fb, _ := New(nil, test.sendTimeout)
			go fb.startBatching()

			go func() {
				for _, msg := range test.data {
					assert.Nil(fb.AddRaw(msg))
				}
			}()

			for _, expectedBatch := range test.expectedBatches {
				select {
				case batch := <-fb.batchSendBuffer:
					assert.Equal(expectedBatch.Length(), batch.Length(), "batch length")
					assert.Equal(expectedBatch.size, batch.size, "batch size")
					assert.Equal(expectedBatch, batch)
				case <-time.After(test.sendTimeout * 5):
					t.Error("5 * sendTime exceeded")
				}
			}

			// Make sure we don't end up with an extra batch because we messed up a test
			select {
			case <-fb.batchSendBuffer:
				t.Error("Unexpected batch! Recieved an unexpected batch after the end of the test.")
			case <-time.After(test.sendTimeout):
				// noop, this is what should happen
			}
		})
	}
}
