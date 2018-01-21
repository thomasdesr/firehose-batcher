package firehosebatcher

// Batch limit taken from https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html
const (
	BATCH_ITEM_LIMIT = 500
	BATCH_SIZE_LIMIT = 4 * 1024 * 1024 // 4 MB

	PER_ITEM_SIZE_LIMIT = 1024 * 1024 // 1 MB
)
