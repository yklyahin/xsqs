# XSQS - AWS SQS Consumer
![Coverage](https://img.shields.io/badge/Coverage-86.0%25-brightgreen)

XSQS is a powerful Go library that simplifies and enhances the process of consuming messages from Amazon Simple Queue Service (SQS). With XSQS, you can seamlessly handle messages in an efficient and reliable manner, enabling you to focus on building robust applications.

## Features

- **Simplified Message Consumption:** XSQS abstracts away the complexities of working with SQS, allowing you to focus on processing messages rather than dealing with low-level details.
- **Error Handling Strategies:** Handle errors with ease using customizable backoff and retry mechanisms. Mark messages as unrecoverable when certain errors occur to prevent endless retries.
- **Flexible Processing Strategies:** Choose between sequential, parallel, or bulk processing strategies based on your application's needs. 
- **Middleware Support**: Enhance XSQS capabilities with middleware, enabling you to add custom logic before or after message processing. Create reusable components to address specific use cases.
- **Extended Deduplication**: Extend SQS FIFO queue deduplication with XSQS's built-in deduplication feature. Handle duplicate messages more effectively, complementing SQS's deduplication 5 minutes interval.

## Installation

To install XSQS, use the following command:

```shell
go get github.com/yklyahin/xsqs
```

Make sure to grant the necessary SQS policies:

```text
"sqs:ReceiveMessage"
"sqs:DeleteMessage"
"sqs:DeleteMessageBatch"
"sqs:ChangeMessageVisibility"
"sqs:ChangeMessageVisibilityBatch"
"sqs:GetQueueUrl"
```

## Usage

Here's an example of how to use XSQS to consume messages from an SQS queue:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/yklyahin/xsqs"
)

func main() {
	// Create an SQS client
	sqsClient := sqs.New(session.Must(session.NewSession()))
	// Create a XSQS client
	xsqsClient := xsqs.NewClient(sqsClient, "arn:aws:sqs:eu-west-1:100000000000:my-queue")
	// Create a message handler
	messageHandler := xsqs.HandlerFunc[sqs.Message](handleMessage)
	// Create a consumer
	consumer := xsqs.NewParallelConsumer(
		xsqsClient, 
		messageHandler, 
		xsqs.WithBackoff(xsqs.ExponentialBackoff(time.Hour * 5)),
	)
	// Create a worker with options
	worker := xsqs.NewWorker("my-worker", xsqsClient, consumer)
	// Start the worker in a separate goroutine
	go worker.Start(context.Background())
}

func handleMessage(ctx context.Context, message *sqs.Message) error {
	// Process the message
	fmt.Println("Received message:", *message.Body)
	// Simulate processing time
	time.Sleep(2 * time.Second)
	return nil
}
```

### Error handling

By default any of the XSQS consumers will keep retrying to process a message on any error.  
If a consumer returns `UnrecoverableError`, it indicates that the error cannot be retried, and the message will be deleted from the queue.

```go
import (
	"os"
	"fmt"

	"github.com/yklyahin/xsqs"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func handleMessage(ctx context.Context, message *sqs.Message) error {
	file, err := os.Open("filepath")
	if err != nil {
		// This message won't be retried
		return xsqs.UnrecoverableError(fmt.Errorf("failed to open the file: %w", err))
	}
	// Do something
	return nil
}
```

### Deduplication

XSQS introduces an advanced deduplication feature to prevent duplicate message processing. This capability complements SQS FIFO queue deduplication, particularly for scenarios involving duplicate messages arriving after the 5-minute interval.

## Examples

For more examples and detailed usage instructions, please refer to the [examples](examples) directory.

## Contributing

Contributions are welcome! If you encounter any issues or have suggestions for improvements, please [open an issue](https://github.com/yklyahin/xsqs/issues) on GitHub. Feel free to fork the repository and submit pull requests for any enhancements.

## License

XSQS is released under the [MIT License](LICENSE).
