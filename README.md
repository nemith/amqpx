#amqpx

This library is a wrapper around the venerable [`github.com/rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go) library but adds support for cluster resolution and reconnection support along with some quality of life improvements for declaring queues, exchanges and bindings.

## Usage 

### Cluster connection
To connect to a cluster you can use a resolver `NewClusterClient` with a resolver.  A Resolver will resolve a ordered list of hosts to connect to.  There are two built in resolvers:

* [StaticResolver] Takes a list of URIs to connect to and will connect to them either in order or randomly based on the options provided.   
* [SRVResolver] Will use DNS SRV records to get a list of servers to use.  This works well with Consul Service Discovery, for exammple.

Resolvers are refreshed on reconnections.

Additional resolvers can be created by implementing the `Resolver` interfaces to plug into additions service discovery mechanisms.

Note that connections are lazy.  The client returned will not attempt to connect (or resolve) until a operation is requested.  To connect immediatly use the `WithForceDial` option.

```go
import "github.com/nemith/amqpx"

func main() {
    resolver := amqpx.NewStaticResolver([]string{"amqp://host1", "amqp://host2"}, true)
    client, err := amqpx.NewClusterClient(resolver)
    if err != nil {
        log.Fatalf("failed to create client: %v", err)
    }
    // Create a consumer or producer 

}
```

### Consumer
To enable a consumer you create a  a new consumer with `NewConsumer`.  During this time you define the queues, exchanges and bindings between them.   This done as part of constructing the consumer so that they are redecleared on a reconnection if needed.

```go
	ctx := context.Background()
	client, err := amqpx.NewClient("amqp://localhost:5672")
	if err != nil {
		slog.Error("failed to create client", "err", err)
	}
	defer client.Close()

	consumer, err := client.NewConsumer(ctx,
		amqpx.WithQueue("my-queue"),
		amqpx.WithAutoAck(true),
		amqpx.WithDeclaration(
			&amqpx.ExchangeDeclaration{
				Name: "my-exchange",
				Kind: "direct",
			},
			&amqpx.QueueDeclaration{Name: "my-queue"},
			&amqpx.QueueBinding{
				Queue:    "my-queue",
				Exchange: "my-exchange",
				Key:      "key",
			},
		),
	)
	if err != nil {
		slog.Error("failed to create consumer", "err", err)
		return
	}

	defer consumer.Close()

	msgs, err := consumer.Consume(ctx)
	if err != nil {
		slog.Error("failed to consume msgs", "err", err)
		return
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

Loop:
	for {
		select {
		case msg := <-msgs:
			log.Printf("received msg: %s", msg.Body)
		case <-stop:
			break Loop
		}
	}
```

### Producer
Producers are created similarly as consimers with the `NewProducer` method.  Like consumers producers can define exchanges 

```go
	ctx := context.Background()
	client, err := amqpx.NewClient("amqp://localhost:5672")
	if err != nil {
		slog.Error("failed to create client", "err", err)
	}
	defer client.Close()

	producer, err := client.NewProducer(ctx,
		amqpx.WithExchange("my-exchange"),
		amqpx.WithDeclaration(
			&amqpx.ExchangeDeclaration{
				Name: "my-exchange",
				Kind: "direct",
			},
		),
	)
	if err != nil {
		slog.Error("failed to create producer", "err", err)
		return
	}
	defer producer.Close()

	msg := amqp.Publishing{
		Body: []byte("Hello, World!"),
	}
	if err := producer.Publish(context.Background(), "key", msg); err != nil {
		slog.Error("failed to publish msg", "err", err)
		return
	}
```