package amqpx_test

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"

	amqpx "github.com/nemith/amqpx"
	amqp "github.com/rabbitmq/amqp091-go"
)

func ExampleProducer() {
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
}

func ExampleConsumer() {
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
}
