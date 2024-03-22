package amqpx

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"go.uber.org/goleak"
)

func TestConsume(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx := context.Background()

	container, err := rabbitmq.RunContainer(ctx)
	require.NoError(t, err)
	defer func() {
		err := container.Terminate(ctx)
		assert.NoError(t, err)
	}()

	uri, err := container.AmqpURL(ctx)
	require.NoError(t, err)

	conn, err := NewClient(uri)
	assert.NoError(t, err)
	defer func() {
		err := conn.Close()
		assert.NoError(t, err)
	}()

	consumer, err := conn.NewConsumer(ctx,
		WithDeclaration(
			&QueueDeclaration{
				Name: "my-queue",
			},
		),
		WithQueue("my-queue"),
		WithAutoAck(true),
	)
	assert.NoError(t, err)
	defer func() {
		err := consumer.Close()
		assert.NoError(t, err)
	}()

	publishMessage(t, uri, "my-queue", "Hello, World!")

	// publish directly to the queue using the default exchange (since we didn't define one)
	msgs, err := consumer.Consume(ctx)
	assert.NoError(t, err)

	select {
	case msg := <-msgs:
		assert.Equal(t, "Hello, World!", string(msg.Body))
	case <-time.After(5 * time.Second):
		assert.Fail(t, "expected message, got none")
	}
}

func TestConsumeReconnect(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx := context.Background()

	container, err := rabbitmq.RunContainer(ctx)
	require.NoError(t, err)
	defer func() {
		err := container.Terminate(ctx)
		assert.NoError(t, err)
	}()

	uri, err := container.AmqpURL(ctx)
	require.NoError(t, err)

	conn, err := NewClient(uri)
	assert.NoError(t, err)
	defer func() {
		err := conn.Close()
		assert.NoError(t, err)
	}()

	consumer, err := conn.NewConsumer(ctx,
		WithDeclaration(
			&QueueDeclaration{
				Name: "my-queue",
			},
		),
		WithQueue("my-queue"),
		WithAutoAck(true),
	)
	assert.NoError(t, err)
	defer func() {
		err := consumer.Close()
		assert.NoError(t, err)
	}()

	// publish directly to the queue using the default exchange (since we didn't define one)
	msgs, err := consumer.Consume(ctx)
	assert.NoError(t, err)

	publishMessage(t, uri, "my-queue", "Msg1")
	publishMessage(t, uri, "my-queue", "Msg2")

	select {
	case msg := <-msgs:
		assert.Equal(t, "Msg1", string(msg.Body))
	case <-time.After(5 * time.Second):
		assert.Fail(t, "expected message, got none")
	}

	// flap the connection
	_, _, err = container.Exec(ctx, []string{"rabbitmqctl", "stop_app"})
	assert.NoError(t, err)
	t.Log("restarting rabbitmq")
	_, _, err = container.Exec(ctx, []string{"rabbitmqctl", "start_app"})
	assert.NoError(t, err)

	select {
	case msg := <-msgs:
		assert.Equal(t, "Msg2", string(msg.Body))
	case <-time.After(5 * time.Second):
		assert.Fail(t, "expected message, got none")
	}
}

func publishMessage(t *testing.T, uri, queue, msg string) {
	t.Helper()

	conn, err := amqp.Dial(uri)
	require.NoError(t, err)

	ch, err := conn.Channel()
	require.NoError(t, err)

	amqpMsg := amqp.Publishing{Body: []byte(msg)}
	err = ch.PublishWithContext(context.Background(), "", queue, false, false, amqpMsg)
	require.NoError(t, err)
}
