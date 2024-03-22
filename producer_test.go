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

func TestPublish(t *testing.T) {
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

	producer, err := conn.NewProducer(ctx,
		WithDeclaration(
			&QueueDeclaration{
				Name: "my-queue",
			},
		),
	)
	assert.NoError(t, err)

	msg := amqp.Publishing{Body: []byte("Hello, World!")}
	// publish directly to the queue using the default exchange (since we didn't define one)
	err = producer.Publish(ctx, "my-queue", msg)
	assert.NoError(t, err)

	// verify message was published
	gotMsg := readMessage(t, uri, "my-queue")
	assert.Equal(t, msg.Body, gotMsg.Body)
}

func TestPublishReconnect(t *testing.T) {
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

	producer, err := conn.NewProducer(ctx,
		WithDeclaration(
			&QueueDeclaration{
				Name: "my-queue",
			},
		),
	)
	assert.NoError(t, err)

	err = producer.Publish(ctx, "my-queue", amqp.Publishing{Body: []byte("Hello, World!")})
	assert.NoError(t, err)

	restartSig := conn.connected

	// flap the connection
	t.Log("stopping rabbitmq")
	_, _, err = container.Exec(ctx, []string{"rabbitmqctl", "stop_app"})
	assert.NoError(t, err)

	// Should error without a server
	err = producer.Publish(ctx, "my-queue", amqp.Publishing{Body: []byte("Hello, World!")})
	assert.Error(t, err)

	t.Log("restarting rabbitmq")
	_, _, err = container.Exec(ctx, []string{"rabbitmqctl", "start_app"})
	assert.NoError(t, err)

	select {
	case <-restartSig:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for reconnect")
	}

	err = producer.Publish(ctx, "my-queue", amqp.Publishing{Body: []byte("Hello, World!")})
	assert.NoError(t, err)
}

func readMessage(t *testing.T, uri, queue string) amqp.Delivery {
	t.Helper()

	conn, err := amqp.Dial(uri)
	require.NoError(t, err)

	ch, err := conn.Channel()
	require.NoError(t, err)

	msg, ok, err := ch.Get(queue, true)
	require.NoError(t, err)
	require.True(t, ok, "no message found in queue")

	return msg
}
