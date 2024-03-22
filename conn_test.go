package amqpx

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"go.uber.org/goleak"
)

type fakeSRVResolver struct {
	addrs []*net.SRV
	err   error

	gotName string
}

func (f *fakeSRVResolver) LookupSRV(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
	f.gotName = name
	return "", f.addrs, f.err
}

var errFakeSRVError = errors.New("dns lookup failed yo")

func TestSRVResolver(t *testing.T) {
	tt := []struct {
		name     string
		svcName  string
		resolver srvResover
		want     []string
		err      error
	}{
		{
			name:    "noAddrs",
			svcName: "_amqp._tcp.example.com",
			resolver: &fakeSRVResolver{
				addrs: []*net.SRV{},
			},
			want: nil,
			err:  ErrNoSRVRecods,
		},
		{
			name:    "simple",
			svcName: "_amqp._tcp.example.com",
			resolver: &fakeSRVResolver{
				addrs: []*net.SRV{
					{Target: "foo1.example.com", Port: 5672},
					{Target: "foo2.example.com", Port: 5672},
				},
			},
			want: []string{
				"amqp://:@foo1.example.com",
				"amqp://:@foo2.example.com",
			},
			err: nil,
		},
		{
			name:     "resolverError",
			svcName:  "_amqp._tcp.example.com",
			resolver: &fakeSRVResolver{err: errFakeSRVError},
			want:     nil,
			err:      errFakeSRVError,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			r := NewSRVResolver(tc.svcName)
			r.resolver = tc.resolver
			got, err := r.Resolve(context.Background())
			assert.ErrorIs(t, err, tc.err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.svcName, tc.resolver.(*fakeSRVResolver).gotName)
		})
	}
}

func TestSRVResolverURI(t *testing.T) {
	r := NewSRVResolver("foo",
		WithUser("user"),
		WithPassword("pass"),
		WithVhost("vhost"),
		WithAMQPS(),
	)
	assert.Equal(t, "amqps://user:pass@:0/vhost", r.uriTemplate.String())
}

func TestDial(t *testing.T) {
	t.Run("activeServer", func(t *testing.T) {
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
		// We don't test if we are connected cause we probably aren't

		err = conn.Close()
		assert.NoError(t, err)
		assert.True(t, conn.IsClosed())
	})

	t.Run("serverAfterClient", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.Background()

		container, err := rabbitmq.RunContainer(ctx)
		require.NoError(t, err)
		defer func() {
			err := container.Terminate(ctx)
			assert.NoError(t, err)
		}()

		t.Log("stopping rabbitmq")
		_, _, err = container.Exec(ctx, []string{"rabbitmqctl", "stop_app"})
		assert.NoError(t, err)

		uri, err := container.AmqpURL(ctx)
		require.NoError(t, err)

		conn, err := NewClient(uri)
		assert.NoError(t, err)
		assert.True(t, conn.IsClosed())

		t.Log("restarting rabbitmq")
		_, _, err = container.Exec(ctx, []string{"rabbitmqctl", "start_app"})
		assert.NoError(t, err)

		t.Log("waiting for reconnect")
		time.Sleep(10 * time.Second)
		assert.False(t, conn.IsClosed())

		err = conn.Close()
		assert.NoError(t, err)
		assert.True(t, conn.IsClosed())
	})
}

func TestForceDial(t *testing.T) {
	t.Run("successfulConnection", func(t *testing.T) {
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

		conn, err := NewClient(uri, WithForceDial(ctx))
		assert.NoError(t, err)
		assert.False(t, conn.IsClosed())

		err = conn.Close()
		assert.NoError(t, err)
		assert.True(t, conn.IsClosed())
	})

	t.Run("failedConnection", func(t *testing.T) {
		ctx := context.Background()
		_, err := NewClient("127.127.127.127:9999", WithForceDial(ctx))
		assert.Error(t, err)
	})
}

func TestReconnect(t *testing.T) {
	defer func() {
		time.Sleep(1000 * time.Millisecond)
		goleak.VerifyNone(t)
	}()
	ctx := context.Background()

	container, err := rabbitmq.RunContainer(ctx)
	require.NoError(t, err)
	defer func() {
		err := container.Terminate(ctx)
		assert.NoError(t, err)
	}()

	uri, err := container.AmqpURL(ctx)
	require.NoError(t, err)

	conn, err := NewClient(uri, WithForceDial(ctx))
	assert.NoError(t, err)
	defer func() {
		err := conn.Close()
		assert.NoError(t, err)
	}()
	assert.False(t, conn.IsClosed())

	const (
		reconnectCycles  = 5
		reconnectTimeout = 10 * time.Second
	)
	for i := 0; i < reconnectCycles; i++ {
		// We need to grab this signal before we close to not get the original
		// signal and not one later
		restartSig := conn.connected

		// flap the connection
		t.Log("stopping rabbitmq")
		_, _, err = container.Exec(ctx, []string{"rabbitmqctl", "stop_app"})
		assert.NoError(t, err)
		t.Log("restarting rabbitmq")
		_, _, err = container.Exec(ctx, []string{"rabbitmqctl", "start_app"})
		assert.NoError(t, err)

		select {
		case <-restartSig:
		case <-time.After(reconnectTimeout):
			t.Fatal("timed out waiting for reconnect")
		}

		assert.False(t, conn.IsClosed())
	}
}

func TestCall(t *testing.T) {
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

	ch, err := conn.newChannel(ctx, nil)
	assert.NoError(t, err)

	t.Log("stopping rabbitmq")
	_, _, err = container.Exec(ctx, []string{"rabbitmqctl", "stop_app"})
	assert.NoError(t, err)

	// wait false, should fail as we are disconnected
	err = ch.call(ctx, false, func(ch *amqp.Channel) error {
		return ch.PublishWithContext(ctx, "exchange", "routing", false, false, amqp.Publishing{Body: []byte("msg")})
	})
	assert.Error(t, err)

	wait := make(chan struct{})
	go func() {
		// wait for reconnection
		err := ch.call(ctx, true, func(ch *amqp.Channel) error {
			return ch.PublishWithContext(ctx, "exchange", "routing", false, false, amqp.Publishing{Body: []byte("msg")})
		})
		assert.NoError(t, err)
		close(wait)
	}()

	t.Log("restarting rabbitmq")
	_, _, err = container.Exec(ctx, []string{"rabbitmqctl", "start_app"})
	assert.NoError(t, err)
	<-wait
}
