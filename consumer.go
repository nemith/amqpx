package amqpx

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	amqp "github.com/rabbitmq/amqp091-go"
)

var sizeBuckets = prometheus.ExponentialBuckets(100, 2.1, 20)

var (
	consumerCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "amqpx_consumer_count",
		Help: "The total number of consumers",
	}, []string{"consumer_name", "queue"})

	consumedMsgs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "amqpx_received_messages_count",
		Help: "The total number of received messages",
	}, []string{"consumer_name", "queue"})

	ackedMsgs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "amqpx_acked_messages_count",
		Help: "The total number of acked message requests. Does not include auto-aked messages",
	}, []string{"consumer_name", "queue"})

	nackedMsgs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "amqpx_nacked_messages_count",
		Help: "The total number of nacked message requests",
	}, []string{"consumer_name", "queue"})

	rejectedMsgs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "amqpx_rejected_messages_count",
		Help: "The total number of reject message requests",
	}, []string{"consumer_name", "queue"})

	consumerErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "amqpx_consumer_errors_count",
		Help: "The total number of consumer errors",
	}, []string{"consumer_name", "queue", "error_type"})

	consumerMsgSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "amqpx_consumer_msg_size_bytes",
		Help:    "The size of messages consumed by the consumer",
		Buckets: sizeBuckets,
	}, []string{"consumer_name", "queue"})
)

type consumerConfig struct {
	queue        string
	consumerName string
	decls        []Declaration
	autoAck      bool
	exclusive    bool
}

type consumerOutput struct {
	ctx context.Context
}

type Consumer struct {
	ch     *channelDelegate
	cfg    consumerConfig
	queue  string
	logger *slog.Logger

	mu      sync.Mutex
	outputs map[chan Delivery]consumerOutput
}

func (c *Client) NewConsumer(ctx context.Context, opts ...ConsumerOption) (*Consumer, error) {
	var cfg consumerConfig
	for _, opt := range opts {
		opt.applyConsumer(&cfg)
	}

	consumer := &Consumer{
		cfg:     cfg,
		queue:   cfg.queue,
		outputs: make(map[chan Delivery]consumerOutput),
		logger: c.logger.With(
			"amqp_channel_type", "consumer",
			"queue", cfg.queue,
		),
	}

	ch, err := c.newChannel(ctx, consumer.init)
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}
	consumer.ch = ch

	return consumer, nil
}

func (c *Consumer) init(ch *amqp.Channel) error {
	for _, decl := range c.cfg.decls {
		c.logger.Debug("initializing declarable", "declaration", decl)
		if err := decl.declare(ch); err != nil {
			return fmt.Errorf("failed to declare %T: %w", decl, err)
		}
	}

	c.logger.Debug("re-establishing consumer outputs")
	for outputCh, output := range c.outputs {
		if err := c.consume(output.ctx, ch, outputCh); err != nil {
			return err
		}
	}

	return nil
}

func (c *Consumer) consume(ctx context.Context, ch *amqp.Channel, output chan Delivery) error {
	incoming, err := ch.ConsumeWithContext(
		ctx,
		c.queue,
		c.cfg.consumerName,
		c.cfg.autoAck,
		c.cfg.exclusive,
		false, /* noLocal. not support on rabbitmq */
		false, /* noWait */
		nil,   /* args */
	)
	if err != nil {
		return err
	}
	consumerCount.WithLabelValues(c.cfg.consumerName, c.queue).Inc()

	// connect the output from the amqp channel (which can change when
	// reconnecting) to the output (which to the consumer is always the same).
	//
	// go routine safety: This **should** exit when the channel is closed and the
	// incommign channel is closed.
	go func() {
		defer func() {
			consumerCount.WithLabelValues(c.cfg.consumerName, c.queue).Dec()
		}()
		for {
			select {
			case <-ctx.Done():
				close(output)

				c.mu.Lock()
				delete(c.outputs, output)
				c.mu.Unlock()

				return
			case delivery, ok := <-incoming:
				if !ok {
					// We don't want to close the output channel here as we want
					// may want to reuse it for a reestablished connection. But
					// still close the goroutine.
					return
				}
				consumerMsgSize.WithLabelValues(c.cfg.consumerName, c.queue).Observe(float64(len(delivery.Body)))
				consumedMsgs.WithLabelValues(c.cfg.consumerName, c.queue).Inc()
				output <- Delivery{
					Delivery: delivery,
					consumer: c,
				}
			}
		}
	}()

	return nil
}

// Consume immediately starts delivering queued messages to the the returned channel.
//
// See [amqp.Channel.Consume] for more information.
func (c *Consumer) Consume(ctx context.Context) (<-chan Delivery, error) {
	output := make(chan Delivery)
	err := c.ch.call(ctx, true, func(ch *amqp.Channel) error {
		return c.consume(ctx, ch, output)
	})
	if err != nil {
		consumerErrors.WithLabelValues(c.cfg.consumerName, c.queue, "consume").Inc()
		return nil, err
	}

	// register the output channel so that it can we can reesblish it after a
	// reconnect on a new conn/chan.
	c.mu.Lock()
	defer c.mu.Unlock()
	c.outputs[output] = consumerOutput{ctx: ctx}

	return output, nil
}

/*
func (c *Consumer) Errs() error {}
*/

func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for output := range c.outputs {
		close(output)
	}

	return c.ch.close()
}

type ConsumerOption interface {
	applyConsumer(*consumerConfig)
}

type queueOption string

func WithQueue(name string) ConsumerOption            { return queueOption(name) }
func (o queueOption) applyConsumer(c *consumerConfig) { c.queue = string(o) }

type consumerNameOption string

func WithConsumerName(name string) ConsumerOption            { return consumerNameOption(name) }
func (o consumerNameOption) applyConsumer(c *consumerConfig) { c.consumerName = string(o) }

type consumerAutoAckOption bool

func WithAutoAck(v bool) ConsumerOption                         { return consumerAutoAckOption(v) }
func (o consumerAutoAckOption) applyConsumer(c *consumerConfig) { c.autoAck = bool(o) }

type consumerExclusiveOption bool

func WithExclusive(v bool) ConsumerOption                         { return consumerExclusiveOption(v) }
func (o consumerExclusiveOption) applyConsumer(c *consumerConfig) { c.exclusive = bool(o) }

// Delivery is a basic wrapper around the [amqp.Delivery] message.
type Delivery struct {
	// Hold a pointer to the consumer so that we can get the config details,
	// etc.
	consumer *Consumer

	amqp.Delivery
}

func (d Delivery) Ack(multiple bool) error {
	if err := d.Delivery.Ack(multiple); err != nil {
		consumerErrors.WithLabelValues(d.consumer.cfg.consumerName, d.consumer.queue, "ack").Inc()
		return err
	}
	ackedMsgs.WithLabelValues(d.consumer.cfg.consumerName, d.consumer.queue).Inc()
	return nil
}

// Nack negatively acknowledge the delivery of message(s) identified by the
// delivery tag from either the client or server. and wraps the
// [amqp.Delivery.Nack] method.
func (d Delivery) Nack(multiple, requeue bool) error {
	if err := d.Delivery.Nack(multiple, requeue); err != nil {
		consumerErrors.WithLabelValues(d.consumer.cfg.consumerName, d.consumer.queue, "nack").Inc()
		return err
	}
	nackedMsgs.WithLabelValues(d.consumer.cfg.consumerName, d.consumer.queue).Inc()
	return nil
}

// Reject delegates a negatively acknowledgement through the Acknowledger
// interface and wraps the [amqp.Delivery.Reject] method.
func (d Delivery) Reject(requeue bool) error {
	if err := d.Delivery.Reject(requeue); err != nil {
		consumerErrors.WithLabelValues(d.consumer.cfg.consumerName, d.consumer.queue, "reject").Inc()
		return err
	}
	rejectedMsgs.WithLabelValues(d.consumer.cfg.consumerName, d.consumer.queue).Inc()
	return nil
}

// JSONUnmarhsal will unmarshal the body of the delivery into the given value.
func (d Delivery) JSONUnmarshal(v any) error {
	err := json.Unmarshal(d.Body, v)
	if err != nil {
		consumerErrors.WithLabelValues(d.consumer.cfg.consumerName, d.consumer.queue, "json_unmarshal").Inc()
	}
	return err
}
