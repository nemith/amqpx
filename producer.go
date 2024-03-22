package amqpx

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	producedMsgs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "amqpx_produced_messages_count",
		Help: "The total number of produced messages",
	}, []string{"exchange", "key"})

	producerErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "amqpx_producer_errors_count",
		Help: "The total number of producer errors",
	}, []string{"exchange", "key", "error_type"})

	producerMsgSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "amqpx_producer_msg_size_bytes",
		Help:    "The size of messages published by the producer",
		Buckets: sizeBuckets,
	}, []string{"exchange", "key"})
)

type producerConfig struct {
	exchange string
	decls    []Declaration
}

type Producer struct {
	ch     *channelDelegate
	cfg    *producerConfig
	logger *slog.Logger
}

func (c *Client) NewProducer(ctx context.Context, opts ...ProducerOption) (*Producer, error) {
	cfg := producerConfig{}
	for _, opt := range opts {
		opt.applyProducer(&cfg)
	}

	p := Producer{
		cfg: &cfg,
		logger: c.logger.With(
			"amqp_channel_type", "producer",
			"exchange", cfg.exchange,
		),
	}

	ch, err := c.newChannel(ctx, p.init)
	if err != nil {
		return nil, err
	}
	p.ch = ch

	return &p, nil
}

func (p *Producer) init(ch *amqp.Channel) error {
	for _, decl := range p.cfg.decls {
		p.logger.Debug("initializing declarable", "declaration", decl)
		if err := decl.declare(ch); err != nil {
			return fmt.Errorf("failed to declare %T: %w", decl, err)
		}
	}
	return nil
}

// Publish will send a new message with the given key.  This function will
// return with an error when the connection is down, however the underlying
// connection will still reconnect to prevent from sending stale messages.
//
// See [amqp.Channel.PublishContext] for more information.
func (p *Producer) Publish(ctx context.Context, key string, msg amqp.Publishing) error {
	err := p.ch.call(ctx, false, func(ch *amqp.Channel) error {
		return ch.PublishWithContext(
			ctx,
			p.cfg.exchange,
			key,
			false, /* mandatory */
			false, /* immediate */
			msg,
		)
	})
	if err != nil {
		producerErrors.WithLabelValues(p.cfg.exchange, key, "publish").Inc()
		return fmt.Errorf("failed to publish message: %w", err)
	}

	producerMsgSize.WithLabelValues(p.cfg.exchange, key).Observe(float64(len(msg.Body)))
	producedMsgs.WithLabelValues(p.cfg.exchange, key).Inc()
	return nil
}

// PublishJSON will publish a message with sane defaults.  The body set to the
// JSON encoding of v. ContentType will be set to "application/json" and the
// timestamp will be set to the current time.
func (p *Producer) PublishJSON(ctx context.Context, key string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		producerErrors.WithLabelValues(p.cfg.exchange, key, "json_encode").Inc()
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	return p.Publish(ctx, key, amqp.Publishing{
		Timestamp:   time.Now(),
		ContentType: "application/json",
		Body:        body,
	})
}

func (p *Producer) Close() error {
	// TODO: Make sure publish is done via NotifyPublish?
	return p.ch.close()
}

type ProducerOption interface {
	applyProducer(*producerConfig)
}

type exchangeOption string

func WithExchange(name string) ProducerOption            { return exchangeOption(name) }
func (e exchangeOption) applyProducer(p *producerConfig) { p.exchange = string(e) }
