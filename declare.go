package amqpx

import (
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeDirect  = amqp.ExchangeDirect
	ExchangeFanout  = amqp.ExchangeFanout
	ExchangeTopic   = amqp.ExchangeTopic
	ExchangeHeaders = amqp.ExchangeHeaders
)

// A Declaration defined a declaration of a Queue/Exchange creation and/or binding.
type Declaration interface {
	fmt.Stringer
	slog.LogValuer
	declare(ch *amqp.Channel) error
}

var (
	_ Declaration = (*ExchangeDeclaration)(nil)
	_ Declaration = (*ExchangeBinding)(nil)
	_ Declaration = (*QueueDeclaration)(nil)
	_ Declaration = (*QueueBinding)(nil)
)

// ExchangeDeclare is a [Declaration] that declares an exchange on the server.
// If the exchange does not already exist, the server will create it. If the
// exchange exists, the server verifies that it is of the provided type,
// durability and auto-delete flags.
//
// See: [amqp.Channel.ExchangeDeclare] for more information.
type ExchangeDeclaration struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

func (d *ExchangeDeclaration) declare(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(
		d.Name,
		d.Kind,
		d.Durable,
		d.AutoDelete,
		d.Internal,
		d.NoWait,
		d.Args,
	)
}

func (d ExchangeDeclaration) String() string {
	return fmt.Sprintf("Exchange.Declare(name=%q, kind=%q, durable=%T, autoDelete=%T, internal=%T, noWait=%T, args=%v)",
		d.Name, d.Kind, d.Durable, d.AutoDelete, d.Internal, d.NoWait, d.Args)
}

func (d ExchangeDeclaration) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("type", "Exchange.Declare"),
		slog.String("exchange_name", d.Name),
		slog.String("kind", d.Name),
		slog.Bool("durable", d.Durable),
		slog.Bool("auto_delete", d.AutoDelete),
		slog.Bool("internal", d.Internal),
		slog.Bool("nowait", d.NoWait),
		slog.Any("args", d.Args),
	)
}

// ExchangeBinding is a [Declaration] that binds an exchange to another exchange
// to create inter-exchange routing topologies on the server. This can decouple
// the private topology and routing exchanges from exchanges intended solely for
// publishing endpoints.
//
// See: [amqp.Channel.ExchangeBind] for more information.
type ExchangeBinding struct {
	Destination string
	Key         string
	Source      string
	NoWait      bool
	Args        amqp.Table
}

func (d *ExchangeBinding) declare(ch *amqp.Channel) error {
	return ch.ExchangeBind(
		d.Destination,
		d.Key,
		d.Source,
		d.NoWait,
		d.Args,
	)
}

func (d ExchangeBinding) String() string {
	return fmt.Sprintf("Exchange.Bind(dst=%q, key=%q, src=%T, noWait=%T, args=%v)",
		d.Destination, d.Key, d.Source, d.NoWait, d.Args)
}

func (d ExchangeBinding) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("type", "Exchange.Bind"),
		slog.String("dst_exchange", d.Destination),
		slog.String("key", d.Key),
		slog.String("src_exchange", d.Source),
		slog.Bool("nowait", d.NoWait),
		slog.Any("args", d.Args),
	)
}

// QueueDeclaration is a [Declaration] that declares a queue to hold messages
// and deliver to consumers. Declaring creates a queue if it doesn't already
// exist, or ensures that an existing queue matches the same parameters.
//
// See: [amqp.Channel.QueueDeclare] for more information.
type QueueDeclaration struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

func (d *QueueDeclaration) declare(ch *amqp.Channel) error {
	// TODO: capture the queue name to deliver back up to support dynamic queue names
	_, err := ch.QueueDeclare(
		d.Name,
		d.Durable,
		d.AutoDelete,
		d.Exclusive,
		d.NoWait,
		d.Args,
	)

	return err
}

func (d QueueDeclaration) String() string {
	return fmt.Sprintf("Queue.Declare(name=%q, durable=%T, autoDelete=%T, exclusive=%T, noWait=%T, args=%v)",
		d.Name, d.Durable, d.AutoDelete, d.Exclusive, d.NoWait, d.Args)
}

func (d QueueDeclaration) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("type", "Queue.Declare"),
		slog.String("exchange_name", d.Name),
		slog.String("kind", d.Name),
		slog.Bool("durable", d.Durable),
		slog.Bool("auto_delete", d.AutoDelete),
		slog.Bool("exclusive", d.Exclusive),
		slog.Bool("nowait", d.NoWait),
		slog.Any("args", d.Args),
	)
}

// QueueBinding is a [Declaration] that binds an exchange to a queue so that
// publishings to the exchange will be routed to the queue when the publishing
// routing key matches the binding routing key.
//
// See: [amqp.Channel.QueueBind] for more information.
type QueueBinding struct {
	Queue    string
	Key      string
	Exchange string
	NoWait   bool
	Args     amqp.Table
}

func (d *QueueBinding) declare(ch *amqp.Channel) error {
	return ch.QueueBind(
		d.Queue,
		d.Key,
		d.Exchange,
		d.NoWait,
		d.Args,
	)
}

func (d QueueBinding) String() string {
	return fmt.Sprintf("Queue.Bind(queue=%q, key=%q, exchange=%q, noWait=%T, args=%v)",
		d.Queue, d.Key, d.Exchange, d.NoWait, d.Args)
}

func (d QueueBinding) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("type", "Exchange.Bind"),
		slog.String("queue", d.Queue),
		slog.String("key", d.Key),
		slog.String("exchange", d.Exchange),
		slog.Bool("nowait", d.NoWait),
		slog.Any("args", d.Args),
	)
}

// CommonOptions apply to both [Consumer]s and [Producer]s
type CommonOptions interface {
	ProducerOption
	ConsumerOption
}

type declarationOptions []Declaration

// WithDeclaration defined one or more rabbitmq declarations to be made when
// creating a Producer or Consumer and redeclared when the connection is
// re-established.
func WithDeclaration(declarations ...Declaration) CommonOptions {
	return declarationOptions(declarations)
}
func (o declarationOptions) applyProducer(p *producerConfig) { p.decls = append(p.decls, o...) }
func (o declarationOptions) applyConsumer(p *consumerConfig) { p.decls = append(p.decls, o...) }
