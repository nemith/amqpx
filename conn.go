package amqpx

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrDialTimeout = errors.New("amqpx: dial timeout")
	ErrNoSRVRecods = errors.New("amqpx: no SRV records found")
	ErrConnClosed  = errors.New("amqpx: connection closed")
)

var (
	reconnectTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "amqpx_reconnect_total",
		Help: "The total number of amqp reconnects",
	})

	reconnectAttempts = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "amqpx_reconnect_attempts",
		Help: "The total number of amqp reconnect attempts. Resets on reconnect",
	})

	connectionFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "amqpx_connection_failures",
		Help: "The total number of amqp connection failures",
	}, []string{"server_node"})

	connectedState = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "amqpx_connected_state",
		Help: "The current state of the amqp connection",
	})
)

// ClusterResolver is an interface for resolving the cluster members.
type ClusterResolver interface {
	// Resolve will return a list of cluster members urls.
	Resolve(ctx context.Context) ([]string, error)
}

// StaticResolver implements a [ClusterResolver] that returns a static list of uris.
type StaticResolver struct {
	uris    []string
	shuffle bool
}

// NewStaticResolver returns a new static resolver with the given uris.  If
// shuffle is true then the uris will be randomized on each resolve call.
func NewStaticResolver(uris []string, shuffle bool) *StaticResolver {
	return &StaticResolver{
		uris:    uris,
		shuffle: shuffle,
	}
}

func (r *StaticResolver) Resolve(context.Context) ([]string, error) {
	uris := slices.Clone(r.uris)
	if r.shuffle {
		rand.Shuffle(len(uris), func(i, j int) {
			uris[i], uris[j] = uris[j], uris[i]
		})
	}
	return uris, nil
}

// srvResover is an interface for the SRV part of a net.Resolver
type srvResover interface {
	LookupSRV(ctx context.Context, service, proto, name string) (string, []*net.SRV, error)
}

// SRVResolver implements a [ClusterResolver] that resolves the cluster members
// using DNS SRV records.  The order that is returned are sorted by proirity and
// and then randomized with records of the same weight.
type SRVResolver struct {
	resolver    srvResover
	name        string
	uriTemplate amqp.URI
	started     bool
}

type SRVResolverOption interface {
	apply(*amqp.URI)
}

type srvResolverUser string

func WithUser(username string) SRVResolverOption { return srvResolverUser(username) }
func (o srvResolverUser) apply(r *amqp.URI)      { r.Username = string(o) }

type srvResolverPassword string

func WithPassword(password string) SRVResolverOption { return srvResolverPassword(password) }
func (o srvResolverPassword) apply(r *amqp.URI)      { r.Password = string(o) }

type srvResolverVhost string

func WithVhost(vhost string) SRVResolverOption { return srvResolverVhost(vhost) }
func (o srvResolverVhost) apply(r *amqp.URI)   { r.Vhost = string(o) }

type srvResolverAQMPSOption bool

func WithAMQPS() SRVResolverOption                 { return srvResolverAQMPSOption(true) }
func (o srvResolverAQMPSOption) apply(r *amqp.URI) { r.Scheme = "amqps" }

// NewSRVResolver returns a new SRV resolver for the given srv record name
//
//	resolver := NewSRVResolver("rabbitmq.service.consul")
//	resolver := NewSRVResolver("_aqmp._tcp.example.com")
func NewSRVResolver(name string, opts ...SRVResolverOption) *SRVResolver {
	uri := amqp.URI{Scheme: "amqp"}
	for _, opt := range opts {
		opt.apply(&uri)
	}

	return &SRVResolver{
		resolver:    &net.Resolver{},
		name:        name,
		uriTemplate: uri,
	}
}

func cloneURI(u amqp.URI) amqp.URI {
	return amqp.URI{
		Scheme:   u.Scheme,
		Host:     u.Host,
		Port:     u.Port,
		Username: u.Username,
		Password: u.Password,
		Vhost:    u.Vhost,
	}
}

func (r *SRVResolver) Resolve(ctx context.Context) ([]string, error) {
	r.started = true

	_, addrs, err := r.resolver.LookupSRV(ctx, "", "", r.name)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve SRV record: %w", err)
	}

	if len(addrs) == 0 {
		return nil, ErrNoSRVRecods
	}

	uris := make([]string, len(addrs))
	for i, addr := range addrs {
		uri := cloneURI(r.uriTemplate)
		uri.Host = addr.Target
		uri.Port = int(addr.Port)
		uris[i] = uri.String()
	}
	return uris, nil
}

type Client struct {
	resolver         ClusterResolver
	config           clientConfig
	logger           *slog.Logger
	reconnectBackoff backoff.BackOff

	mu   sync.Mutex
	conn *amqp.Connection

	channelMu sync.Mutex
	channels  map[*channelDelegate]*amqp.Channel
	connected chan struct{} // closed (brodcasted) when a reconnection happens

	close chan struct{}
}

// NewClient will create a new client to a RabbitMQ server defined by the aqmp URI.
//
// See https://www.rabbitmq.com/docs/uri-spec for more information on the URI
func NewClient(uri string, opts ...DialOption) (*Client, error) {
	return NewClusterClient(NewStaticResolver([]string{uri}, false), opts...)
}

// NewClusterClient will create a new client to multiple RabbitMQs in a cluster.
// The given resolver is used to resolve the cluster members and then a single
// cluster member is joined.
func NewClusterClient(resolver ClusterResolver, opts ...DialOption) (*Client, error) {
	config := clientConfig{
		dialTimeout:      30 * time.Second,
		reconnectBackoff: &backoff.ConstantBackOff{Interval: 5 * time.Second},
		logger:           slog.Default(),
	}
	for _, opt := range opts {
		opt.apply(&config)
	}

	c := &Client{
		resolver:         resolver,
		config:           config,
		logger:           config.logger,
		reconnectBackoff: config.reconnectBackoff,
		close:            make(chan struct{}),

		channels:  make(map[*channelDelegate]*amqp.Channel),
		connected: make(chan struct{}),
	}

	// Connect synchronously if required
	if config.forceDial {
		if err := c.dial(config.dialCtx); err != nil {
			return nil, fmt.Errorf("failed to dial: %w", err)
		}
	}
	go c.reconnectLoop()

	return c, nil
}

// dial will resolve a list of servers from the given resolver and try to
// connect to them one by one.  First successful connection is return or an
// error if all failed.
func dial(ctx context.Context, resolver ClusterResolver, cfg clientConfig) (*amqp.Connection, error) {
	uris, err := resolver.Resolve(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve cluster uris: %w", err)
	}

	//nolint: prealloc
	var errs []error
	for _, uri := range uris {
		u, err := amqp.ParseURI(uri)
		if err != nil {
			return nil, fmt.Errorf("bad URI: %w", err)
		}

		ctx, cancel := context.WithTimeoutCause(ctx, cfg.dialTimeout, ErrDialTimeout)
		defer cancel()

		// Set the dialer to use the context's deadline.
		cfg.Dial = func(network, addr string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, network, addr)
		}
		conn, err := amqp.DialConfig(uri, cfg.Config)
		if err == nil {
			return conn, nil
		}
		connectionFailures.WithLabelValues(
			net.JoinHostPort(u.Host, strconv.Itoa(u.Port)), /* server_node */
		).Inc()
		errs = append(errs, err)
	}

	return nil, fmt.Errorf("failed to connect to amqp server: %w", errors.Join(errs...))
}

// reconect will attempt to reconnect to the cluster.
//
// assumes lock is held by caller.
func (c *Client) dial(ctx context.Context) error {
	conn, err := dial(ctx, c.resolver, c.config)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	c.channelMu.Lock()
	defer c.channelMu.Unlock()
	// reconnect all the channels and redeclare all the delcarations.
	for delegate, ch := range c.channels {
		// This should be safe to call on already closed connections and ensures
		// they are closed.
		ch.Close()

		newCh, err := conn.Channel()
		if err != nil {
			return fmt.Errorf("failed to create channel: %w", err)
		}

		if delegate.initFn != nil {
			if err := delegate.initFn(newCh); err != nil {
				return fmt.Errorf("failed to reinitialize channel: %w", err)
			}
		}
		c.channels[delegate] = newCh
	}
	c.conn = conn

	// Close the old connection and create new one for next reconnect.
	close(c.connected)
	c.connected = make(chan struct{})
	connectedState.Set(1)

	return nil
}

func (c *Client) reconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.conn = nil

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if c.config.maxReconnectTime > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), c.config.maxReconnectTime)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	// Watch the close channel to make sure we cancel any active connection
	// attempts.
	go func() {
		select {
		case <-c.close:
			cancel()
		case <-ctx.Done():
		}
	}()

	c.reconnectBackoff.Reset()

	var attempts int
	for {
		attempts++
		reconnectAttempts.Inc()
		err := c.dial(ctx)
		if ctx.Err() != nil {
			return fmt.Errorf("amqpx: reconnect canceled: %w", err)
		}
		if err == nil {
			break
		}
		c.logger.Error("amqpx: failed to connect", "err", err, "attempts", attempts)
		// ensure the connection is closed
		if c.conn != nil {
			c.conn.Close()
		}

		if c.config.maxReconnectAttempts > 0 && attempts >= c.config.maxReconnectAttempts {
			return fmt.Errorf("amqpx: max reconnect attempts reached after %d attempts", attempts)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("amqpx: reconnect canceled: %w", ctx.Err())
		case <-c.close:
			return ErrConnClosed
		case <-time.After(c.reconnectBackoff.NextBackOff()):
		}
		// TODO: Support max reconnect attempts
	}

	return nil
}

func (c *Client) reconnectLoop() {
	for {
		if !c.IsClosed() {
			// Wait for a signal to reconnect
			err, ok := <-c.conn.NotifyClose(make(chan *amqp.Error))
			if !ok {
				// channel was closed and we are done
				return
			}
			slog.Error("amqpx: connection unexpecedly closed", "err", err)
			connectedState.Set(0)
		}
		if err := c.reconnect(); err != nil {
			slog.Error("amqpx: failed to reconnect", "err", err)
			return
		}

		c.logger.Info("amqpx: connected")
		reconnectAttempts.Set(0)
		reconnectTotal.Inc()
	}
}

func (c *Client) getChannel(ch *channelDelegate) (*amqp.Channel, <-chan struct{}) {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()
	return c.channels[ch], c.connected
}

func (c *Client) newChannel(ctx context.Context, initFn func(ch *amqp.Channel) error) (*channelDelegate, error) {
	c.channelMu.Lock()
	connected := c.connected
	c.channelMu.Unlock()

	select {
	case <-connected:
	case <-c.close:
		return nil, ErrConnClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Too bad amqp.Connection.Channel() doesn't take a context.
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	cd := &channelDelegate{
		client: c,
		initFn: initFn,
	}

	if initFn != nil {
		if err := initFn(ch); err != nil {
			return nil, fmt.Errorf("failed to initialize channel: %w", err)
		}
	}

	c.channels[cd] = ch
	return cd, nil
}

func (c *Client) closeChannel(cd *channelDelegate) error {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()

	ch, ok := c.channels[cd]
	if !ok {
		return fmt.Errorf("amqp channel not found")
	}
	delete(c.channels, cd)

	return ch.Close()
}

func (c *Client) LocalAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil
	}

	return c.conn.LocalAddr()
}

func (c *Client) RemoteAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil
	}

	return c.conn.RemoteAddr()
}

func (c *Client) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return true
	}

	return c.conn.IsClosed()
}

func (c *Client) Close() error {
	close(c.close)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil
	}

	return c.conn.Close()
}

// channelDelegate acts as a stand-in for a *amqp.Channel and is used as key for
// retreiving the underlying connection from the conn.  We do this so that Conn
// can manage all the channels and reconnect when there is an issue.
//
// We don't include a pointer to an *amqp.Channel here because we want *Conn to
// control the lifecycle of the *amqp.Channel.
type channelDelegate struct {
	client *Client
	initFn func(*amqp.Channel) error
}

func (cd *channelDelegate) call(ctx context.Context, block bool, fn func(ch *amqp.Channel) error) error {
	for {
		ch, connected := cd.client.getChannel(cd)

		err := fn(ch)
		if err == nil {
			return nil
		}

		if !block || !errors.Is(err, amqp.ErrClosed) {
			return err
		}

		// We are not sure whether the connection is closed or the channel is
		// closed here so we don't signal reconnect and hope that Connection/Channel.NotifyClose()
		// will be signaled.

		select {
		case <-connected:
		case <-ctx.Done():
			return ctx.Err()
		case <-cd.client.close:
			return ErrConnClosed
		}
	}
}

func (cd *channelDelegate) close() error {
	return cd.client.closeChannel(cd)
}

type clientConfig struct {
	amqp.Config
	logger               *slog.Logger
	dialTimeout          time.Duration
	maxReconnectAttempts int
	maxReconnectTime     time.Duration
	reconnectBackoff     backoff.BackOff

	forceDial bool
	dialCtx   context.Context
}

// DialOptions are options to be used when establishing a connection to a
// RabbitMQ with [Dial] or [DialCluster].
type DialOption interface {
	apply(*clientConfig)
}

type authenticationOption []amqp.Authentication

// WithAuth sets the SASL authentication mechanisms to try in the client
// request, and the successful mechanism used on the Connection object.  If not
// specified then PlainAuth is used with username/password set from the URL.
func WithAuth(auths ...amqp.Authentication) DialOption { return authenticationOption(auths) }
func (o authenticationOption) apply(c *clientConfig)   { c.SASL = append(c.SASL, o...) }

type channelMaxOption uint16

// WithChannelMax sets the maximum number of channels to use for the connection.
// 0 max channels means 2^16 - 1 and is the default.
func WithChannelMax(n uint16) DialOption         { return channelMaxOption(n) }
func (o channelMaxOption) apply(c *clientConfig) { c.ChannelMax = uint16(o) }

type frameSizeOption int

// WithFrameSize set the maximum frame size in bytes to use for the connection.
// 0 means unlimited and is the default.
func WithFrameSize(n int) DialOption            { return frameSizeOption(n) }
func (o frameSizeOption) apply(c *clientConfig) { c.FrameSize = int(o) }

type heartbeatOption time.Duration

// WithHeartbeat sets the interval at which the server will send heartbeats
// anything less than 1s uses the server's interval.
func WithHearthbeat(d time.Duration) DialOption { return heartbeatOption(d) }
func (o heartbeatOption) apply(c *clientConfig) { c.Heartbeat = time.Duration(o) }

type tlsClientConfig struct{ cfg *tls.Config }

// WithTLSClientConfig specifies the client configuration of the TLS connection
// when establishing a tls transport.
// If the URL uses an amqps scheme, then an empty tls.Config with the
// ServerName from the URL is used.
func WithTLSConfig(tlsConfig *tls.Config) DialOption { return tlsClientConfig{tlsConfig} }
func (o tlsClientConfig) apply(c *clientConfig)      { c.TLSClientConfig = o.cfg }

type dialTimeoutOption time.Duration

// WithDialTimeout sets the connection per connection.  If connecting to
// multiple nodes in a cluster this timeout is used for each connection.  To
// control connection timeout overall use the context on [Dial] and
// [DialContext].
//
// Default timeout is 30 seconds.
func WithDialTimeout(d time.Duration) DialOption  { return dialTimeoutOption(d) }
func (o dialTimeoutOption) apply(c *clientConfig) { c.dialTimeout = time.Duration(o) }

/*
type maxReconnectAttemptsOption int

func WithMaxReattempt(n int) DialOption                  { return maxReconnectAttemptsOption(n) }
func (o maxReconnectAttemptsOption) apply(c *dialConfig) { c.maxReconnectAttempts = int(o) }
*/

type reconnectBackoffOption struct{ backoff backoff.BackOff }

func WithReconnectBackoff(b backoff.BackOff) DialOption { return reconnectBackoffOption{b} }
func (o reconnectBackoffOption) apply(c *clientConfig)  { c.reconnectBackoff = o.backoff }

type loggerOption struct{ *slog.Logger }

func WithLogger(l *slog.Logger) DialOption   { return loggerOption{l} }
func (o loggerOption) apply(c *clientConfig) { c.logger = o.Logger }

type forceDial struct {
	ctx context.Context
}

// WithForceDial will make sure the client is connected to the server or cancled
// by the given context before returning the client back.  If the context was
// cancelled or the server cannot be contacted then and error is returned.
func WithForceDial(ctx context.Context) DialOption { return forceDial{ctx} }

func (o forceDial) apply(c *clientConfig) {
	c.forceDial = true
	c.dialCtx = o.ctx
}
