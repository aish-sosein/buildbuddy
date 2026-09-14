package pubsub

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
)

const (
	// Error detail reason indicating that there was a problem reading from the
	// pubsub channel.
	pubsubChannelErrorReason = "PUBSUB_CHANNEL_ERROR"

	// batchedCheckAttempts is how many pipelines in a row a batched check
	// may fail to get an answer before its subscription is told. An
	// unanswered read is a timeout, a dropped connection, or a reply such as
	// LOADING, which a read on its own would have retried inside the client.
	// A missing stream is reported by the first pipeline that reads it.
	// Every subscription on a shard shares the shard's fate, so when the
	// attempts run out they all fail on the same execution.
	batchedCheckAttempts = 3

	// batchedCheckTimeout bounds one execution of the shared pipeline. It
	// matches the client's read timeout, so an execution is one full read
	// attempt and retrying happens across pipelines, in check, rather than
	// inside the client. The pipeline covers every shard and finishes with
	// the slowest, so a hung shard delays every subscription's check by this
	// long, and when the deadline cuts a shard's pipeline short the client
	// marks that shard's already-answered reads as failed too, costing them
	// an attempt.
	batchedCheckTimeout = 3 * time.Second
)

var (
	batchMonitoredStreamChecks = flag.Bool("remote_execution.pubsub_batch_monitored_stream_checks", false, "Check the monitored pubsub streams of all subscriptions with one pipeline per interval, split by shard, instead of one read per subscription per interval.")
)

type PubSub struct {
	rdb redis.UniversalClient
}

// NewPubSub creates a PubSub client based on the built-in Redis pubsub commands.
// Note that this mechanism is "lossy" in the sense that published messages are lost if there are no listeners.
// See NewListPubSub for a Redis list-based implementation that retains messages even if there are no subscribers.
func NewPubSub(redisClient redis.UniversalClient) *PubSub {
	return &PubSub{
		rdb: redisClient,
	}
}

func (p *PubSub) Publish(ctx context.Context, channelName string, message string) error {
	return p.rdb.Publish(ctx, channelName, message).Err()
}

// To prevent resource leakage, you should close the subscriber when done.
// For example:
//
//	subscriber := ps.Subscribe(ctx, channelName)
//	defer subscriber.Close()
//	for m := range subscriber.Chan() {
//	  // GOT CALLBACK!
//	}
func (p *PubSub) Subscribe(ctx context.Context, channelName string) interfaces.Subscriber {
	return &Subscriber{
		ps:  p.rdb.Subscribe(ctx, channelName),
		ctx: ctx,
	}
}

type Subscriber struct {
	ps  *redis.PubSub
	ctx context.Context
}

func (s *Subscriber) Close() error {
	return s.ps.Close()
}

func (s *Subscriber) Chan() <-chan string {
	internalChannel := s.ps.Channel()
	externalChannel := make(chan string)
	go func() {
		defer close(externalChannel)
		for m := range internalChannel {
			select {
			case externalChannel <- m.Payload:
			case <-s.ctx.Done():
				return
			}
		}
	}()
	return externalChannel
}

const (
	listTTL = 8 * time.Hour
)

const (
	// To keep things simple for now, we maintain streams with a single value per element stored under the following key.
	streamDataField = "data"
	// Key prefix to identify monitored channels.
	monitoredKeyPrefix = "monitoredPubSub/"
	// For "monitored" channels, we publish a dummy message at channel creation time so we have a means to verify
	// whether the object still exists in Redis.
	streamStartMarkerMessage = "this is a dummy message to verify existence of stream"
	// Frequency at which we check whether the PubSub stream still exists in Redis.
	monitoredChannelExistenceCheckInterval = 1 * time.Second
)

type StreamPubSub struct {
	rdb     redis.UniversalClient
	checker *streamExistenceChecker
}

// NewStreamPubSub creates a PubSub client based on a Redis-stream.
func NewStreamPubSub(redisClient redis.UniversalClient) *StreamPubSub {
	return &StreamPubSub{
		rdb:     redisClient,
		checker: newStreamExistenceChecker(redisClient, monitoredChannelExistenceCheckInterval),
	}
}

type Channel struct {
	name string
}

func (c *Channel) String() string {
	return c.name
}

func (p *StreamPubSub) CreateMonitoredChannel(ctx context.Context, name string) error {
	channelName := monitoredKeyPrefix + name
	channel := &Channel{name: channelName}
	if err := p.Publish(ctx, channel, streamStartMarkerMessage); err != nil {
		return err
	}
	return nil
}

func (p *StreamPubSub) DeleteMonitoredChannel(ctx context.Context, name string) error {
	return p.rdb.Del(ctx, monitoredKeyPrefix+name).Err()
}

func (p *StreamPubSub) MonitoredChannel(name string) *Channel {
	return &Channel{name: monitoredKeyPrefix + name}
}

func (p *StreamPubSub) UnmonitoredChannel(name string) *Channel {
	return &Channel{name: name}
}

type Message struct {
	Err  error
	Data string
}

type StreamSubscription struct {
	cancel context.CancelFunc
	ch     <-chan *Message
}

func (s *StreamSubscription) Close() error {
	s.cancel()
	return nil
}

func (s *StreamSubscription) Chan() <-chan *Message {
	return s.ch
}

func extractMsgData(channel *Channel, msg *redis.XMessage) (string, error) {
	data, ok := msg.Values[streamDataField]
	if !ok {
		return "", status.FailedPreconditionErrorf("Message %q on stream %q missing data field", msg.ID, channel.name)
	}
	str, ok := data.(string)
	if !ok {
		return "", status.FailedPreconditionErrorf("Message %q on stream %q is of type %T, wanted string", msg.ID, channel.name, data)
	}

	return str, nil
}

func deliverError(ctx context.Context, outCh chan *Message, err error) {
	select {
	case outCh <- &Message{Err: err}:
	case <-ctx.Done():
	}
}

func (p *StreamPubSub) deliverMsg(ctx context.Context, psChannel *Channel, outCh chan *Message, msg *redis.XMessage) bool {
	data, err := extractMsgData(psChannel, msg)
	if err != nil {
		alert.UnexpectedEvent("could not extract PubSub message contents", "error: %s", err)
		return false
	}
	if data == streamStartMarkerMessage {
		return true
	}

	select {
	case outCh <- &Message{Data: data}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *StreamPubSub) checkMonitoredChannelExists(ctx context.Context, channel *Channel) error {
	if *batchMonitoredStreamChecks {
		// A batched check waits for the next pipeline, up to an interval, so
		// a subscription's first check reports a lost stream up to that much
		// later than a read of its own would.
		return p.checker.check(ctx, channel)
	}
	result, err := p.rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{channel.name, "0"},
		Count:   1,
		Block:   -1, // No blocking.
	}).Result()
	return checkMonitoredStreamResult(channel, result, err)
}

// checkMonitoredStreamResult interprets a read from the start of a monitored
// stream: the stream must exist and begin with the marker written when it
// was created, otherwise Redis has lost it.
func checkMonitoredStreamResult(channel *Channel, result []redis.XStream, err error) error {
	if err == redis.Nil {
		return status.UnavailableErrorf("PubSub channel %q disappeared", channel.name)
	}
	if err != nil {
		return status.UnavailableErrorf("unable to check existence of PubSub channel %q: %s", channel.name, err)
	}
	if len(result) != 1 {
		return status.UnknownErrorf("invalid xread return for channel %q", channel.name)
	}
	stream := result[0]
	if len(stream.Messages) == 0 {
		return status.UnavailableErrorf("PubSub channel %q disappeared", channel.name)
	}
	data, err := extractMsgData(channel, &stream.Messages[0])
	if err != nil {
		return status.UnavailableErrorf("unable to check existence of PubSub channel %q", channel.name)
	}
	if data != streamStartMarkerMessage {
		return status.UnavailableErrorf("PubSub channel %q does not start with verification message", channel.name)
	}
	return nil
}

func (p *StreamPubSub) subscribe(ctx context.Context, psChannel *Channel, startFromTail bool) *StreamSubscription {
	ctx, cancel := context.WithCancel(ctx)

	// If this is a monitored channel, start a goroutine that will periodically check that the stream still exists or
	// publish an error if it does not.
	monChan := make(chan *Message)
	if strings.HasPrefix(psChannel.name, monitoredKeyPrefix) {
		go func() {
			defer close(monChan)
			ticker := time.NewTicker(monitoredChannelExistenceCheckInterval)
			defer ticker.Stop()

			for {
				if err := p.checkMonitoredChannelExists(ctx, psChannel); err != nil {
					// Wrap error with details so the client can differentiate
					// pubsub channel errors from execution errors.
					err = status.WithReason(err, pubsubChannelErrorReason)
					deliverError(ctx, monChan, err)
					return
				}
				select {
				case <-ticker.C:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	msgChan := make(chan *Message)
	go func() {
		defer close(msgChan)

		// Start from beginning of stream.
		streamID := "0"

		// If starting from tail, check if the stream has any elements.
		// If it does then publish the last element and subscribe to messages following that element.
		if startFromTail {
			msgs, err := p.rdb.XRevRangeN(ctx, psChannel.name, "+", "-", 1).Result()
			if err != nil {
				if err != context.Canceled {
					log.CtxErrorf(ctx, "Unable to retrieve last element of stream: %q: %s", psChannel.name, err)
				}
				deliverError(ctx, msgChan, err)
				return
			}
			if len(msgs) == 1 {
				msg := msgs[0]
				streamID = msg.ID
				if !p.deliverMsg(ctx, psChannel, msgChan, &msg) {
					return
				}
			}
		}

		for {
			result, err := p.rdb.XRead(ctx, &redis.XReadArgs{
				Streams: []string{psChannel.name, streamID},
				Block:   0, // Block indefinitely
			}).Result()
			if err != nil {
				if err != context.Canceled {
					log.CtxErrorf(ctx, "Error reading from stream %q: %s", psChannel.name, err)
					deliverError(ctx, msgChan, err)
				}
				return
			}
			// We are subscribing to a single stream so there should be exactly one response.
			if len(result) != 1 {
				log.CtxErrorf(ctx, "Did not receive exactly one result for channel %q, got %d", psChannel.name, len(result))
				return
			}
			for _, msg := range result[0].Messages {
				if !p.deliverMsg(ctx, psChannel, msgChan, &msg) {
					return
				}
				streamID = msg.ID
			}
		}
	}()

	ch := make(chan *Message)
	go func() {
		defer cancel()
		defer close(ch)
		for {
			select {
			case msg, ok := <-monChan:
				if !ok {
					return
				}
				ch <- msg
			case msg, ok := <-msgChan:
				if !ok {
					return
				}
				ch <- msg
			case <-ctx.Done():
				return
			}
		}
	}()

	return &StreamSubscription{
		cancel: cancel,
		ch:     ch,
	}
}

// SubscribeHead returns a subscription for all previous and future message on the stream.
func (p *StreamPubSub) SubscribeHead(ctx context.Context, channel *Channel) *StreamSubscription {
	// Subscribe from the beginning of the stream.
	return p.subscribe(ctx, channel, false /*startFromTail=*/)
}

// SubscribeTail returns a subscription for messages starting from the last message already on the stream, if any.
func (p *StreamPubSub) SubscribeTail(ctx context.Context, channel *Channel) *StreamSubscription {
	// Subscribe from the last elements of the stream, if any.
	return p.subscribe(ctx, channel, true /*startFromTail=*/)
}

func (p *StreamPubSub) Publish(ctx context.Context, channel *Channel, message string) error {
	pipe := p.rdb.TxPipeline()
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: channel.name,
		Values: map[string]any{streamDataField: message},
	})
	pipe.Expire(ctx, channel.name, listTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (p *StreamPubSub) Expire(ctx context.Context, channel *Channel, d time.Duration) error {
	return p.rdb.Expire(ctx, channel.name, d).Err()
}

// streamExistenceChecker batches the stream existence checks of many
// subscriptions into shared pipelines. A check queues its read on the current
// pipeline and waits for it to be executed, which happens once per interval,
// so a shard sees one pipeline per interval per app instead of a read per
// subscription.
type streamExistenceChecker struct {
	rdb      redis.UniversalClient
	interval time.Duration

	mu sync.Mutex // mu protects: pipe, executed
	// pipe collects reads until it is executed; nil when nothing is queued.
	// executed is closed once pipe has been executed.
	pipe     redis.Pipeliner
	executed chan struct{}
}

func newStreamExistenceChecker(rdb redis.UniversalClient, interval time.Duration) *streamExistenceChecker {
	return &streamExistenceChecker{rdb: rdb, interval: interval}
}

// check reads the start of the stream through the shared pipeline and
// interprets the reply the way checkMonitoredStreamResult does. A read that
// never reached Redis says nothing about the stream, so it is queued again
// on the next pipeline, up to batchedCheckAttempts in a row, before it is
// reported. Waiting for the next pipeline is already an interval apart, so
// there is no backoff beyond that.
func (c *streamExistenceChecker) check(ctx context.Context, channel *Channel) error {
	for attempt := 1; ; attempt++ {
		read, executed := c.queue(channel)
		select {
		case <-executed:
		case <-ctx.Done():
			return checkMonitoredStreamResult(channel, nil, ctx.Err())
		}
		result, err := read.Result()
		if !retryable(err) {
			return checkMonitoredStreamResult(channel, result, err)
		}
		if attempt == batchedCheckAttempts {
			return status.UnavailableErrorf("unable to check existence of PubSub channel %q after %d attempts: %s", channel.name, attempt, err)
		}
	}
}

// queue adds a read of the stream's first entry to the current pipeline,
// starting one and scheduling its execution if none is open, and returns the
// read along with the channel that closes once it has been executed.
func (c *streamExistenceChecker) queue(channel *Channel) (*redis.XStreamSliceCmd, chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pipe == nil {
		c.pipe = c.rdb.Pipeline()
		c.executed = make(chan struct{})
		time.AfterFunc(c.interval, c.execute)
	}
	read := c.pipe.XRead(context.Background(), &redis.XReadArgs{
		Streams: []string{channel.name, "0"},
		Count:   1,
		Block:   -1, // No blocking.
	})
	return read, c.executed
}

// execute runs the current pipeline and releases the checks waiting on it.
// Each read's own result says what happened to its stream. A failure to
// reach Redis is logged here, once per execution, because it otherwise only
// surfaces as each affected subscription failing on its own.
func (c *streamExistenceChecker) execute() {
	c.mu.Lock()
	pipe, executed := c.pipe, c.executed
	c.pipe, c.executed = nil, nil
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), batchedCheckTimeout)
	defer cancel()
	cmds, _ := pipe.Exec(ctx)
	for _, cmd := range cmds {
		if err := cmd.Err(); retryable(err) {
			log.Warningf("Batched pubsub stream check of %d streams could not reach Redis: %s", len(cmds), err)
			break
		}
	}
	close(executed)
}

// retryable returns whether a pipelined read should be issued again. The
// ring's shard clients don't retry pipelines at all, and no client retries
// an error reply inside one, so this follows the retry logic go-redis
// applies to individually issued commands. The deadline it also accepts is
// execute's own; the subscriber's context never reaches the pipeline.
func retryable(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || redisutil.IsTransientError(err)
}
