package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	channel1Name = "testChannelName"
	message1     = "msg1"
	message2     = "msg2"
	message3     = "msg3"
)

func TestLossyPubSub(t *testing.T) {
	pubSub := NewPubSub(testredis.Start(t).Client())

	ctx := context.Background()
	subscriber := pubSub.Subscribe(ctx, "test")
	ch := subscriber.Chan()

	err := pubSub.Publish(ctx, "test", "hello")
	require.NoError(t, err)

	msg := <-ch
	require.Equal(t, "hello", msg)

	err = subscriber.Close()
	require.NoError(t, err)

	// Channel should be closed
	_, ok := <-ch
	require.False(t, ok)
}

func TestStreamPubSub(t *testing.T) {
	redisHandle := testredis.Start(t)
	pubSub := NewStreamPubSub(redis.NewClient(redisutil.TargetToOptions(redisHandle.Target)))

	ctx := context.Background()

	channel1 := pubSub.UnmonitoredChannel(channel1Name)

	subscriber := pubSub.SubscribeHead(ctx, channel1)
	defer subscriber.Close()
	requireNoMessages(t, subscriber)

	// Publish a message and it should be immediately available to the subscriber.
	err := pubSub.Publish(ctx, channel1, message1)
	require.NoError(t, err)
	requireMessages(t, subscriber, message1)

	// Subscriber should not receive any other messages.
	requireNoMessages(t, subscriber)

	// Publish a second message and verify subscriber receives it.
	err = pubSub.Publish(ctx, channel1, message2)
	require.NoError(t, err)
	requireMessages(t, subscriber, message2)

	// Create a new "head" subscriber which should see both previously published messages.
	subscriber2 := pubSub.SubscribeHead(ctx, channel1)
	requireMessages(t, subscriber2, message1, message2)

	// Create a "tail" subscriber which should only see the last message.
	tailSubscriber := pubSub.SubscribeTail(ctx, channel1)
	requireMessages(t, tailSubscriber, message2)

	// Publish another message which should be seen by all subscribers,
	err = pubSub.Publish(ctx, channel1, message3)
	require.NoError(t, err)
	requireMessages(t, subscriber, message3)
	requireMessages(t, subscriber2, message3)
	requireMessages(t, tailSubscriber, message3)
}

func TestMonitoredPubSub(t *testing.T) {
	// The stream can be checked once per subscription or once per interval
	// for all subscriptions; a lost stream must surface either way.
	for _, batched := range []bool{false, true} {
		t.Run(fmt.Sprintf("batched=%t", batched), func(t *testing.T) {
			flags.Set(t, "remote_execution.pubsub_batch_monitored_stream_checks", batched)
			redisHandle := testredis.Start(t)
			rdb := redis.NewClient(redisutil.TargetToOptions(redisHandle.Target))
			pubSub := NewStreamPubSub(rdb)

			ctx := t.Context()

			err := pubSub.CreateMonitoredChannel(ctx, channel1Name)
			require.NoError(t, err)
			channel1 := pubSub.MonitoredChannel(channel1Name)

			subscriber := pubSub.SubscribeHead(ctx, channel1)
			defer subscriber.Close()
			requireNoMessages(t, subscriber)

			// Publish a message and it should be immediately available to the subscriber.
			err = pubSub.Publish(ctx, channel1, message1)
			require.NoError(t, err)
			requireMessages(t, subscriber, message1)

			// Let at least one check complete against the intact stream, so
			// the loss below has to be caught by a later check. Batched
			// checks execute at most once per interval.
			time.Sleep(monitoredChannelExistenceCheckInterval * 3 / 2)

			// Losing the stream, as a Redis restart would, must be reported by
			// the check. Flushing rather than restarting keeps connections
			// intact, so the subscription's own read can't fail first.
			err = rdb.FlushAll(ctx).Err()
			require.NoError(t, err)

			err = requireError(t, subscriber)
			require.True(t, status.IsUnavailableError(err), "expected UNAVAILABLE error but got %s", err)
			require.Contains(t, err.Error(), "disappeared")
		})
	}
}

func TestDeleteMonitoredChannel(t *testing.T) {
	// A deleted stream must surface whether it is checked per subscription
	// or in the shared batch.
	for _, batched := range []bool{false, true} {
		t.Run(fmt.Sprintf("batched=%t", batched), func(t *testing.T) {
			flags.Set(t, "remote_execution.pubsub_batch_monitored_stream_checks", batched)
			redisHandle := testredis.Start(t)
			pubSub := NewStreamPubSub(redis.NewClient(redisutil.TargetToOptions(redisHandle.Target)))

			ctx := t.Context()

			err := pubSub.CreateMonitoredChannel(ctx, channel1Name)
			require.NoError(t, err)
			channel1 := pubSub.MonitoredChannel(channel1Name)

			subscriber := pubSub.SubscribeHead(ctx, channel1)
			defer subscriber.Close()

			err = pubSub.Publish(ctx, channel1, message1)
			require.NoError(t, err)
			requireMessages(t, subscriber, message1)

			// Let at least one check complete against the intact stream, so
			// the deletion below has to be caught by a later check. Batched
			// checks execute at most once per interval.
			time.Sleep(monitoredChannelExistenceCheckInterval * 3 / 2)

			err = pubSub.DeleteMonitoredChannel(ctx, channel1Name)
			require.NoError(t, err)

			err = requireError(t, subscriber)
			require.True(t, status.IsUnavailableError(err), "expected UNAVAILABLE error but got %s", err)
			require.Contains(t, err.Error(), "disappeared")

			// Deleting a non-existent channel should be a no-op.
			err = pubSub.DeleteMonitoredChannel(ctx, channel1Name)
			require.NoError(t, err)
		})
	}
}

func TestStreamExistenceChecker(t *testing.T) {
	redisHandle := testredis.Start(t)
	rdb := redis.NewClient(redisutil.TargetToOptions(redisHandle.Target))
	pubSub := NewStreamPubSub(rdb)
	ctx := t.Context()

	// A short interval keeps the checks quick; each check waits for the
	// next pipeline execution.
	interval := 50 * time.Millisecond
	checker := newStreamExistenceChecker(rdb, interval)

	// An intact stream checks clean, and a stream that was never created
	// reports as disappeared on the first pipeline that reads it.
	err := pubSub.CreateMonitoredChannel(ctx, channel1Name)
	require.NoError(t, err)
	err = checker.check(ctx, pubSub.MonitoredChannel(channel1Name))
	require.NoError(t, err)
	err = checker.check(ctx, pubSub.MonitoredChannel("never-created"))
	require.True(t, status.IsUnavailableError(err), "expected UNAVAILABLE error but got %s", err)
	require.Contains(t, err.Error(), "disappeared")

	// With Redis gone, reads go unanswered. The check queues on later
	// pipelines and only gives up after batchedCheckAttempts in a row, which
	// its message records.
	redisHandle.Shutdown()
	err = checker.check(ctx, pubSub.MonitoredChannel(channel1Name))
	require.True(t, status.IsUnavailableError(err), "expected UNAVAILABLE error but got %s", err)
	require.Contains(t, err.Error(), fmt.Sprintf("after %d attempts", batchedCheckAttempts))
}

func requireNoMessages(t *testing.T, subscriber *StreamSubscription) {
	select {
	case msg, ok := <-subscriber.Chan():
		if !ok {
			assert.FailNow(t, "subscriber channel closed prematurely")
		}
		assert.FailNow(t, "received PubSub message but none were expected", "message: %q", msg)
	case <-time.After(500 * time.Millisecond):
		return
	}
}

func requireMessages(t *testing.T, subscriber *StreamSubscription, expectedMessages ...string) {
	done := false
	var receivedMsgs []string
	for !done {
		select {
		case msg, ok := <-subscriber.Chan():
			if !ok {
				assert.FailNow(t, "subscriber channel closed prematurely")
			}
			require.NoError(t, msg.Err, "expected message, but got error")
			receivedMsgs = append(receivedMsgs, msg.Data)
			if len(receivedMsgs) == len(expectedMessages) {
				done = true
			}
		case <-time.After(2 * time.Second):
			done = true
		}
	}

	if len(receivedMsgs) == 0 {
		assert.FailNow(t, "expected PubSub messages to be available, but none received")
	}

	require.Equal(t, expectedMessages, receivedMsgs, "received PubSub messages did not match expected messages")
}

func requireError(t *testing.T, subscriber *StreamSubscription) error {
	select {
	case msg, ok := <-subscriber.Chan():
		if !ok {
			assert.FailNow(t, "subscriber channel closed prematurely")
		}
		require.Error(t, msg.Err, "subscriber should have returned an error")
		return msg.Err
	case <-time.After(5 * time.Second):
		assert.FailNow(t, "expected to receive an error but none received")
	}
	return nil
}
