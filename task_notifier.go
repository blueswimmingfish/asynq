// Copyright 2024 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
	"github.com/redis/go-redis/v9"
)

// taskNotifier subscribes to Redis Pub/Sub channels to receive
// near-real-time notifications when tasks become pending.
// It feeds a channel that the processor selects on to wake up immediately
// instead of waiting for the next TaskCheckInterval poll.
type taskNotifier struct {
	logger *log.Logger
	broker base.Broker

	// done is used to signal the subscriber goroutine to stop.
	done chan struct{}

	// taskReadyCh delivers signals to the processor when tasks are ready.
	// The channel is buffered with capacity 1 to coalesce multiple rapid
	// notifications into a single wakeup.
	taskReadyCh chan struct{}

	// queue names to subscribe to.
	queues []string

	// time to wait before retrying to connect to redis.
	retryTimeout time.Duration
}

type taskNotifierParams struct {
	logger *log.Logger
	broker base.Broker
	queues []string
}

func newTaskNotifier(params taskNotifierParams) *taskNotifier {
	return &taskNotifier{
		logger:       params.logger,
		broker:       params.broker,
		done:         make(chan struct{}),
		taskReadyCh:  make(chan struct{}, 1),
		queues:       params.queues,
		retryTimeout: 5 * time.Second,
	}
}

func (n *taskNotifier) shutdown() {
	n.logger.Debug("TaskNotifier shutting down...")
	n.done <- struct{}{}
}

// C returns a receive-only channel that signals when tasks may be ready.
func (n *taskNotifier) C() <-chan struct{} {
	return n.taskReadyCh
}

func (n *taskNotifier) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			pubsub, ok := n.subscribe()
			if !ok {
				return // shutdown requested during subscribe
			}
			if n.listen(pubsub) {
				return // shutdown requested during listen
			}
			// listen returned false => pubsub channel closed, retry
			n.logger.Warn("TaskNotifier: pubsub connection lost, reconnecting...")
		}
	}()
}

// subscribe attempts to subscribe to task-ready channels with retries.
// Returns the pubsub and true on success, or nil and false if shutdown
// was requested.
func (n *taskNotifier) subscribe() (*redis.PubSub, bool) {
	for {
		pubsub, err := n.broker.TaskReadyPubSub(n.queues...)
		if err != nil {
			n.logger.Errorf("cannot subscribe to task ready channels: %v", err)
			select {
			case <-time.After(n.retryTimeout):
				continue
			case <-n.done:
				n.logger.Debug("TaskNotifier done")
				return nil, false
			}
		}
		return pubsub, true
	}
}

// listen reads from the pubsub channel and signals taskReadyCh.
// Returns true if shutdown was requested, false if the pubsub
// channel was closed (e.g. connection pool closed).
func (n *taskNotifier) listen(pubsub *redis.PubSub) bool {
	msgCh := pubsub.Channel()
	for {
		select {
		case <-n.done:
			pubsub.Close()
			n.logger.Debug("TaskNotifier done")
			return true
		case msg, ok := <-msgCh:
			if !ok {
				// Channel closed (pubsub connection pool closed).
				return false
			}
			if msg == nil {
				continue
			}
			// Non-blocking send to coalesce rapid notifications.
			select {
			case n.taskReadyCh <- struct{}{}:
			default:
			}
		}
	}
}
