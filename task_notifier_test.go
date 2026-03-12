// Copyright 2024 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/rdb"
	"github.com/hibiken/asynq/internal/testbroker"
)

func TestTaskNotifier(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r, rdb.WithTaskNotification())

	qname := "default"
	notifier := newTaskNotifier(taskNotifierParams{
		logger: testLogger,
		broker: rdbClient,
		queues: []string{qname},
	})

	var wg sync.WaitGroup
	notifier.start(&wg)
	defer notifier.shutdown()

	// Wait for subscription to be established.
	time.Sleep(time.Second)

	// Publish a task-ready signal.
	if err := rdbClient.PublishTaskReady(qname); err != nil {
		t.Fatalf("PublishTaskReady returned error: %v", err)
	}

	// Expect to receive on the channel.
	select {
	case <-notifier.C():
		// success
	case <-time.After(2 * time.Second):
		t.Error("timed out waiting for task ready notification")
	}
}

func TestTaskNotifierCoalescing(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r, rdb.WithTaskNotification())

	qname := "default"
	notifier := newTaskNotifier(taskNotifierParams{
		logger: testLogger,
		broker: rdbClient,
		queues: []string{qname},
	})

	var wg sync.WaitGroup
	notifier.start(&wg)
	defer notifier.shutdown()

	time.Sleep(time.Second)

	// Publish multiple signals rapidly.
	for i := 0; i < 5; i++ {
		if err := rdbClient.PublishTaskReady(qname); err != nil {
			t.Fatalf("PublishTaskReady returned error: %v", err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Drain the channel — should get at least one signal.
	count := 0
	for {
		select {
		case <-notifier.C():
			count++
		default:
			goto done
		}
	}
done:
	if count == 0 {
		t.Error("expected at least one notification, got zero")
	}
	// The buffered channel (cap 1) should coalesce multiple rapid publishes.
	// We may get 1 or 2 (if the goroutine drains between publishes), but
	// certainly not 5.
	if count > 2 {
		t.Errorf("expected coalesced notifications (1-2), got %d", count)
	}
}

func TestTaskNotifierMultipleQueues(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r, rdb.WithTaskNotification())

	queues := []string{"critical", "default", "low"}
	notifier := newTaskNotifier(taskNotifierParams{
		logger: testLogger,
		broker: rdbClient,
		queues: queues,
	})

	var wg sync.WaitGroup
	notifier.start(&wg)
	defer notifier.shutdown()

	time.Sleep(time.Second)

	// Publish to each queue and verify we get a notification.
	for _, qname := range queues {
		if err := rdbClient.PublishTaskReady(qname); err != nil {
			t.Fatalf("PublishTaskReady(%q) returned error: %v", qname, err)
		}
		select {
		case <-notifier.C():
			// success
		case <-time.After(2 * time.Second):
			t.Errorf("timed out waiting for notification for queue %q", qname)
		}
	}
}

func TestTaskNotifierWithRedisDown(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic occurred: %v", r)
		}
	}()
	r := rdb.NewRDB(setup(t), rdb.WithTaskNotification())
	defer r.Close()
	tb := testbroker.NewTestBroker(r)

	notifier := newTaskNotifier(taskNotifierParams{
		logger: testLogger,
		broker: tb,
		queues: []string{"default"},
	})
	notifier.retryTimeout = 1 * time.Second

	tb.Sleep() // simulate redis being unavailable
	var wg sync.WaitGroup
	notifier.start(&wg)
	defer notifier.shutdown()

	time.Sleep(2 * time.Second) // notifier should wait and retry

	tb.Wakeup() // simulate redis coming back

	time.Sleep(2 * time.Second) // allow notifier to establish subscription

	if err := r.PublishTaskReady("default"); err != nil {
		t.Fatalf("PublishTaskReady returned error: %v", err)
	}

	select {
	case <-notifier.C():
		// success
	case <-time.After(2 * time.Second):
		t.Error("timed out waiting for notification after redis recovery")
	}
}
