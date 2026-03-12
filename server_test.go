// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/hibiken/asynq/internal/testbroker"
	"github.com/hibiken/asynq/internal/testutil"

	"github.com/redis/go-redis/v9"
	"go.uber.org/goleak"
)

func testServer(t *testing.T, c *Client, srv *Server) {
	// no-op handler
	h := func(ctx context.Context, task *Task) error {
		return nil
	}

	err := srv.Start(HandlerFunc(h))
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Enqueue(NewTask("send_email", testutil.JSON(map[string]interface{}{"recipient_id": 123})))
	if err != nil {
		t.Errorf("could not enqueue a task: %v", err)
	}

	_, err = c.Enqueue(NewTask("send_email", testutil.JSON(map[string]interface{}{"recipient_id": 456})), ProcessIn(1*time.Hour))
	if err != nil {
		t.Errorf("could not enqueue a task: %v", err)
	}

	srv.Shutdown()
}

func TestServer(t *testing.T) {
	// https://github.com/go-redis/redis/issues/1029
	ignoreOpt := goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNone(t, ignoreOpt)

	redisConnOpt := getRedisConnOpt(t)
	c := NewClient(redisConnOpt)
	defer c.Close()
	srv := NewServer(redisConnOpt, Config{
		Concurrency: 10,
		LogLevel:    testLogLevel,
	})

	testServer(t, c, srv)
}

func TestTaskNotificationEndToEnd(t *testing.T) {
	ignoreOpt := goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNone(t, ignoreOpt)

	redisConnOpt := getRedisConnOpt(t)

	// Use a very long TaskCheckInterval so polling alone would never pick up the task
	// within our deadline. If the task is processed quickly, it proves pub/sub worked.
	srv := NewServer(redisConnOpt, Config{
		Concurrency:            10,
		LogLevel:               testLogLevel,
		TaskCheckInterval:      30 * time.Second,
		EnableTaskNotification: true,
	})

	// Client with notification enabled — publishes to the task-ready channel on enqueue.
	c := NewClientWithNotification(redisConnOpt)
	defer c.Close()

	processed := make(chan struct{}, 1)
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		select {
		case processed <- struct{}{}:
		default:
		}
		return nil
	})

	if err := srv.Start(handler); err != nil {
		t.Fatalf("srv.Start failed: %v", err)
	}

	// Wait for the server to fully start (subscriber established, etc.)
	time.Sleep(1 * time.Second)

	_, err := c.Enqueue(NewTask("test:notification", nil))
	if err != nil {
		t.Fatalf("c.Enqueue failed: %v", err)
	}

	// Task must be processed within 3 seconds. With a 30s TaskCheckInterval,
	// this can only succeed if the pub/sub notification woke the processor.
	select {
	case <-processed:
		// success — task was processed via pub/sub notification
	case <-time.After(3 * time.Second):
		t.Error("task was not processed within 3s; pub/sub notification likely failed")
	}

	srv.Shutdown()
}

func TestTaskNotificationEndToEndWithoutClientNotification(t *testing.T) {
	ignoreOpt := goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNone(t, ignoreOpt)

	redisConnOpt := getRedisConnOpt(t)

	// Server with notification enabled and a short TaskCheckInterval (for polling fallback).
	srv := NewServer(redisConnOpt, Config{
		Concurrency:            10,
		LogLevel:               testLogLevel,
		TaskCheckInterval:      1 * time.Second,
		EnableTaskNotification: true,
	})

	// Client WITHOUT notification — uses standard NewClient (no pub/sub publish).
	c := NewClient(redisConnOpt)
	defer c.Close()

	processed := make(chan struct{}, 1)
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		select {
		case processed <- struct{}{}:
		default:
		}
		return nil
	})

	if err := srv.Start(handler); err != nil {
		t.Fatalf("srv.Start failed: %v", err)
	}

	time.Sleep(1 * time.Second)

	_, err := c.Enqueue(NewTask("test:fallback", nil))
	if err != nil {
		t.Fatalf("c.Enqueue failed: %v", err)
	}

	// Even without client-side notification, polling fallback should process the task
	// within 3 seconds (TaskCheckInterval is 1s).
	select {
	case <-processed:
		// success — polling fallback worked
	case <-time.After(3 * time.Second):
		t.Error("task was not processed within 3s via polling fallback")
	}

	srv.Shutdown()
}

func TestTaskNotificationForwarderNotifies(t *testing.T) {
	ignoreOpt := goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNone(t, ignoreOpt)

	redisConnOpt := getRedisConnOpt(t)

	// Server with notification and a very long TaskCheckInterval.
	// The forwarder moves scheduled tasks to pending, which should trigger pub/sub.
	srv := NewServer(redisConnOpt, Config{
		Concurrency:            10,
		LogLevel:               testLogLevel,
		TaskCheckInterval:      30 * time.Second,
		EnableTaskNotification: true,
	})

	// Client without notification — the server's forwarder will publish when
	// moving the task from scheduled to pending.
	c := NewClient(redisConnOpt)
	defer c.Close()

	processed := make(chan struct{}, 1)
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		select {
		case processed <- struct{}{}:
		default:
		}
		return nil
	})

	if err := srv.Start(handler); err != nil {
		t.Fatalf("srv.Start failed: %v", err)
	}

	time.Sleep(1 * time.Second)

	// Schedule the task to be processed 1 second from now.
	_, err := c.Enqueue(NewTask("test:scheduled", nil), ProcessIn(1*time.Second))
	if err != nil {
		t.Fatalf("c.Enqueue failed: %v", err)
	}

	// The forwarder checks scheduled tasks every 5s by default. After moving
	// the task to pending, the server's RDB publishes a task-ready notification.
	// With 30s TaskCheckInterval, the processor can only pick it up via pub/sub.
	// Allow up to 10s for the forwarder interval + notification propagation.
	select {
	case <-processed:
		// success
	case <-time.After(10 * time.Second):
		t.Error("scheduled task was not processed within 10s; forwarder notification likely failed")
	}

	srv.Shutdown()
}

func TestServerFromRedisClient(t *testing.T) {
	// https://github.com/go-redis/redis/issues/1029
	ignoreOpt := goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNone(t, ignoreOpt)

	redisConnOpt := getRedisConnOpt(t)
	redisClient := redisConnOpt.MakeRedisClient().(redis.UniversalClient)
	c := NewClientFromRedisClient(redisClient)
	srv := NewServerFromRedisClient(redisClient, Config{
		Concurrency: 10,
		LogLevel:    testLogLevel,
	})

	testServer(t, c, srv)

	err := c.Close()
	if err == nil {
		t.Error("client.Close() should have failed because of a shared client but it didn't")
	}
}

func TestServerWithQueueConcurrency(t *testing.T) {
	// https://github.com/go-redis/redis/issues/1029
	ignoreOpt := goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNone(t, ignoreOpt)

	redisConnOpt := getRedisConnOpt(t)
	r, ok := redisConnOpt.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		t.Fatalf("asynq: unsupported RedisConnOpt type %T", r)
	}

	const taskNum = 8
	const serverNum = 2
	tests := []struct {
		name             string
		concurrency      int
		queueConcurrency int
		wantActiveNum    int
	}{
		{
			name:             "based on client concurrency control",
			concurrency:      2,
			queueConcurrency: 6,
			wantActiveNum:    2 * serverNum,
		},
		{
			name:             "no queue concurrency control",
			concurrency:      2,
			queueConcurrency: 0,
			wantActiveNum:    2 * serverNum,
		},
		{
			name:             "based on queue concurrency control",
			concurrency:      6,
			queueConcurrency: 2,
			wantActiveNum:    2,
		},
	}

	// no-op handler
	handle := func(ctx context.Context, task *Task) error {
		time.Sleep(time.Second * 2)
		return nil
	}

	var servers [serverNum]*Server
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			testutil.FlushDB(t, r)
			c := NewClient(redisConnOpt)
			defer c.Close()
			for i := 0; i < taskNum; i++ {
				_, err = c.Enqueue(NewTask("send_email",
					testutil.JSON(map[string]interface{}{"recipient_id": i + 123})))
				if err != nil {
					t.Fatalf("could not enqueue a task: %v", err)
				}
			}

			for i := 0; i < serverNum; i++ {
				srv := NewServer(redisConnOpt, Config{
					Concurrency:      tc.concurrency,
					LogLevel:         testLogLevel,
					QueueConcurrency: map[string]int{base.DefaultQueueName: tc.queueConcurrency},
				})
				err = srv.Start(HandlerFunc(handle))
				if err != nil {
					t.Fatal(err)
				}
				servers[i] = srv
			}
			defer func() {
				for _, srv := range servers {
					srv.Shutdown()
				}
			}()

			time.Sleep(time.Second)
			inspector := NewInspector(redisConnOpt)
			tasks, err := inspector.ListActiveTasks(base.DefaultQueueName)
			if err != nil {
				t.Fatalf("could not list active tasks: %v", err)
			}
			if len(tasks) != tc.wantActiveNum {
				t.Errorf("default queue has %d active tasks, want %d", len(tasks), tc.wantActiveNum)
			}
		})
	}
}

func TestServerWithDynamicQueue(t *testing.T) {
	// https://github.com/go-redis/redis/issues/1029
	ignoreOpt := goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNone(t, ignoreOpt)

	redisConnOpt := getRedisConnOpt(t)
	r, ok := redisConnOpt.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		t.Fatalf("asynq: unsupported RedisConnOpt type %T", r)
	}

	const taskNum = 8
	const serverNum = 2
	tests := []struct {
		name             string
		concurrency      int
		queueConcurrency int
		wantActiveNum    int
	}{
		{
			name:             "based on client concurrency control",
			concurrency:      2,
			queueConcurrency: 6,
			wantActiveNum:    2 * serverNum,
		},
		{
			name:             "no queue concurrency control",
			concurrency:      2,
			queueConcurrency: 0,
			wantActiveNum:    2 * serverNum,
		},
		{
			name:             "based on queue concurrency control",
			concurrency:      6,
			queueConcurrency: 2,
			wantActiveNum:    2 * serverNum,
		},
	}

	// no-op handler
	handle := func(ctx context.Context, task *Task) error {
		time.Sleep(time.Second * 2)
		return nil
	}

	var DynamicQueueNameFmt = "dynamic:%d:%d"
	var servers [serverNum]*Server
	for tcn, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			testutil.FlushDB(t, r)
			c := NewClient(redisConnOpt)
			defer c.Close()
			for i := 0; i < taskNum; i++ {
				_, err = c.Enqueue(NewTask("send_email",
					testutil.JSON(map[string]interface{}{"recipient_id": i + 123})),
					Queue(fmt.Sprintf(DynamicQueueNameFmt, tcn, i%2)))
				if err != nil {
					t.Fatalf("could not enqueue a task: %v", err)
				}
			}

			for i := 0; i < serverNum; i++ {
				srv := NewServer(redisConnOpt, Config{
					Concurrency:      tc.concurrency,
					LogLevel:         testLogLevel,
					QueueConcurrency: map[string]int{base.DefaultQueueName: tc.queueConcurrency},
				})
				err = srv.Start(HandlerFunc(handle))
				if err != nil {
					t.Fatal(err)
				}
				srv.AddQueue(fmt.Sprintf(DynamicQueueNameFmt, tcn, i), 1, tc.queueConcurrency)
				servers[i] = srv
			}
			defer func() {
				for _, srv := range servers {
					srv.Shutdown()
				}
			}()

			time.Sleep(time.Second)
			inspector := NewInspector(redisConnOpt)

			var tasks []*TaskInfo

			for i := range servers {
				qtasks, err := inspector.ListActiveTasks(fmt.Sprintf(DynamicQueueNameFmt, tcn, i))
				if err != nil {
					t.Fatalf("could not list active tasks: %v", err)
				}
				tasks = append(tasks, qtasks...)
			}

			if len(tasks) != tc.wantActiveNum {
				t.Errorf("dynamic queue has %d active tasks, want %d", len(tasks), tc.wantActiveNum)
			}
		})
	}
}

func TestServerRun(t *testing.T) {
	// https://github.com/go-redis/redis/issues/1029
	ignoreOpt := goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNone(t, ignoreOpt)

	srv := NewServer(getRedisConnOpt(t), Config{LogLevel: testLogLevel})

	done := make(chan struct{})
	// Make sure server exits when receiving TERM signal.
	go func() {
		time.Sleep(2 * time.Second)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
		done <- struct{}{}
	}()

	go func() {
		select {
		case <-time.After(10 * time.Second):
			panic("server did not stop after receiving TERM signal")
		case <-done:
		}
	}()

	mux := NewServeMux()
	if err := srv.Run(mux); err != nil {
		t.Fatal(err)
	}
}

func TestServerErrServerClosed(t *testing.T) {
	srv := NewServer(getRedisConnOpt(t), Config{LogLevel: testLogLevel})
	handler := NewServeMux()
	if err := srv.Start(handler); err != nil {
		t.Fatal(err)
	}
	srv.Shutdown()
	err := srv.Start(handler)
	if err != ErrServerClosed {
		t.Errorf("Restarting server: (*Server).Start(handler) = %v, want ErrServerClosed error", err)
	}
}

func TestServerErrNilHandler(t *testing.T) {
	srv := NewServer(getRedisConnOpt(t), Config{LogLevel: testLogLevel})
	err := srv.Start(nil)
	if err == nil {
		t.Error("Starting server with nil handler: (*Server).Start(nil) did not return error")
		srv.Shutdown()
	}
}

func TestServerErrServerRunning(t *testing.T) {
	srv := NewServer(getRedisConnOpt(t), Config{LogLevel: testLogLevel})
	handler := NewServeMux()
	if err := srv.Start(handler); err != nil {
		t.Fatal(err)
	}
	err := srv.Start(handler)
	if err == nil {
		t.Error("Calling (*Server).Start(handler) on already running server did not return error")
	}
	srv.Shutdown()
}

func TestServerWithRedisDown(t *testing.T) {
	// Make sure that server does not panic and exit if redis is down.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic occurred: %v", r)
		}
	}()
	r := rdb.NewRDB(setup(t))
	testBroker := testbroker.NewTestBroker(r)
	srv := NewServer(getRedisConnOpt(t), Config{LogLevel: testLogLevel})
	srv.broker = testBroker
	srv.forwarder.broker = testBroker
	srv.heartbeater.broker = testBroker
	srv.processor.broker = testBroker
	srv.subscriber.broker = testBroker
	testBroker.Sleep()

	// no-op handler
	h := func(ctx context.Context, task *Task) error {
		return nil
	}

	err := srv.Start(HandlerFunc(h))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second)

	srv.Shutdown()
}

func TestServerWithFlakyBroker(t *testing.T) {
	// Make sure that server does not panic and exit if redis is down.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic occurred: %v", r)
		}
	}()
	r := rdb.NewRDB(setup(t))
	testBroker := testbroker.NewTestBroker(r)
	redisConnOpt := getRedisConnOpt(t)
	srv := NewServer(redisConnOpt, Config{LogLevel: testLogLevel})
	srv.broker = testBroker
	srv.forwarder.broker = testBroker
	srv.heartbeater.broker = testBroker
	srv.processor.broker = testBroker
	srv.subscriber.broker = testBroker

	c := NewClient(redisConnOpt)

	h := func(ctx context.Context, task *Task) error {
		// force task retry.
		if task.Type() == "bad_task" {
			return fmt.Errorf("could not process %q", task.Type())
		}
		time.Sleep(2 * time.Second)
		return nil
	}

	err := srv.Start(HandlerFunc(h))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_, err := c.Enqueue(NewTask("enqueued", nil), MaxRetry(i))
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Enqueue(NewTask("bad_task", nil))
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Enqueue(NewTask("scheduled", nil), ProcessIn(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}

	// simulate redis going down.
	testBroker.Sleep()

	time.Sleep(3 * time.Second)

	// simulate redis comes back online.
	testBroker.Wakeup()

	time.Sleep(3 * time.Second)

	srv.Shutdown()
}

func TestLogLevel(t *testing.T) {
	tests := []struct {
		flagVal string
		want    LogLevel
		wantStr string
	}{
		{"debug", DebugLevel, "debug"},
		{"Info", InfoLevel, "info"},
		{"WARN", WarnLevel, "warn"},
		{"warning", WarnLevel, "warn"},
		{"Error", ErrorLevel, "error"},
		{"fatal", FatalLevel, "fatal"},
	}

	for _, tc := range tests {
		level := new(LogLevel)
		if err := level.Set(tc.flagVal); err != nil {
			t.Fatal(err)
		}
		if *level != tc.want {
			t.Errorf("Set(%q): got %v, want %v", tc.flagVal, level, &tc.want)
			continue
		}
		if got := level.String(); got != tc.wantStr {
			t.Errorf("String() returned %q, want %q", got, tc.wantStr)
		}
	}
}
