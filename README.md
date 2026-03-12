# Asynq — Enhanced Fork

Fork of [hibiken/asynq](https://github.com/hibiken/asynq) with two additional features:

1. **Per-queue concurrency control**
2. **Redis Pub/Sub task-ready notifications**

For upstream documentation, see the [original README](https://github.com/hibiken/asynq/blob/master/README.md).

---

## Installation

Add asynq as a dependency, then point it to this fork with a `replace` directive:

```bash
go get github.com/hibiken/asynq@v0.26.0
go mod edit -replace github.com/hibiken/asynq=github.com/blueswimmingfish/asynq@v0.26.0-ext.1
go mod tidy
```

This adds the following to your `go.mod`:

```
require github.com/hibiken/asynq v0.26.0

replace github.com/hibiken/asynq => github.com/blueswimmingfish/asynq v0.26.0-ext.1
```

Import path stays the same — no code changes needed:

```go
import "github.com/hibiken/asynq"
```

---

## Per-Queue Concurrency Control

Limit the number of concurrent workers per queue, preventing a single queue from consuming all worker slots.

```go
srv := asynq.NewServer(redisOpt, asynq.Config{
    Concurrency: 10,
    Queues: map[string]int{
        "critical": 6,
        "default":  3,
        "low":      1,
    },
    QueueConcurrency: map[string]int{
        "critical": 4, // at most 4 concurrent workers for "critical"
    },
})
```

Queues can be added or updated at runtime:

```go
srv.AddQueue("notifications", 5, 3) // priority=5, concurrency=3
srv.SetQueueConcurrency("notifications", 5)
```

---

## Redis Pub/Sub Task-Ready Notifications

Reduce task pickup latency from ~1s (polling) to near-instant by notifying the server via Redis Pub/Sub when tasks are enqueued. Polling remains as a fallback.

**Server:**

```go
srv := asynq.NewServer(redisOpt, asynq.Config{
    Concurrency:            10,
    EnableTaskNotification: true,
    TaskCheckInterval:      10 * time.Second, // safe to increase with pub/sub enabled
})
```

**Client:**

```go
client := asynq.NewClientWithNotification(redisOpt)
defer client.Close()

client.Enqueue(asynq.NewTask("email:send", payload)) // notifies server immediately
```

`NewClient` (without notification) still works — the server falls back to polling.

---

## License

[MIT](./LICENSE)
