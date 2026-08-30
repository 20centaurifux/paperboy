# Paperboy

![Paperboy](doc/Paperboy.jpeg)

`Paperboy` is a Clojure abstraction for reliably passing messages to arbitrary brokers. It provides the building blocks needed to buffer, order, claim, deliver, and acknowledge messages between an application and a destination system.

For example, `Paperboy` can provide a persistent buffer between an application and a messaging system such as MQTT. More generally, it can sit in front of any destination that may be temporarily unavailable: the application submits a message to `Paperboy`, which can persist and deliver it independently of the calling code and retry it later when necessary.

> **Project status:** `Paperboy` is at an early stage of development. Its API, protocols, data model, and behavior are still subject to change. The current implementations primarily exist to develop and validate the architecture and its concurrent workflows. Persistent storage and network destinations are not included yet.

## Architecture

`Paperboy` consists of three separate components:

1. The **Spooler** accepts new messages from the application.
2. The **Queue** coordinates available, in-flight, and successfully delivered messages.
3. The **Broker** takes messages from the queue and transmits them to the destination system.

![`Paperboy` architecture](doc/architecture.png)

The components communicate through small Clojure protocols. Each implementation can therefore be replaced independently. A persistent spooler does not need to know which concrete queue or broker is being used. Similarly, a broker does not need to know whether messages were persisted in memory, a file, or a database.

### Envelopes and messages

`Paperboy` transports each message inside an envelope:

```clojure
{:path "/sensors/temperature"
 :message {:id "1"
           :payload "21.5"}}
```

An envelope contains:

- `:path`: the logical destination, such as a topic, channel, or routing key;
- `:message`: the message itself, containing:
  - `:id`: a unique identifier used to coordinate delivery and deletion;
  - `:payload`: the content to transmit.

### Message flow

![`Paperboy` message flow](doc/message-flow.png)

The message flow separates accepting a message from delivering it and from cleaning up its persisted copy.

1. The application submits a path and payload to the **Spooler** using `submit!`.
2. The Spooler assigns a unique ID and creates an envelope. A persistent implementation stores the message before making it available for delivery.
3. The Spooler passes the envelope to the **Queue** using `put!`.
4. The Queue makes the envelope available and notifies a waiting **Broker**.
5. The Broker waits for work with `await-ready!` and reserves the next envelope with `claim!`.
6. Claiming prevents another Broker from processing the same delivery concurrently. It does not mean that the message has been delivered successfully.
7. The Broker transmits the envelope to its destination.

If transmission succeeds, the Broker calls `ack!`. The Queue records the successful delivery and notifies the Spooler that the corresponding persisted message may be removed. The Spooler's cleanup worker waits for this notification with `await-removed!`, obtains acknowledged message IDs in batches through `drain-removed!`, and deletes their persisted copies. A batch is completed only when the Spooler's removal function reports success; otherwise, the removal request is retained for a later attempt.

If transmission fails, the Broker must not call `ack!`. The delivery remains unacknowledged, and the Broker is responsible for buffering the in-flight message and retrying it later. This allows each Broker implementation to apply the retry, backoff, and destination-specific error-handling strategy appropriate for its transport.

### Queue implementations

`paperboy.queue.fifo` offers a simple in-memory FIFO queue. It keeps all waiting envelopes in insertion order and makes them available to Brokers one at a time.

`paperboy.queue.quota` offers an in-memory queue with per-path quotas. It accepts a default quota and optional route-specific quotas:

```clojure
(quota/quota {:size 100
              :routes {"/tenants/a/*" {:size 10}
                       "/alerts/?" {:size 20}}})
```

Quota route patterns use the same path segment rules as envelopes, plus two wildcards:

- `?` matches exactly one path segment.
- `*` matches the rest of the path.

When an envelope is put into the queue, its path is matched against the quota routes. A match uses the matching route's `:size`, but the bucket is still the concrete matched path. For example, `/tenants/a/one` and `/tenants/a/two` both match `/tenants/a/*`, but they are stored in separate buckets, each with size `10`. Paths that match no route use the default `:size` and the default bucket.

When a bucket is full, adding a newer envelope evicts older waiting envelopes from that bucket. Evicted message IDs become ready for cleanup through `await-removed!` and `drain-removed!`, just like acknowledged message IDs.

## Example

The following example wires the three components together and deliberately submits messages before starting the Broker. This demonstrates that accepting a message and delivering it are independent operations.

```clojure
(ns example
  (:require [paperboy.api :as api]
            ;; The FIFO implements both sides of the Queue abstraction:
            ;; Producer for the Spooler and Consumer for the Broker.
            [paperboy.queue.fifo :as pq]

            ;; Passthrough is the current non-persistent Spooler. It creates
            ;; envelopes and forwards them directly to the Queue.
            [paperboy.spooler.passthrough :as pp]

            ;; Stdout is a simple Broker that prints claimed messages and
            ;; acknowledges them after they have been written successfully.
            [paperboy.broker.stdout :as pb]))

;; Create the shared Queue. Both the Spooler and Broker receive this same
;; instance, but interact with it through different protocols.
(def queue (pq/fifo))

;; Connect the Spooler to the Producer side of the Queue.
(def spooler (pp/passthrough queue))

;; Start the Spooler's cleanup worker. It waits for acknowledged message IDs
;; and removes their persisted copies. Passthrough has no external storage,
;; so removal always succeeds, while still exercising the complete workflow.
(api/start! spooler)

;; Submit two messages. The Spooler assigns an ID to each payload, creates an
;; envelope, and puts it into the Queue. At this point the Broker has not been
;; started, so the messages remain available in FIFO order.
(api/submit! spooler "/foo" "hello")
(api/submit! spooler "/bar" "world")

;; Connect the Stdout Broker to the Consumer side of the same Queue.
(def broker (pb/stdout queue))

;; Start the Broker worker. It waits for available messages, claims them one
;; at a time, writes them to stdout, and acknowledges successful deliveries.
;; The acknowledgements wake the Spooler's cleanup worker.
(api/start! broker)
```

The example creates background worker threads. In a long-running application they normally live for the lifetime of the application. During controlled shutdown, stop both components explicitly:

```clojure
(api/stop! broker)
(api/stop! spooler)
```