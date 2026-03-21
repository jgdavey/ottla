Ottla
=====

Franz Kafka's favorite sister

## Usage

All of the examples assume the following require:

```clojure
(require '[ottla.core :as ottla])
```

### Initialization and config

The primary API operations take a config, which has at least the following keys:

- `:schema` - The postgres schema for the ottla logs and subscription tables.
- `:conn-map` - A map of parameters for connecting to the the DB.

Additionally, the administrative operations (create/remove topic, etc)
require a *connected* config, which is a config like the above that
includes a `:conn` key for the active database connection.

Unless you need more control, though, the following pattern is the
easiest way to start build and use this config. To set up a new ottla
system in a database:

```clojure
(ottla/with-connected-config [config (ottla/make-config {:database "fancy"
                                                         :user "bob"
                                                         :password "the-password"})]
  (ottla/init! config))
```

Which would connect to the postgres database and create the necessary
schema and tables within it.

### Adding a topic

After ottla has been initialized with the `ottla/init!` operation,
topics can be created with `add-topic!`:

```clojure
(ottla/with-connected-config [config {,,,}]
  (ottla/add-topic! config "my-new-topic"))
```

When creating a topic, you choose the underlying data type of the key
and value columns for the backing store with the `:key-type` and
`:value-type` options. The default is binary (`bytea` column).

Please note that not all columns types will be compatible with every
serializer. Below is the compatability chart for the built-in
serializers:

| serializer, type | bytea | text | jsonb |
|------------------|-------|------|-------|
| `:edn`           | [x]   | [x]  |       |
| `:json`          | [x]   | [x]  | [x]   |
| `:string`        | [x]   | [x]  |       |


For production use, `ensure-topic` is often preferable to `add-topic!` — it
returns the existing topic if it already exists with the same column types,
or creates it if it doesn't. It throws if the topic exists with different
column types, which guards against accidental schema drift:

```clojure
(ottla/with-connected-config [config {,,,}]
  (ottla/ensure-topic config "my-new-topic" :value-type :jsonb))
```

To list all existing topics:

```clojure
(ottla/with-connected-config [config {,,,}]
  (ottla/list-topics config))
;; => [{:topic "my-new-topic" :key-type :bytea :value-type :jsonb} ,,,]
```

### Removing a topic

After ottla has been initialized with the `ottla/init!` operation,
topics can be created with `remove-topic!`:

```clojure
(ottla/with-connected-config [config {,,,}]
  (ottla/remove-topic! config "my-new-topic"))
```

Note that is a *destructive* action, and all data from the topic will
be removed immediately.

### Log retention

For long-running topics, use `trim-topic!` to delete old records and prevent unbounded table growth. Exactly one mode must be provided:

```clojure
;; Delete all records with eid less than 1000
(ottla/trim-topic! config "my-topic" :before-eid 1000)

;; Delete all records older than a given timestamp
(ottla/trim-topic! config "my-topic" :before-timestamp #inst "2024-01-01")

;; Delete all records before the current maximum eid
;; The most recent record is always retained
(ottla/trim-topic! config "my-topic" :all? true)
```

`trim-topic!` returns the number of records deleted.

By default, the deletion is clamped to the minimum subscription cursor across all consumer groups. This prevents deleting records that have not yet been consumed. Pass `:ignore-subscriptions? true` to delete unconditionally:

```clojure
;; Delete unconditionally, regardless of subscriber position
(ottla/trim-topic! config "my-topic" :all? true :ignore-subscriptions? true)
```

Only the most recent record will be retained from the above.

### Producing data


To add to a topic's log, call `append!` with records, which are maps like the following:

- `:key` - data key
- `:value` - data value
- `:meta` - A map of optional metadata (analogous to Kafka headers)

There are several built-in serializers that can be applied to the key
and value. The key and value do not need to be the same column type or
use the same serializer.

The built-in serializers can be referenced by keyword instead of a
function. `:string`, `:json`, or `:edn`

When the key are value are stored in binary format, a key and/or value
serializer must be provided if the key or value are not already binary
data. A serializer is a function that takes a key or value and returns
a binary representation that can later be deserialized by consumers.
As a simple example, here's an edn serializer and deserializer:

```clojure
(def charset java.nio.charset.StandardCharsets/UTF_8)

(defn serialize-edn
  [obj]
  (.getBytes (pr-str obj) charset))

(defn deserialize-edn
  [ba]
  (with-open [rdr (java.io.PushbackReader.
                   (java.io.InputStreamReader.
                    (java.io.ByteArrayInputStream. ba)
                    charset))]
    (clojure.edn/read rdr)))
```

So, assuming these serializing functions, here's how we might insert edn data into a topic:

```clojure
(ottla/append! config "my-new-topic"
               [{:value {:oh "cool"}} ,,,]
               {:serialize-value serialize-edn})
```

Note that this is already provided as a built-in `:edn` serializer,
but the above is for demonstration.

To insert a single record, use `append-one!`:

```clojure
(ottla/append-one! config "my-new-topic"
                   {:key "user-123" :value {:event "login"}}
                   {:serialize-key :string :serialize-value :json})
```

### Consumers

Consumers asynchronously listen for messages on a topic and run a
handler to deal with them, updating the subscription afterwards.
Consumers are designed to be run in a managed `Consumer` object, which
can be started like this:

```clojure
(ottla/start-consumer config {:topic "my-new-topic"} handler {:deserialize-value deserialize-edn})
```

This will spin up and return a `Consumer` that should be kept around
until ready to stop with `(.close consumer)`. Consumers will usually be
long-lived and can be managed with whatever component lifecycle
framework you choose, but can also be used with `with-open` for
short-lived consumers.

If no subscription exists for the given topic and group, one is created
automatically at startup with the cursor set to 0, so the consumer will
read from the beginning of the topic. To start from a specific point,
call `reset-consumer-offset!` before starting the consumer.

```clojure
;; Graceful shutdown — waits up to await-close-ms for in-flight work to finish
(.close consumer)

;; Or use with-open for short-lived consumers
(with-open [consumer (ottla/start-consumer config {:topic "my-new-topic"} handler opts)]
  (Thread/sleep 5000))
```

A Consumer maintains 2 database connections: one solely for
listening for real-time notifications from the database, and one for
periodic fetching of records. This latter connection can be reused in
handlers.

#### Consumer groups

The `:group` key in the selection map identifies an independent consumer
group. Multiple groups can read the same topic and each maintains its own
offset, so they receive all records independently:

```clojure
(ottla/start-consumer config {:topic "my-new-topic" :group "indexer"} handler opts)
(ottla/start-consumer config {:topic "my-new-topic" :group "notifier"} handler opts)
```

A handler is a function of 2 args, the ottla connected config and a
vector of records. It will be called on its own Thread, but will
receive records in order. Each invocation may receive up to `:max-records`
records at once, so handlers should be written to process a variable-sized
batch rather than assuming a single record.

The options arg accepts the following keys:

- `:poll-ms` - how often to poll the database for new records on the
  topic. This is primarily used as a fallback in case of LISTEN/NOTIFY
  misses. (default 15000)
- `:await-close-ms` - when closing the Consumer, how long to wait for
  all threads to completely finish their work before shutting it down
  forcibly.
- `:max-records` - maximum number of records to fetch per batch (default 100)
- `:deserialize-key` - a deserializer for the record keys
- `:deserialize-value` - a deserializer for the record values
- `:exception-handler` - a function that will receive any uncaught
  Exception object (see below)
- `:xform` - optional transducer applied to records after deserialization
- `:commit-mode` - controls when the consumer offset is advanced:
  - `:auto` (default) — commits after each successful batch fetch, before calling the handler
  - `:tx-wrap` — wraps the fetch and handler call in a single transaction; commits only if the handler returns without throwing
  - `:manual` — never commits automatically; use `commit-offset!` in your handler

Uncaught exceptions thrown either in the handler, any deserializer, or
from fetching will be caught and fed to the `exception-handler`, which
by default just prints the exception, but could instead log it or in
some other way act on it. If the exception-handler returns
`ottla/shutdown` (i.e. `:ottla/shutdown`), the Consumer will begin its
shutdown process.

#### Handler performance

The handler runs on a single worker thread. If the handler is slow, work
will queue up and LISTEN/NOTIFY-triggered fetches may be dropped (the
worker uses a bounded queue with a discard-oldest policy). Long-running
work should be handed off to a separate thread pool inside the handler.

#### Delivery guarantees

Ottla provides **at-least-once** delivery. Records will not be skipped,
but may be delivered more than once if the consumer crashes after fetching
but before committing. The `:tx-wrap` commit mode minimizes this window by
committing the offset within the same transaction as the handler.

#### Replaying records

To replay a topic from the beginning or from a specific point, reset the
consumer group's offset before starting (or while stopped):

```clojure
;; Replay everything from the start
(ottla/reset-consumer-offset! config {:topic "my-new-topic" :group "default"} 0)

;; Replay from a specific record id
(ottla/reset-consumer-offset! config {:topic "my-new-topic" :group "default"} 42)
```

### Monitoring

`list-subscriptions` returns all consumer group offsets along with the current max eid and calculated lag for each:

```clojure
(ottla/with-connected-config [config {,,,}]
  (ottla/list-subscriptions config))
;; => [{:topic "my-new-topic" :group "default"
;;      :offset 42 :topic-eid 50 :lag 8
;;      :updated-at      #inst "2024-01-01T12:00:05Z"
;;      :timestamp       #inst "2024-01-01T12:00:00Z"
;;      :topic-timestamp #inst "2024-01-01T12:05:00Z"
;;      :timestamp-lag   #object[java.time.Duration "PT5M"]
;;      :processing-delay #object[java.time.Duration "PT5S"]} ,,,]
```

- `:lag` is the count of unread records; `0` means fully caught up
- `:updated-at` is the `java.time.Instant` when the consumer last committed its offset; `nil` if the subscription has never consumed a record
- `:timestamp-lag` is a `java.time.Duration` between the last consumed record and the latest record; `nil` if the topic is empty or `:offset` is `0`
- `:processing-delay` is a `java.time.Duration` from when the last consumed record was published to when the consumer committed it; `nil` if `:updated-at` or `:timestamp` is `nil`
- Topics with no subscriptions do not appear in this list

`topic-subscriptions` returns the same information grouped by topic. Every topic appears in the result, even those with no subscribers:

```clojure
(ottla/with-connected-config [config {,,,}]
  (ottla/topic-subscriptions config))
;; => [{:topic "my-new-topic"
;;      :subscriptions [{:group "default"
;;                       :offset 42 :topic-eid 50 :lag 8
;;                       :updated-at       #inst "2024-01-01T12:00:05Z"
;;                       :timestamp        #inst "2024-01-01T12:00:00Z"
;;                       :topic-timestamp  #inst "2024-01-01T12:05:00Z"
;;                       :timestamp-lag    #object[java.time.Duration "PT5M"]
;;                       :processing-delay #object[java.time.Duration "PT5S"]}]}
;;     {:topic "unused-topic"
;;      :subscriptions []} ,,,]
```

The subscription maps inside `:subscriptions` carry the same keys as `list-subscriptions` entries, minus `:topic` (which is on the outer map).
