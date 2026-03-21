(ns ottla.core
  (:require [ottla.postgresql :as postgres]
            [ottla.consumer :as consumer]
            [ottla.serde.edn]
            [ottla.serde.json]
            [ottla.serde.string])
  (:import [java.lang AutoCloseable]
           [ottla.consumer Consumer]))

(defn make-config
  "Build a config map for use with ottla functions.
    - `conn-map` a pg2 connection map (host, port, database, user, password, etc.)
    - `opts`     optional keyword args:
       - `:schema` the PostgreSQL schema name to use (default: \"ottla\")"
  [conn-map & {:as opts}]
  (merge {:schema "ottla"}
         opts
         {:conn-map conn-map}))

(def shutdown :ottla/shutdown)

(defmacro with-connected-config
  "Ensures `config` has an open database connection for the duration of `body`,
  binding the connected config to `sym`. If the config is already connected,
  the existing connection is used as-is. Otherwise, a new connection is opened
  and automatically closed when `body` completes (even on exception)."
  [[sym config] & body]
  `(let* [config# ~config]
     (if (:conn config#)
       (let* [~sym config#]
         ~@body)
       (let* [config# (postgres/connect-config config#)
              ~sym config#]
         (try
           ~@body
           (finally
             (.close ^AutoCloseable (:conn config#))))))))

(defn init!
  "Create all necessary tables and functions in a connected postgres database.
  `config` should be a connected config map"
  [config]
  (postgres/ensure-schema config))

(defn add-topic!
  "Create a new topic
    - `config`   a connected config map
    - `topic`    the name of the topic that will be published to and subscribed to
    - `opts`     optional map containing any of the following keys:
       - `:value-type` column type for record values
       - `:key-type`   column type for record keys

  Both key-type and value-type can be any of :bytea, :jsonb, or :text

  The default is :bytea (binary data) and can handle any serializer.

  :jsonb works great with the :json serializer and may also work with,
  e.g., a transit serializer (supplied by you)

  :text works with textual serializers that return strings. The
  built-in serializers :edn, :json, and :string all return strings."
  [config topic & {:as opts}]
  (postgres/create-topic config (name topic) opts))

(defn ensure-topic
  "Find or create a topic. This takes the same arguments as `add-topic!`.
  If the topic already exists with the same key-type and value-type, returns
  it unchanged. Throws if the topic exists but was created with different
  column types."
  [config topic & {:as opts}]
  (postgres/ensure-topic config (name topic) opts))

(defn list-topics
  "Returns a vector of all topics in the ottla schema, ordered by name.
  Each topic is a map with :topic, :key-type, and :value-type."
  [config]
  (postgres/list-topics config))

(defn list-subscriptions
  "Returns a vector of all subscriptions, ordered by topic and group.
  Each subscription is a map with:
    - :topic             the topic name
    - :group             the consumer group id
    - :offset            the last committed eid for this group
    - :topic-eid         the highest eid available in the topic
    - :lag               the number of unread records (topic-eid - offset)
    - :timestamp         java.time.Instant of the last consumed record (nil if offset is 0)
    - :topic-timestamp   java.time.Instant of the latest record in the topic (nil if empty)
    - :timestamp-lag     java.time.Duration between subscription and topic timestamps (nil if either is nil)

   Options:
    - `:topics`  a collection of topic names to filter by. When
                 not specified, all topics will be fetched"
  [config & {:as opts}]
  (postgres/list-subscriptions config opts))

(defn remove-topic!
  "Remove a topic. Warning: permanently and immediately removes all records for the topic.
    - `config`   a connected config map
    - `topic`    the name of the topic "
  [config topic]
  (postgres/delete-topic config (name topic)))

(defn start-consumer
  "Start a consumer process. This will spin up several worker threads to
  handle the machinery of listening to the topic, and at least 2
  database connections (one for LISTEN/NOTIFY and one for fetching
  and committing)

    - `selection` a topic (string) or a map:
       - `:topic`        (string) name of topic to listen to
       - `:group`        (string) name of listening group (default: \"default\")
       - `:commit-mode`  #{:manual :auto :tx-wrap} (default: :auto)
    - `handler` a function that will receive a sequence of deserialized records
    - `opts` option map:
      - `:poll-ms`           (int) how often to fallback to polling
      - `:await-close-ms`    (int) how long to wait when closing consumer
      - `:listen-ms`         (int) how often to check for new messages (fast)
      - `:exception-handler` (fn [e]) handle raised exceptions
      - `:xform`             (fn) transducer for post-deserialization processing
      - `:deserialize-key`   Key deserializer. Keyword or function
      - `:deserialize-value` Value deserializer. Keyword or function
      - `:max-records`       (int) max records fetched per batch (default 100)

  Deserializers do the opposite of serializers, turning a serialized
  value into a deserialized one. The same built-ins are available as
  for the serializers: :string, :json, and :edn
  "
  ^Consumer
  [config selection handler & {:as opts}]
  (consumer/start-consumer config selection handler opts))

(defn commit-offset!
  "Only use with consumer tx-mode `:manual`.

    - `selection` a topic (string) or a map:
       - `:topic`    (string) name of topic to listen to
       - `:group`    (string) name of listening group (default: \"default\")
    - `message`   the record that has been processed

  The selection must match the what was passed to `start-consumer`.

  Note: when using commit-mode `:auto` and `:tx-wrap`, this is handled
  automatically."
  [config selection record]
  (postgres/commit-offset! config selection (:eid record)))

(defn reset-consumer-offset!
  "Change a consumer's offset to the provided number exactly.
  Use `0` as the new-offset to replay all records from the earliest
  available."
  [config selection new-offset]
  (postgres/reset-offset! config selection new-offset))

(defn append!
  "Add records to a topic stream.
    - `config`   a connected config map
    - `topic`    the name of the topic (must already exist)
    - `records`  a seq of records to insert (see below)
    - `opts`     option map:
       - `:serialize-key`   Key serializer. Must be a keyword or function.
       - `:serialize-value` Value serializer. Must be a keyword or function.

  The default serializers are no-op and take the data as-is, but there
  are other built-in serializers available including `:json`, `:edn`,
  and `:string`. Alternatively, a function that takes a single arg can
  be passed. It will be invoked with the key or value.

  Records are maps with the following:
    - `:key`   (required, even if `nil`)
    - `:value` (required)
    - `:meta`  (optional) map of metadata
  "
  [config topic records & {:as opts}]
  (postgres/insert-records config (name topic) records opts))

(defn append-one!
  "Like append! but for a single record"
  [config topic record & {:as opts}]
  (postgres/insert-records config (name topic) [record] opts))
