(ns ottla.core
  (:require [ottla.postgresql :as postgres]
            [ottla.consumer :as consumer])
  (:import [java.lang AutoCloseable]
           [ottla.consumer Consumer]))

(defn make-config
  [conn-map & {:as opts}]
  (merge {:schema "ottla"}
         opts
         {:conn-map conn-map}))

(def shutdown :ottla/shutdown)

(defmacro with-connected-config
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

(defn remove-topic!
  "Remove a topic. Warning: permanently and immediately removes all records for the topic.
    - `config`   a connected config map
    - `topic`    the name of the topic "
  [config topic]
  (postgres/delete-topic config (name topic)))

(defn start-consumer
  "Start a consumer process. This will spin up several worker threads to
  handle the machinery of listinging to the topic, and at least 2
  database connections (one for NOTIFY/LISTING and one for fetching
  and commiting)

    - `selection` a topic (string) or a map:
       - `:topic`    (string) name of topic to listen to
       - `:group`    (string) name of listening group (default: \"default\")
       - `:tx-mode`  #{:manual :auto :tx-wrap} (default: :auto)
    - `handler` a function that will receive a sequence of deserialized records
    - `opts` option map:
      - `:poll-ms`           (int) how often to fallback to polling
      - `:await-close-ms`    (int) how long to wait when closing consumer
      - `:listen-ms`         (int) how often to check for new messages (fast)
      - `:exception-handler` (fn [e]) handle raised exceptions
      - `:xform`             (fn) transducer for post-deserialization processing
      - `:deserialize-key`   Key deserializer. Keyword or function
      - `:deserialize-value` Value deserializer. Keyword or function

  Deserializers do the opposite of serializers, turning a serialized
  value into a deserialized one. The same built-ins are available as
  for the serializers: :string, :json, and :edn
  "
  ^Consumer
  [config selection handler & {:as opts}]
  (consumer/start-consumer config selection handler opts))

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
