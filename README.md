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

### Removing a topic

After ottla has been initialized with the `ottla/init!` operation,
topics can be created with `remove-topic!`:

```clojure
(ottla/with-connected-config [config {,,,}]
  (ottla/remove-topic! config "my-new-topic"))
```

Note that is a *destructive* action, and all data from the topic will
be removed immediately.

### Producing data


To add to a topic's log, call `append` with records, which are maps like the following:

- `:key` - data key
- `:value` - data value
- `:meta` - A map of optional metadata (analogous to Kafka headers)

The key are value are stored in binary format, so a key and/or value
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
(ottla/append config "my-new-topic"
              [{:value {:oh "cool"}} ,,,]
              {:serialize-value serialize-edn})
```

### Consumers

Consumers are designed to be run in a managed `Consumer` object, which can be started like this:

```clojure
(ottla/start-consumer config {:topic "my-new-topic"} handler {:deserialize-value deserialize-edn})
```

This will spin up and return a `Consumer` that should be kept around
until read to stop with `(.close consumer)`. Consumers will usually be
long-lived and can be managed with whatever component lifecycle
framework you choose, but can also be used with `with-open` for
short-lived consumers.

A Consumer will maintain 2 database connections: one solely for
listening for real time notifications from the database, and one for
periodic fetching of records. This latter connection can be reused in
handlers.

A handler is a function of 2 args, the ottla connected config and a
vector of records. It will be called on its own Thread, but will
receive records in order.

The options arg accepts the following keys:

- `:poll-ms` - how often to poll the database for new records on the
  topic. This is primarily used as a fallback in case of LISTEN/NOTIFY
  misses. (default 15000)
- `:await-close-ms` - when closing the Consumer, how long to wait for
  all threads to completely finish their work before shutting it down
  forcibly.
- `:deserialize-key` - a deserializer for the record keys
- `:deserialize-value` - a deserializer for the record values
- `:exception-handler` - a function that will receive any uncaught
  Exception object (see below)
- `:xform` - optional transducer for records (after deserialize)

Uncaught exceptions thrown either in the handler, any deserializer, or
from fetching will be caught and fed to the `exception-handler`, which
by default just prints the exception, but could instead log it or in
some other way act on it. If this exception-handler returns the
keyword `:ottla/shutdown`, the Consumer will begin its shutdown
process.
