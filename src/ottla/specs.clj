(ns ottla.specs
  "Specs for the core API of ottla"
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [ottla.core :as ottla]
            [ottla.postgresql :as postgres]))

(set! *warn-on-reflection* true)

(def idfn (s/with-gen fn?
            #(gen/return identity)))

(defn- sql-identifier-gen []
  (gen/fmap (fn [[c1 rest]]
              (apply str c1 rest))
        (gen/tuple (gen/char-alpha)
                   (gen/vector (gen/frequency [[100 (gen/char-alphanumeric)]
                                               [1 (gen/return \_)]])
                               0 41))))

;;; Config

(s/def :ottla.config/conn-map map?)
(s/def :ottla.config/schema (s/with-gen postgres/legal-identifier?
                              sql-identifier-gen))
(s/def :ottla.config/conn any?)

(s/def :ottla/config (s/keys :req-un [:ottla.config/conn-map :ottla.config/schema]))
(s/def :ottla/connected-config (s/keys :req-un [:ottla.config/conn-map
                                                :ottla.config/schema
                                                :ottla.config/conn]))

(s/fdef postgres/connect-config
  :args (s/cat :config :ottla/config)
  :ret :ottla/connected-config
  :fn (fn [{:keys [args ret]}]
        (=
         (dissoc (get args :config) :conn)
         (dissoc ret :conn))))

;;; Topics

(s/def :ottla.topic/topic (s/and string?
                                 #(pos? (count %))))
(s/def :ottla.topic/table-name (s/and string?
                                      #(pos? (count %))))
(s/def :ottla.topic/key-type #{:text :bytea :jsonb})
(s/def :ottla.topic/value-type #{:text :bytea :jsonb})
(s/def :ottla.topic/index-key? boolean?)

(s/def :ottla/topic-map (s/keys :req-un [:ottla.topic/topic
                                         :ottla.topic/key-type
                                         :ottla.topic/value-type]
                                :opt-un [:ottla.topic/table-name]))

(s/fdef ottla/add-topic!
  :args (s/cat :config :ottla/config
               :ottla.topic :ottla.topic/topic
               :opts (s/keys* :opt-un [:ottla.topic/key-type
                                       :ottla.topic/value-type
                                       :ottla.topic/index-key?]))
  :ret :ottla/topic-map)

(s/fdef ottla/ensure-topic
  :args (s/cat :config :ottla/config
               :topic :ottla.topic/topic
               :opts (s/keys* :opt-un [:ottla.topic/key-type
                                       :ottla.topic/value-type
                                       :ottla.topic/index-key?]))
  :ret :ottla/topic-map)

(s/fdef ottla/remove-topic!
  :args (s/cat :config :ottla/config
               :ottla.topic :ottla.topic/topic))

(s/fdef ottla/list-topics
  :args (s/cat :config :ottla/config)
  :ret (s/coll-of :ottla/topic-map))

;;; Records and append

(def serialize-spec (s/with-gen
                      (s/or :kw keyword?
                            :fn fn?)
                      #(s/gen #{:string :json :edn})))

(s/def :ottla.append/serialize-key serialize-spec)
(s/def :ottla.append/serialize-value serialize-spec)

(s/def :ottla.record/eid pos-int?)
(s/def :ottla.record/key any?)
(s/def :ottla.record/value any?)
(s/def :ottla.record/meta (s/nilable (s/map-of keyword? any?)))

(s/def :ottla/record (s/keys :req-un [:ottla.record/key
                                      :ottla.record/value]
                             :opt-un [:ottla.record/meta]))

(s/fdef ottla/append!
  :args (s/cat :config :ottla/config
               :topic :ottla.topic/topic
               :records (s/coll-of :ottla/record :min-count 1)
               :opts (s/keys* :opt-un [:ottla.append/serialize-key
                                       :ottla.append/serialize-value])))

(s/fdef ottla/append-one!
  :args (s/cat :config :ottla/config
               :topic :ottla.topic/topic
               :record :ottla/record
               :opts (s/keys* :opt-un [:ottla.append/serialize-key
                                       :ottla.append/serialize-value])))

;;; Trim

(s/def :ottla.trim/before-eid pos-int?)
(s/def :ottla.trim/before-timestamp inst?)
(s/def :ottla.trim/all? true?)
(s/def :ottla.trim/ignore-subscriptions? boolean?)

(s/fdef ottla/trim-topic!
  :args (s/cat :config :ottla/config
               :topic :ottla.topic/topic
               :opts (s/keys* :opt-un [:ottla.trim/before-eid
                                       :ottla.trim/before-timestamp
                                       :ottla.trim/all?
                                       :ottla.trim/ignore-subscriptions?]))
  :ret nat-int?)

;;; Selection and consumers

(s/def :ottla.selection/topic :ottla.topic/topic)
(s/def :ottla.selection/group string?)
(s/def :ottla.selection/commit-mode #{:auto :manual :tx-wrap})

(s/def :ottla/selection
  (s/or :topic-name string?
        :map (s/keys :req-un [:ottla.selection/topic]
                     :opt-un [:ottla.selection/group
                               :ottla.selection/commit-mode])))

(s/def :ottla.consumer/poll-ms pos-int?)
(s/def :ottla.consumer/await-close-ms pos-int?)
(s/def :ottla.consumer/listen-ms pos-int?)
(s/def :ottla.consumer/reconnect-ms pos-int?)
(s/def :ottla.consumer/max-records pos-int?)
(s/def :ottla.consumer/deserialize-key serialize-spec)
(s/def :ottla.consumer/deserialize-value serialize-spec)
(s/def :ottla.consumer/xform idfn)
(s/def :ottla.consumer/exception-handler idfn)

(def ^:private consumer-opt-keys
  [:ottla.consumer/poll-ms
   :ottla.consumer/await-close-ms
   :ottla.consumer/listen-ms
   :ottla.consumer/reconnect-ms
   :ottla.consumer/max-records
   :ottla.consumer/deserialize-key
   :ottla.consumer/deserialize-value
   :ottla.consumer/xform
   :ottla.consumer/exception-handler])

(eval
 `(s/def :ottla/consumer-opts
    (s/alt
     :kwargs (s/keys* :opt-un ~consumer-opt-keys)
     :map (s/keys :opt-un ~consumer-opt-keys))))

(s/fdef ottla/start-consumer
  :args (s/cat :config :ottla/config
               :selection :ottla/selection
               :handler fn?
               :opts :ottla/consumer-opts))

(s/fdef ottla/commit-offset!
  :args (s/cat :config :ottla/config
               :selection :ottla/selection
               :record (s/keys :req-un [:ottla.record/eid])))

(s/fdef ottla/reset-consumer-offset!
  :args (s/cat :config :ottla/config
               :selection :ottla/selection
               :new-offset nat-int?))

;;; Subscriptions

(s/def :ottla.subscription/from
  (s/or :keyword #{:earliest :latest}
        :eid nat-int?))

(s/fdef ottla/ensure-subscription
  :args (s/cat :config :ottla/config
               :selection :ottla/selection
               :opts (s/keys* :opt-un [:ottla.subscription/from]))
  :ret boolean?)

(s/fdef ottla/add-subscription!
  :args (s/cat :config :ottla/config
               :selection :ottla/selection
               :opts (s/keys* :opt-un [:ottla.subscription/from]))
  :ret true?)

;;; Monitoring

(s/def :ottla.subscription/group string?)
(s/def :ottla.subscription/offset nat-int?)
(s/def :ottla.subscription/topic-eid nat-int?)
(s/def :ottla.subscription/lag nat-int?)
(s/def :ottla.subscription/updated-at (s/nilable #(instance? java.time.Instant %)))
(s/def :ottla.subscription/timestamp (s/nilable #(instance? java.time.Instant %)))
(s/def :ottla.subscription/topic-timestamp (s/nilable #(instance? java.time.Instant %)))
(s/def :ottla.subscription/timestamp-lag (s/nilable #(instance? java.time.Duration %)))
(s/def :ottla.subscription/processing-delay (s/nilable #(instance? java.time.Duration %)))

(s/def :ottla/subscription
  (s/keys :req-un [:ottla.topic/topic
                   :ottla.subscription/group
                   :ottla.subscription/offset
                   :ottla.subscription/topic-eid
                   :ottla.subscription/lag
                   :ottla.subscription/updated-at
                   :ottla.subscription/timestamp
                   :ottla.subscription/topic-timestamp
                   :ottla.subscription/timestamp-lag
                   :ottla.subscription/processing-delay]))

(s/def :ottla.list-subscriptions/selection-item
  (s/or :topic-name string?
        :map (s/keys :req-un [:ottla.selection/topic]
                     :opt-un [:ottla.selection/group])))

(s/def :ottla.list-subscriptions/selections
  (s/coll-of :ottla.list-subscriptions/selection-item))

(s/fdef ottla/list-subscriptions
  :args (s/cat :config :ottla/config
               :opts (s/keys* :opt-un [:ottla.list-subscriptions/selections]))
  :ret (s/coll-of :ottla/subscription))

(s/def :ottla/topic-with-subscriptions
  (s/keys :req-un [:ottla.topic/topic
                   :ottla/subscriptions]))

(s/def :ottla/subscriptions
  (s/coll-of (s/keys :req-un [:ottla.subscription/group
                               :ottla.subscription/offset
                               :ottla.subscription/topic-eid
                               :ottla.subscription/lag
                               :ottla.subscription/updated-at
                               :ottla.subscription/timestamp
                               :ottla.subscription/topic-timestamp
                               :ottla.subscription/timestamp-lag
                               :ottla.subscription/processing-delay])))

(s/fdef ottla/topic-subscriptions
  :args (s/cat :config :ottla/config
               :opts (s/keys* :opt-un [:ottla.list-subscriptions/selections]))
  :ret (s/coll-of :ottla/topic-with-subscriptions))
