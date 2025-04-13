(ns ottla.specs
  "Specs for the core API of ottla"
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [ottla.core :as ottla]
            [ottla.postgresql :as postgres]))

(set! *warn-on-reflection* true)

(defn- sql-identifier-gen []
  (gen/fmap (fn [[c1 rest]]
              (apply str c1 rest))
        (gen/tuple (gen/char-alpha)
                   (gen/vector (gen/frequency [[100 (gen/char-alphanumeric)]
                                               [1 (gen/return \_)]])
                               0 41))))

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

(s/def :ottla.topic/topic (s/and string?
                                 #(pos? (count %))))
(s/def :ottla.topic/table-name (s/and string?
                                      #(pos? (count %))))
(s/def :ottla.topic/key-type #{:text :bytea :jsonb})
(s/def :ottla.topic/value-type #{:text :bytea :jsonb})

(s/def :ottla/topic-map (s/keys :req-un [:ottla.topic/topic
                                         :ottla.topic/key-type
                                         :ottla.topic/value-type]
                                :opt-un [:ottla.topic/table-name]))

(s/fdef ottla/add-topic!
  :args (s/cat :config :ottla/config
               :ottla.topic :ottla.topic/topic
               :opts (s/keys* :opt-un [:ottla.topic/key-type :ottla.topic/value-type]))
  :ret :ottla/topic-map)

(s/fdef ottla/remove-topic!
  :args (s/cat :config :ottla/config
               :ottla.topic :ottla.topic/topic))

(def serialize-spec (s/with-gen
                      (s/or :kw keyword?
                            :fn fn?)
                      #(s/gen #{:string :json :edn})))

(s/def :ottla.append/serialize-key serialize-spec)
(s/def :ottla.append/serialize-value serialize-spec)

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

(s/def :ottla.selection/topic :ottla.topic/topic)
(s/def :ottla.selection/group string?)
(s/def :ottla.selection/tx-mode #{:auto :manual :tx-wrap})

(s/def :ottla/selection (s/keys :req-un [:ottla.selection/topic]
                                :opt-un [:ottla.selection/group
                                         :ottla.selection/tx-mode]))

(s/def :ottla.consumer/poll-ms int?)
(s/def :ottla.consumer/await-close-ms int?)
(s/def :ottla.consumer/listen-ms int?)
(s/def :ottla.consumer/deserialize-key serialize-spec)
(s/def :ottla.consumer/deserialize-value serialize-spec)
(s/def :ottla.consumer/xform fn?)
(s/def :ottla.consumer/exception-handler fn?)


(def ^:private consumer-opt-keys
  [:ottla.consumer/poll-ms
   :ottla.consumer/await-close-ms
   :ottla.consumer/listen-ms
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
