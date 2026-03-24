(ns ottla.postgresql
  (:require [clojure.string :as str]
            [pg.core :as pg]
            [pg.honey :as honey]
            [ottla.serde.registry :refer [get-serializer! get-deserializer!]]
            [honey.sql])
  (:import [java.time Duration]))

(def ^:private negative-infinity [:raw "'-Infinity'::timestamptz"])

(defn legal-identifier?
  [^String s]
  (boolean
   (and s
        (< (count s) 64)
        (re-find #"^[a-zA-Z_][a-zA-Z0-9_]*$" s))))

(defn connect-config
  [config]
  (assert (nil? (:conn config)) "config is already connected")
  (assert (legal-identifier? (:schema config)) "schema must be a legal postgres identifier")
  (let [pool? (or (get-in config [:conn-map :pool-max-size])
                  (get-in config [:conn-map :pool-min-size]))]
    (assoc config :conn (if pool?
                          (pg/pool (:conn-map config))
                          (pg/connect (:conn-map config))))))

(defn sql-entity
  [x]
  (first (honey.sql/format (keyword x))))

(def trigger-function-template
  ;; currval() is session-local and returns the last value obtained from nextval()
  ;; in the current session. For a statement-level AFTER INSERT trigger this is the
  ;; max eid of the just-inserted batch, which is correct. It would be stale only if
  ;; the trigger fired with zero rows inserted, which cannot happen with a plain INSERT.
  "CREATE OR REPLACE FUNCTION %s() RETURNS TRIGGER AS $$
DECLARE
    newoffset bigint;
BEGIN
    SELECT currval(pg_get_serial_sequence(TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, 'eid'))
      into newoffset;

    PERFORM pg_notify(TG_ARGV[0], newoffset::text);
    IF TG_LEVEL = 'ROW' THEN
        return NEW;
    ELSE
        return NULL;
    END IF;
END;
$$ LANGUAGE 'plpgsql';
")

(def trigger-template "CREATE TRIGGER %s
AFTER INSERT ON %s
FOR EACH STATEMENT EXECUTE FUNCTION %s('%s')")

(def brin-index-template "CREATE INDEX ON %s USING BRIN (timestamp)")

(def btree-key-index-template "CREATE INDEX ON %s USING BTREE (key)")

(defn trigger-function-name
  [schema]
  (sql-entity (str schema ".notify_subs")))

(defn normalize-topic-name
  [topic]
  (-> topic
      (str/replace \- \_)
      (str/replace \. \_)
      (str/replace #"[^\w\.]" "")))

(defn topic-table-name
  [topic]
  (str "log__" topic))

(declare fetch-topic)

(defn- resolve-cursor
  "Resolve a :from option to a cursor value. Must be called with an active conn in config."
  [{:keys [conn schema] :as config} topic-name from]
  (cond
    (or (nil? from) (= :earliest from)) 0
    (= :latest from) (let [{:keys [table-name]} (fetch-topic config topic-name)
                           qtable (keyword schema table-name)]
                       (or (:maxeid (first (honey/execute conn
                                                         {:select [[[:coalesce [[:max :eid]] [:inline 0]] :maxeid]]
                                                          :from qtable})))
                           0))
    (nat-int? from) from
    :else (throw (IllegalArgumentException.
                  (str "Invalid :from value: " (pr-str from)
                       "; expected :earliest, :latest, or a non-negative integer")))))

(def default-subscription-group "default")

(defn- ->topic-map
  [{:keys [topic table_name key_type value_type]}]
  {:topic topic
   :table-name table_name
   :key-type (keyword key_type)
   :value-type (keyword value_type)})

(defn ensure-schema
  [{:keys [conn schema]}]
  (pg/with-connection
   [conn conn]
   (pg/with-transaction [conn conn]
     (pg/execute conn (str "create schema if not exists \"" schema "\""))
     (pg/query conn (format trigger-function-template (trigger-function-name schema)))
     (honey/execute conn
                    {:create-table [(keyword schema "topics") :if-not-exists]
                     :with-columns [[:tid :int :primary-key :generated :always :as :identity]
                                    [:topic :text [:not nil] :unique]
                                    [:table_name :text [:not nil] :unique]
                                    [:key_type :text [:not nil]]
                                    [:value_type :text [:not nil]]]})
     (honey/execute conn
                    {:create-table [(keyword schema "subs") :if-not-exists]
                     :with-columns [[:sid :int :primary-key :generated :always :as :identity]
                                    [:topic :text [:not nil] [:references (keyword schema "topics") :topic]]
                                    [:group_id :text [:not nil] [:default [:inline default-subscription-group]]]
                                    [:cursor :bigint [:not nil] [:default [:inline 0]]]
                                    [:updated_at :timestamptz [:not nil] [:default negative-infinity]]
                                    [[:unique] [:composite :topic :group_id]]]}))))

(defn delete-topic
  [{:keys [conn schema]} topic]
  (pg/with-connection [conn conn]
    (pg/with-transaction [conn conn]
      (honey/execute conn {:delete-from (keyword schema "subs")
                           :where [:= :topic topic]})
      (when-let [{:keys [table_name]} (first (honey/execute conn {:delete-from (keyword schema "topics")
                                                                  :where [:= :topic topic]
                                                                  :returning :*}))]
        (honey/execute conn {:drop-table [:if-exists (keyword schema table_name)]})))))


(def column-types #{:bytea :text :jsonb})

(defn create-topic
  [{:keys [conn schema]} topic & {:keys [key-type value-type index-key?] :as opts
                                  :or {key-type :bytea
                                       value-type :bytea
                                       index-key? false}}]
  (when-not (contains? column-types key-type)
    (throw (IllegalArgumentException. (str "Invalid key-type " key-type "; must be one of " column-types))))
  (when-not (contains? column-types value-type)
    (throw (IllegalArgumentException. (str "Invalid value-type " value-type "; must be one of " column-types))))
  (let [normalized-topic (normalize-topic-name topic)
        table (topic-table-name normalized-topic)
        qtable (keyword schema table)
        qtable-name (sql-entity qtable)
        trigger-fn-name (trigger-function-name schema)
        trigger-name (sql-entity (str (topic-table-name normalized-topic) "_trigger"))]
    (pg/with-connection [conn conn]
      (pg/with-transaction [conn conn]
        (when-let [{existing-topic :topic} (first (honey/execute conn {:select [:topic]
                                                                       :from [(keyword schema "topics")]
                                                                       :where [:= :table_name table]}))]
          (throw (ex-info "Topic name collision: normalized table name already in use"
                          {:topic topic :collides-with existing-topic :table-name table})))
        (let [[row] (honey/execute conn {:insert-into (keyword schema "topics")
                                         :columns [:topic :table_name :key_type :value_type]
                                         :values [[topic table (name key-type) (name value-type)]]
                                         :returning :*})]

          (honey/execute conn {:create-table qtable
                               :with-columns [[:eid :bigint :primary-key :generated :always :as :identity]
                                              [:timestamp :timestamptz [:not nil] [:default [:now]]]
                                              [:meta :jsonb]
                                              [:key key-type]
                                              [:value value-type]]})
          (pg/query conn (format brin-index-template qtable-name))
          (when index-key?
            (pg/query conn (format btree-key-index-template qtable-name)))
          (pg/query conn (format trigger-template trigger-name qtable-name trigger-fn-name topic))
          (->topic-map row))))))

(defn list-topics
  [{:keys [conn schema]}]
  (mapv (comp #(dissoc % :table-name) ->topic-map)
        (honey/execute conn
                       {:select [:topic :table_name :key_type :value_type]
                        :from (keyword schema "topics")
                        :order-by [[:topic :asc]]})))

(defn maybe-fetch-topic
  [{:keys [conn schema]} topic-name]
  (some->
   (honey/execute conn
                  {:select [:topic :table_name :key_type :value_type]
                   :from (keyword schema "topics")
                   :where [:= :topic topic-name]})
   first
   ->topic-map))

(defn fetch-topic
  [config topic-name]
  (if-let [found (maybe-fetch-topic config topic-name)]
    found
    (throw (IllegalArgumentException. (str "No such topic: " topic-name)))))

(defn trim-topic
  [{:keys [conn schema] :as config} topic & {:keys [before-eid before-timestamp all? ignore-subscriptions?]
                                             :or {ignore-subscriptions? false}}]
  (let [mode-count (+ (if (some? before-eid) 1 0)
                      (if (some? before-timestamp) 1 0)
                      (if all? 1 0))]
    (when (not= 1 mode-count)
      (throw (IllegalArgumentException. "exactly one of :before-eid, :before-timestamp, or :all? must be provided"))))
  (pg/with-connection [conn conn]
    (pg/with-transaction [conn conn]
      (let [config (assoc config :conn conn)
            {:keys [table-name]} (fetch-topic config topic)
            qtable (keyword schema table-name)
            primary-condition (cond
                                before-eid [:< :eid before-eid]
                                before-timestamp [:< :timestamp before-timestamp]
                                all? [:< :eid {:select [[[:max :eid]]] :from qtable}])
            ;; Unless we're ignoring subscriptions, AND with eid < min-cursor to protect unconsumed records.
            ;; COALESCE to Long/MAX_VALUE so that no subscriptions = no restriction.
            where-clause (if ignore-subscriptions?
                           primary-condition
                           [:and primary-condition
                            [:< :eid
                             {:select [[[:coalesce [[:min :cursor]] [:inline Long/MAX_VALUE]]]]
                              :from [(keyword schema "subs")]
                              :where [:= :topic topic]}]])]
        (:deleted (honey/execute conn {:delete-from qtable
                                       :where where-clause}))))))

(defn ensure-topic
  "Fetch or create a topic. Returns the existing topic if it already exists
  with the same key-type and value-type. Throws if the topic exists but was
  created with different column types. Note: opts apply only at creation time
  and cannot be used to change an existing topic."
  [{:keys [conn] :as config} topic-name & {:keys [key-type value-type] :as opts
                                           :or {key-type :bytea
                                                value-type :bytea}}]
  (pg/with-connection [conn conn]
    (pg/with-transaction [conn conn]
      (let [config (assoc config :conn conn)]
        (if-let [found (maybe-fetch-topic config topic-name)]
          (let [passed {:topic topic-name
                        :key-type key-type
                        :value-type value-type}
                stored (select-keys found [:topic :key-type :value-type])]
            (if (= stored passed)
              found
              (throw (ex-info "Topic definition differs from stored" {:stored stored, :passed passed}))))
          (create-topic config topic-name opts))))))

(defn- parse-selections
  "Parse a collection of selections (strings or {:topic :group?} maps) into
  a map of topic-name -> set-of-groups-or-nil (nil means all groups for that topic)."
  [selections]
  (when (seq selections)
    (reduce (fn [acc sel]
              (let [topic (if (string? sel) sel (:topic sel))
                    group (when (map? sel) (:group sel))]
                (cond
                  (and (contains? acc topic) (nil? (get acc topic))) acc
                  (nil? group) (assoc acc topic nil)
                  :else (update acc topic (fnil conj #{}) group))))
            {}
            selections)))

(defn list-subscriptions
  [{:keys [conn schema]}
   & [{:keys [selections]}]]
  (pg/with-connection [conn conn]
    (let [subs-table (keyword schema "subs")
          topics-table (keyword schema "topics")
          sel-map (parse-selections selections)
          topic-names (some-> sel-map keys vec)
          tt (honey/execute conn
                            (cond->
                             {:select [:t.topic :t.table_name]
                              :from [[topics-table :t]]}
                              topic-names (assoc :where [:= :t.topic [:any [:lift topic-names]]])))
          queries (mapv (fn [{:keys [topic table_name]}]
                          (let [qtable (keyword schema table_name)
                                groups (when sel-map (get sel-map topic))]
                            {:with [[:m
                                     {:select [[:eid :maxeid]
                                               [:timestamp :maxts]]
                                      :from [qtable]
                                      :order-by [[:eid :desc]]
                                      :limit [:inline 1]}]]
                             :select [:s.topic
                                      [:s.group_id :group]
                                      [:s.cursor :offset]
                                      [[:nullif :s.updated_at negative-infinity] :updated_at]
                                      [:t.timestamp :timestamp]
                                      [:maxts :topic_timestamp]
                                      [[:coalesce :maxeid [:inline 0]] :topic_eid]
                                      [[:- [:coalesce :maxeid [:inline 0]] :s.cursor] :lag]]
                             :from [[subs-table :s]]
                             :left-join [:m true
                                         [[:lateral
                                           {:select [:eid :timestamp]
                                            :from [[qtable :l]]
                                            :where [:<= :l.eid :s.cursor]
                                            :order-by [[:eid :desc]]
                                            :limit [:inline 1]}]
                                          :t]
                                         true]
                             :where (if (seq groups)
                                      [:and [:= :s.topic topic]
                                            [:= :s.group_id [:any [:lift (vec groups)]]]]
                                      [:= :s.topic topic])}))
                        tt)]
      (into []
            (comp
             (map #(honey/execute conn %))
             cat
             (map (fn [{:keys [topic group offset updated_at timestamp topic_timestamp topic_eid lag]}]
                    {:topic topic
                     :group group
                     :offset offset
                     :updated-at (some-> updated_at .toInstant)
                     :timestamp (some-> timestamp .toInstant)
                     :topic-timestamp (some-> topic_timestamp .toInstant)
                     :topic-eid topic_eid
                     :lag lag
                     :timestamp-lag (when (and timestamp topic_timestamp)
                                      (Duration/between timestamp topic_timestamp))
                     :processing-delay (when (and updated_at timestamp)
                                         (Duration/between timestamp updated_at))})))
            queries))))

(defn topic-subscriptions
  "Returns all topics with their consumer group subscriptions nested under each
  topic. Unlike `list-subscriptions`, every topic appears in the result even if
  it has no subscribers.

  Each entry is a map with:
    - :topic          the topic name
    - :subscriptions  a vector of subscription maps (empty when no subscribers)

  Each subscription map contains the same keys as `list-subscriptions` (except
  :topic, which is on the outer map):
    - :group             consumer group id
    - :offset            last committed eid for this group
    - :topic-eid         highest eid available in the topic
    - :lag               number of unread records (topic-eid - offset)
    - :updated-at        java.time.Instant of the last offset commit (nil if never committed)
    - :timestamp         java.time.Instant of the last consumed record (nil if offset is 0)
    - :topic-timestamp   java.time.Instant of the latest record in the topic (nil if empty)
    - :timestamp-lag     java.time.Duration between subscription and topic timestamps (nil if either is nil)
    - :processing-delay  java.time.Duration from publish time to consumer commit for the most
                         recently consumed record (nil if either :updated-at or :timestamp is nil)"
  [{:keys [conn] :as config} & [{:keys [selections]}]]
  (pg/with-connection [conn conn]
    (let [config (assoc config :conn conn)
          sel-map (parse-selections selections)
          topic-names (some-> sel-map keys set)
          all-topics (list-topics config)
          topics (if topic-names
                   (filterv #(contains? topic-names (:topic %)) all-topics)
                   all-topics)
          subs (list-subscriptions config (when selections {:selections selections}))
          subs-by-topic (group-by :topic subs)]
      (mapv (fn [{:keys [topic]}]
              {:topic topic
               :subscriptions (mapv #(dissoc % :topic) (get subs-by-topic topic []))})
            topics))))

(def commit-modes #{:manual :auto :tx-wrap})

(def selection-defaults {:group "default"
                         :commit-mode :auto})

(defn normalize-selection
  [selection]
  (merge selection-defaults (if (string? selection)
                              {:topic selection}
                              selection)))

(defn insert-records
  [{:keys [conn conn-map schema] :as cfg}
   topic
   records
   & {:keys [serialize-key serialize-value]
      :as opts}]
  (let [{:keys [table-name key-type value-type]} (if (map? topic)
                                                   topic
                                                   (fetch-topic cfg topic))
        table-name (keyword schema table-name)
        serialize-key (get-serializer! serialize-key key-type)
        serialize-value (get-serializer! serialize-value value-type)
        conform (fn* [rec]
                     (-> rec
                         (assoc :key (some-> rec :key serialize-key))
                         (assoc :value (some-> rec :value serialize-value))))
        conformed (into [] (map conform) records)]
    (pg/with-connection
      [conn (or conn conn-map)]
      (->
       (pg/execute conn
                   (str "with inserted as ("
                        "insert into " (sql-entity table-name) "(meta, key, value) "
                        "select * from unnest($1::jsonb[], $2::" (name key-type) "[],"
                        " $3::" (name value-type) "[]) returning eid"
                        ") select max(eid) eid from inserted")
                   {:params [(mapv :meta conformed)
                             (mapv :key conformed)
                             (mapv :value conformed)]})
       first
       :eid))))

(defn ensure-subscription
  "Create a subscription for topic/group if one does not already exist.
  Returns true if created, false if it already existed. The :from option
  only applies when creating; an existing subscription is not modified."
  [{:keys [conn schema] :as config} {:keys [topic group]} & {:keys [from]}]
  (pg/with-connection [conn conn]
    (pg/with-transaction [conn conn]
      (let [config (assoc config :conn conn)
            cursor (resolve-cursor config topic from)
            subs-table (keyword schema "subs")
            sql {:insert-into [[subs-table [:topic :group_id :cursor]]
                               {:select [topic group cursor]
                                :where [:not [:exists {:select [[[:inline 1]]]
                                                       :from [subs-table]
                                                       :where [:and [:= :topic topic]
                                                               [:= :group_id group]]}]]}]
                 :on-conflict [:topic :group_id]
                 :do-nothing true}
            result (honey/execute conn sql)]
        (= 1 (-> result :inserted))))))

(defn create-subscription
  "Create a subscription for topic/group. Throws ex-info with
  {:topic ... :group ...} if a subscription already exists."
  [{:keys [conn schema] :as config} {:keys [topic group]} & {:keys [from]}]
  (pg/with-connection [conn conn]
    (pg/with-transaction [conn conn]
      (let [config (assoc config :conn conn)
            subs-table (keyword schema "subs")]
        (when (seq (honey/execute conn {:select [[[:inline 1]]]
                                        :from [subs-table]
                                        :where [:and [:= :topic topic]
                                                [:= :group_id group]]
                                        :for :update}))
          (throw (ex-info "Subscription already exists" {:topic topic :group group})))
        (let [cursor (resolve-cursor config topic from)]
          (honey/execute conn {:insert-into subs-table
                               :columns [:topic :group_id :cursor]
                               :values [[topic group cursor]]})
          true)))))

(defn delete-subscription
  "Delete a subscription for topic/group. Returns true if a subscription was
  deleted, false if none existed."
  [{:keys [conn schema]} {:keys [topic group]}]
  (pg/with-connection [conn conn]
    (let [result (honey/execute conn {:delete-from (keyword schema "subs")
                                      :where [:and [:= :topic topic]
                                              [:= :group_id group]]})]
      (= 1 (:deleted result)))))

(defn- fetch-records
  [{:keys [conn schema]} {:keys [topic min max limit xf]}]
  (let [xf (or xf identity)
        table (keyword schema (topic-table-name (normalize-topic-name topic)))]
    (honey/execute conn (cond-> {:select [:* [topic :topic]]
                                 :from [[table :t]]
                                 :where (cond-> [:and [:> :eid min]]
                                          max (conj [:<= :eid max]))
                                 :order-by [[:eid :asc]]}
                          limit (assoc :limit limit))
                   {:into [xf []]})))

(defn commit-offset!
  [{:keys [conn schema]} {:keys [topic group]} cursor]
  (= 1 (:updated
         (honey/execute conn {:update [(keyword schema "subs")]
                              :set {:cursor cursor :updated_at [:now]}
                              :where [:and [:= :topic topic]
                                      [:= :group_id group]
                                      [:< :cursor cursor]]}))))

(defn reset-offset!
  [{:keys [conn schema]} {:keys [topic group]} cursor]
  (= 1 (:updated
         (honey/execute conn {:update [(keyword schema "subs")]
                              :set {:cursor cursor :updated_at negative-infinity}
                              :where [:and [:= :topic topic]
                                      [:= :group_id group]]}))))

(defn deserializer-xf
  [config topic {:keys [deserialize-key deserialize-value]}]
  (let [{:keys [key-type value-type]} (fetch-topic config topic)
        deserialize-key (get-deserializer! (or deserialize-key identity) key-type)
        deserialize-value (get-deserializer! (or deserialize-value identity) value-type)]
    (map (fn [rec] (-> rec
                       (update :key deserialize-key)
                       (update :value deserialize-value))))))

(defn fetch-records*
  [{:keys [conn schema] :as config} {:keys [topic group commit-mode] :as selection}]
  (pg/with-connection [conn conn]
    (pg/with-transaction [conn conn]
      (let [config (assoc config :conn conn)
            {:keys [cursor]} (first
                              (honey/execute conn {:select [:*]
                                                   :from [(keyword schema "subs")]
                                                   :where [:and [:= :topic topic]
                                                           [:= :group_id group]]
                                                   :limit 1
                                                   :for :update}))
            records (fetch-records config (assoc selection :min cursor))
            final (peek records)]
        (when (and final (contains? #{:auto :tx-wrap} commit-mode))
          (commit-offset! config selection (:eid final)))
        records))))

(defn fetch-records!
  [config selection]
  (let [selection (normalize-selection selection)]
    (ensure-subscription config selection)
    (fetch-records* config selection)))
