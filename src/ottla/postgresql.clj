(ns ottla.postgresql
  (:require [clojure.string :as str]
            [pg.core :as pg]
            [pg.honey :as honey]
            [ottla.serde.registry :refer [get-serializer! get-deserializer!]]
            [honey.sql]))

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

(def ^:private ensure-subs-sql
  "INSERT INTO %s(topic, group_id)
SELECT $1, $2
WHERE NOT EXISTS (
  select 1 from %s where topic=$1 AND group_id=$2
)
ON CONFLICT (topic, group_id) DO NOTHING")

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
  [{:keys [conn schema]} topic & {:keys [key-type value-type] :as opts
                                  :or {key-type :bytea
                                       value-type :bytea}}]
  (when-not (contains? column-types key-type)
    (throw (IllegalArgumentException. "Invalid key-type")))
  (when-not (contains? column-types value-type)
    (throw (IllegalArgumentException. "Invalid value-type")))
  (let [normalized-topic (normalize-topic-name topic)
        table (topic-table-name normalized-topic)
        qtable (keyword schema table)
        qtable-name (sql-entity qtable)
        trigger-fn-name (trigger-function-name schema)
        trigger-name (sql-entity (str (topic-table-name normalized-topic) "_trigger"))]
    (pg/with-connection [conn conn]
      (pg/with-transaction [conn conn]
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
          (pg/query conn (format trigger-template trigger-name qtable-name trigger-fn-name topic))
          (->topic-map row))))))

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

(defn ensure-topic
  "Idempotent version of fetch or create"
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

(defn topic-subscriptions
  [{:keys [conn schema]}]
  (honey/execute conn {:select [:topic
                                [[:coalesce {:select [[[:jsonb_agg
                                                        [:jsonb_build_object
                                                         [:inline "offset"] :cursor
                                                         [:inline "group"] :group_id]] :sub]]
                                             :from [[(keyword schema "subs") :s]]
                                             :where [:= :s.topic :t.topic]}
                                  [:raw "'[]'::jsonb"]] :subscriptions]]
                       :from [[(keyword schema "topics") :t]]}))

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
      (pg/execute conn
                  (str "insert into " (sql-entity table-name) "(meta, key, value) "
                       "select * from unnest($1::jsonb[], $2::" (name key-type) "[],"
                       " $3::" (name value-type) "[])")
                  {:params [(mapv :meta conformed)
                            (mapv :key conformed)
                            (mapv :value conformed)]}))))

(defn ensure-subscription
  [{:keys [conn schema]} {:keys [topic group]}]
  (let [subs-table (sql-entity (keyword schema "subs"))
        sql (format ensure-subs-sql subs-table subs-table)
        result (pg/execute conn sql {:params [topic group]})]
    (= 1 (-> result :inserted))))

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
  (honey/execute conn {:update [(keyword schema "subs")]
                       :set {:cursor cursor}
                       :where [:and [:= :topic topic]
                               [:= :group_id group]
                               [:< :cursor cursor]]}))

(defn reset-offset!
  [{:keys [conn schema]} {:keys [topic group]} cursor]
  (honey/execute conn {:update [(keyword schema "subs")]
                       :set {:cursor cursor}
                       :where [:and [:= :topic topic]
                               [:= :group_id group]]}))

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
