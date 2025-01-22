(ns ottla.postgresql
  (:require [pg.core :as pg]
            [pg.honey :as honey]
            [honey.sql]))

(defn connect-config
  [config]
  (assert (nil? (:conn config)) "config is already connected")
  (assoc config :conn (pg/connect (:conn-map config))))

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

(defn trigger-function-name
  [schema]
  (sql-entity (str schema ".notify_subs")))

(defn topic-table-name
  [topic]
  (str "log__" topic))

(def default-subscription-group "default")

(defn ensure-schema
  [{:keys [conn schema]}]
  (pg/on-connection
   [conn conn]
   (pg/with-tx [conn]
     (pg/execute conn (str "create schema if not exists \"" schema "\""))
     (pg/query conn (format trigger-function-template (trigger-function-name schema)))
     (honey/execute conn
                    {:create-table [(keyword schema "topics") :if-not-exists]
                     :with-columns [[:tid :int :primary-key :generated :always :as :identity]
                                    [:topic :text [:not nil] :unique]
                                    [:table_name :text [:not nil] :unique]]})
     (honey/execute conn
                    {:create-table [(keyword schema "subs") :if-not-exists]
                     :with-columns [[:sid :int :primary-key :generated :always :as :identity]
                                    [:topic :text [:not nil] [:references (keyword schema "topics") :topic]]
                                    [:group_id :text [:not nil] [:default [:inline default-subscription-group]]]
                                    [:cursor :bigint [:not nil] [:default [:inline 0]]]
                                    [[:unique] [:composite :topic :group_id]]]}))))

(defn delete-topic
  [{:keys [conn schema]} topic]
  (pg/on-connection [conn conn]
    (pg/with-tx [conn]
      (honey/execute conn {:delete-from (keyword schema "subs")
                           :where [:= :topic topic]})
      (honey/execute conn {:delete-from (keyword schema "topics")
                           :where [:= :topic topic]})
      (honey/execute conn {:drop-table [:if-exists (keyword schema (topic-table-name topic))]}))))

(defn create-topic
  [{:keys [conn schema]} topic]
  (let [table (keyword schema (topic-table-name topic))
        table-name (sql-entity table)
        trigger-fn-name (trigger-function-name schema)
        trigger-name (sql-entity (str (topic-table-name topic) "_trigger"))]
    (pg/on-connection [conn conn]
      (pg/with-tx [conn]
        (honey/execute conn {:insert-into (keyword schema "topics")
                             :columns [:topic :table_name]
                             :values [[topic (topic-table-name topic)]]})
        (honey/execute conn {:create-table (keyword schema (topic-table-name topic))
                             :with-columns [[:eid :bigint :primary-key :generated :always :as :identity]
                                            [:meta :jsonb]
                                            [:timestamp :timestamptz [:not nil] [:default [:now]]]
                                            [:key :bytea]
                                            [:value :bytea]]})
        (pg/query conn (format trigger-template trigger-name table-name trigger-fn-name topic))))))

(defn ->bytes
  [x]
  (if (string? x)
    (.getBytes x "UTF-8")
    x))

(def commit-modes #{:manual :auto :tx-wrap})

(def selection-defaults {:group "default"
                         :commit-mode :auto})

(defn normalize-selection
  [selection]
  (merge selection-defaults selection))

(defn insert-records
  [{:keys [conn schema]} topic records & {:keys [serialize-key serialize-value]
                                          :or {serialize-key identity
                                               serialize-value identity}
                                          :as opts}]
  (let [table (keyword schema (topic-table-name topic))
        conform (fn* [rec]
                     (-> rec
                         (assoc :key (some-> rec :key serialize-key ->bytes))
                         (assoc :value (some-> rec :value serialize-value ->bytes))))
        conformed (into [] (map conform) records)]
    (pg/on-connection
     [conn conn]
     (pg/execute conn
                 (str "insert into " (sql-entity table) "(meta, key, value) "
                      "select * from unnest($1::jsonb[], $2::bytea[], $3::bytea[])")
                 {:params [(mapv :meta conformed)
                           (mapv :key conformed)
                           (mapv :value conformed)]}))))

(defn ensure-subscription
  [{:keys [conn schema]} {:keys [topic group]}]
  (honey/execute conn {:insert-into [(keyword schema "subs")]
                       :values [{:topic topic :group_id group}]
                       :on-conflict [:topic :group_id]
                       :do-nothing true}))

(defn- fetch-records
  [{:keys [conn schema]} {:keys [topic min max limit xf]}]
  (let [xf (or xf identity)
        table (keyword schema (topic-table-name topic))]
    (honey/execute conn (cond-> {:select [:* [topic :topic]]
                                 :from [[table :t]]
                                 :where (cond-> [:and [:> :eid min]]
                                          max (conj [:<= :eid max]))
                                 :order-by [[:eid :asc]]}
                          limit (assoc :limit limit))
                   {:into [xf []]})))

(defn commit-cursor!
  [{:keys [conn schema]} {:keys [topic group]} cursor]
  (honey/execute conn {:update [(keyword schema "subs")]
                       :set {:cursor cursor}
                       :where [:and [:= :topic topic]
                               [:= :group_id group]
                               [:< :cursor cursor]]}))

(defn fetch-records!
  [{:keys [conn schema] :as config} {:keys [topic group commit-mode] :as selection}]
  (pg/on-connection
   [conn conn]
   (pg/with-tx [conn]
     (ensure-subscription config selection)
     (let [{:keys [cursor]} (first
                             (honey/execute conn {:select [:*]
                                                  :from [(keyword schema "subs")]
                                                  :where [:and [:= :topic topic]
                                                          [:= :group_id group]]
                                                  :limit 1
                                                  :for :update}))
           records (fetch-records config (assoc selection :min cursor))
           final (peek records)]
       (when (and final (contains? #{:auto :tx-wrap} commit-mode))
         (commit-cursor! config selection (:eid final)))
       records))))
