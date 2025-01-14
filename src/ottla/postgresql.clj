(ns ottla.postgresql
  (:require [pg.core :as pg]
            [pg.honey :as honey]
            [honey.sql]))

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

(defn ensure-schema
  [{:keys [conn schema]}]
  (pg/on-connection [conn conn]
   (pg/with-tx [conn]
     (pg/execute conn (str "create schema if not exists \"" schema "\""))
     (pg/query conn (format trigger-function-template (trigger-function-name schema)))
     (honey/execute conn
                    {:create-table [(keyword schema "topics") :if-not-exists]
                     :with-columns [[:tid :int :primary-key :generated :always :as :identity]
                                    [:topic :text [:not nil] :unique]
                                    [:table_name :text [:not nil] :unique]]}))))

(defn delete-topic
  [{:keys [conn schema]} topic]
  (pg/on-connection [conn conn]
    (pg/with-tx [conn]
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
                                            [:meta :jsonb [:not nil] [:default [:inline "{}"]]]
                                            [:timestamp :timestamptz [:not nil] [:default [:now]]]
                                            [:key :bytea]
                                            [:value :bytea]]})
        (pg/query conn (format trigger-template trigger-name table-name trigger-fn-name topic))))))

(defn insert-records
  [{:keys [conn schema]} topic records]
  (let [table (keyword schema (topic-table-name topic))]
    (pg/on-connection [conn conn]
      (honey/execute conn {:insert-into table :values records}))))

(comment

  (pg/with-connection [conn {:user "jgdavey" :database "test"}]
    (pg/execute conn "drop schema ottla cascade"))

  (let [config {:schema "ottla"}]
    (pg/with-connection [conn {:user "jgdavey"
                               :database "test"}]
      (let [config (assoc config :conn conn)]
        (ensure-schema config)
        (delete-topic config "foo")
        (create-topic config "foo"))))

  (let [config {:schema "ottla"}]
    (pg/with-connection [conn {:user "jgdavey"
                               :database "test"
                               :binary-encode? true}]
      (let [config (assoc config :conn conn)]
        (insert-records config "foo" [{:key (.getBytes "the-key" "UTF-8")
                                       :value (.getBytes "" "UTF-8")}]))))

  
  (pg/with-connection [conn  {:user "jgdavey" :database "test"}]
    (pg/query conn "select * from ottla.log__foo_eid_seq"))

  :done)
