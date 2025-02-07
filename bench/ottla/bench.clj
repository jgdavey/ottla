(ns ottla.bench
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [criterium.core :as crit]
            [pg.core :as pg]
            [pg.honey :as honey]
            [ottla.postgresql :as postgres]))

(def cfg
  (aero/read-config
   (io/resource "test-config.edn")))

(def conn-params (:pg cfg))

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
    (edn/read rdr)))

(defn reset-schema!
  [{:keys [conn conn-map schema] :as config}]
  (pg/with-connection [conn (or conn conn-map)]
    (pg/execute conn (str "drop schema if exists \"" schema "\" cascade"))
    (postgres/ensure-schema config)))

(defn make-records
  [n]
  (into []
        (map (fn [i]
               {:key (* 2 i)
                :value (- 1000000 i)
                :meta {:i i}}))
        (range 1 (inc n))))

(defn bench-insert-records [num]
  (let [records (make-records num)
        topic "benchy"
        schema "bench"
        config {:schema schema :conn-map conn-params}]
    (pg/with-connection [conn (assoc conn-params :binary-encode? true :binary-decode? true)]
      (let [config (assoc config :conn conn)
            table (keyword schema (postgres/topic-table-name topic))
            conform (fn* [rec]
                         (-> rec
                             (assoc :key (some-> rec :key serialize-edn postgres/->bytes))
                             (assoc :value (some-> rec :value serialize-edn postgres/->bytes))))
            conformed-maps (into [] (map conform) records)]
        (reset-schema! config)
        (postgres/create-topic config topic)
        #_(postgres/insert-records config topic records {:serialize-key serialize-edn
                                                         :serialize-value serialize-edn})
        (crit/benchmark-round-robin
         [(pg/execute conn
                         (str
                          (first
                           (honey/format
                            {:insert-into [table]
                             :columns [:meta :key :value]}))
                          " select * from unnest($1::jsonb[], $2::bytea[], $3::bytea[])")
                         {:params [(mapv :meta conformed-maps)
                                   (mapv :key conformed-maps)
                                   (mapv :value conformed-maps)]})
          ]
         crit/*default-quick-bench-opts*)))))

(defn bench-fetch-records [num]
  (let [records (make-records num)
        topic "benchy"
        schema "bench"
        config {:schema schema :conn-map conn-params}]
    (pg/with-connection [conn (assoc conn-params :binary-encode? true :binary-decode? true)]
      (let [config (assoc config :conn conn)
            table (keyword schema (postgres/topic-table-name topic))
            xf (map (fn [rec] (-> rec
                                  (update :key deserialize-edn)
                                  (update :value deserialize-edn))))]
        (reset-schema! config)
        (postgres/create-topic config topic)
        (postgres/insert-records config topic records {:serialize-key serialize-edn
                                                       :serialize-value serialize-edn})
        (crit/benchmark-round-robin
         [(pg/execute conn (str "select *, '" topic "' as topic from " (postgres/sql-entity table))
                      {:into [xf []]})
          (honey/execute conn {:select [:* [topic :topic]]
                               :from [[table :t]]}
                         {:into [xf []]})]
         crit/*default-quick-bench-opts*)))))


(comment

  (def results-1
    (bench-insert-records 1))

  (def results-10
    (bench-insert-records 10))

  (def results-100
    (bench-insert-records 100))

  (def results-1000
    (bench-insert-records 1000))

  (defn report [[unnest-honey unnest-raw]]
    (println "================")
    (println "\n *** unnest honey ***")
    (crit/report-result unnest-honey)

    (println "\n *** unnest raw ***")
    (crit/report-result unnest-raw))

  (report results-1)
  (report results-10)
  (report results-100)
  (report results-1000)

  (def results-1
    (bench-fetch-records 1))

  (def results-10
    (bench-fetch-records 10))

  (def results-100
    (bench-fetch-records 100))

  (def results-1000
    (bench-fetch-records 1000))

  (defn report [[raw honey]]
    (println "================")
    (println "\n *** raw ***")
    (crit/report-result raw)
    (println "\n ***  honey ***")
    (crit/report-result honey))

  (do
    (report results-1)
    (report results-10)
    (report results-100)
    (report results-1000))

  :ok)
