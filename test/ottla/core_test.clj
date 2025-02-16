(ns ottla.core-test
  (:require [clojure.test :as test :refer [deftest is testing]]
            [ottla.test-helpers :as th :refer [*config*]]
            [ottla.serde.edn :refer [serialize-edn-bytea serialize-edn-text
                                     deserialize-bytea-edn deserialize-text-edn]]
            [ottla.serde.json :refer [serialize-object-bytea serialize-object-jsonb serialize-object-text
                                      deserialize-bytea-object deserialize-jsonb-object]]
            [ottla.serde.string :refer [serialize-string-text deserialize-text-string]]
            [ottla.core :as ottla]
            [ottla.consumer :as consumer]))

(test/use-fixtures :each
  th/config-fixture
  th/connection-fixture)

(deftest test-consumer
  (let [topic "so_good"
        _ (ottla/add-topic! *config* topic)
        p (promise)
        records (atom [])
        ex (atom [])
        handler (fn [_ recs]
                  (swap! records into recs)
                  (deliver p :received))
        ex-handler #(swap! ex conj %)]
    (with-open [_consumer (ottla/start-consumer (dissoc *config* :conn)
                                                {:topic topic}
                                                handler
                                                {:deserialize-key deserialize-bytea-edn
                                                 :deserialize-value deserialize-bytea-edn
                                                 :exception-handler ex-handler})]
      (ottla/append *config* topic [{:key 1 :value 42}] {:serialize-key serialize-edn-bytea
                                                         :serialize-value serialize-edn-bytea})
      (is (= :received (deref p 100 :timed-out))))
    (is (= [] (mapv Throwable->map @ex)))
    (is (= [{:meta nil :key 1 :value 42 :topic topic}]
           (mapv #(dissoc % :eid :timestamp) @records)))))

(deftest test-json-serde
  (let [topic "so_json"
        _ (ottla/add-topic! *config* topic)
        p (promise)
        records (atom [])
        ex (atom [])
        handler (fn [_ recs]
                  (swap! records into recs)
                  (deliver p :received))
        ex-handler #(swap! ex conj %)]
    (with-open [_consumer (ottla/start-consumer (dissoc *config* :conn)
                                                {:topic topic}
                                                handler
                                                {:deserialize-key deserialize-bytea-object
                                                 :deserialize-value deserialize-bytea-object
                                                 :exception-handler ex-handler})]
      (ottla/append *config* topic [{:key 1 :value "42...."}] {:serialize-key serialize-json
                                                               :serialize-value serialize-json})
      (is (= :received (deref p 100 :timed-out))))
    (is (= [] (mapv Throwable->map @ex)))
    (is (= [{:meta nil :key 1 :value "42...." :topic topic}]
           (mapv #(dissoc % :eid :timestamp) @records)))))

(deftest test-json-column-type
  (let [topic "so_json"
        _ (ottla/add-topic! *config* topic :key-type :text :val-type :jsonb)
        p (promise)
        records (atom [])
        ex (atom [])
        handler (fn [_ recs]
                  (swap! records into recs)
                  (deliver p :received))
        ex-handler #(swap! ex conj %)
        msg {:meta {:foo 0} :key "1" :value {:a 1}}]
    (with-open [_consumer (ottla/start-consumer (dissoc *config* :conn)
                                                {:topic topic}
                                                handler
                                                {:deserialize-key deserialize-text-string
                                                 :deserialize-value deserialize-jsonb-object
                                                 :exception-handler ex-handler})]
      (ottla/append *config* topic [msg] {:serialize-key serialize-string-text
                                          :serialize-value serialize-object-jsonb})
      (is (= :received (deref p 100 :timed-out))))
    (is (= [] (mapv Throwable->map @ex)))
    (is (= [(assoc msg :topic topic)]
           (mapv #(dissoc % :eid :timestamp) @records)))))

(deftest test-consumer-ex-continue
  (let [topic "theproblem"
        _ (ottla/add-topic! *config* topic)
        r1 (promise)
        ex (promise)
        r3 (promise)
        handler (fn [_ [{:keys [key]}]]
                  (case key
                    1 (deliver r1 :yes)
                    2 (throw (ex-info "Yikes" {}))
                    3 (deliver r3 :yes)))
        ex-handler (fn [e] (deliver ex e))]
    (with-open [consumer (ottla/start-consumer (dissoc *config* :conn)
                                               {:topic topic}
                                               handler
                                               {:deserialize-key deserialize-bytea-edn
                                                :exception-handler ex-handler})]
      (ottla/append *config* topic [{:key 1 :value 42}] {:serialize-key serialize-edn-bytea
                                                         :serialize-value serialize-edn-bytea})
      (is (not= :timed-out (deref r1 100 :timed-out)))
      (is (= :running (consumer/status consumer)))
      (ottla/append *config* topic [{:key 2 :value 42}] {:serialize-key serialize-edn-bytea
                                                         :serialize-value serialize-edn-bytea})
      (is (not= :timed-out (deref ex 100 :timed-out)))
      (is (= :running (consumer/status consumer)))
      (ottla/append *config* topic [{:key 3 :value 42}] {:serialize-key serialize-edn-bytea
                                                         :serialize-value serialize-edn-bytea})
      (is (not= :timed-out (deref r3 100 :timed-out)))
      (is (= :running (consumer/status consumer))))))

(deftest test-consumer-ex-shutdown
  (let [topic "theproblem"
        _ (ottla/add-topic! *config* topic)
        ex (promise)
        handler (fn [_ _]
                  (throw (ex-info "Yikes" {})))
        ex-handler (fn [e]
                     (deliver ex e)
                     ottla/shutdown)]
    (with-open [consumer (ottla/start-consumer (dissoc *config* :conn)
                                               {:topic topic}
                                               handler
                                               {:exception-handler ex-handler})]
      (ottla/append *config* topic [{:key 1 :value 42}] {:serialize-key serialize-edn-bytea
                                                         :serialize-value serialize-edn-bytea})
      (is (= :running (consumer/status consumer)))
      (is (not= :timed-out (deref ex 100 :timed-out)))
      (Thread/sleep 10)
      (is (not= :running (consumer/status consumer))))))
