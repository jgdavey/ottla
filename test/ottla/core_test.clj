(ns ottla.core-test
  (:require [clojure.test :as test :refer [deftest is testing]]
            [ottla.test-helpers :as th :refer [*config*]]
            [ottla.serde.edn :refer [serialize-edn deserialize-edn]]
            [ottla.serde.json :refer [serialize-json deserialize-json]]
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
                                                {:deserialize-key deserialize-edn
                                                 :deserialize-value deserialize-edn
                                                 :exception-handler ex-handler})]
      (ottla/append *config* topic [{:key 1 :value 42}] {:serialize-key serialize-edn
                                                         :serialize-value serialize-edn})
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
                                                {:deserialize-key deserialize-json
                                                 :deserialize-value deserialize-json
                                                 :exception-handler ex-handler})]
      (ottla/append *config* topic [{:key 1 :value "42...."}] {:serialize-key serialize-json
                                                               :serialize-value serialize-json})
      (is (= :received (deref p 100 :timed-out))))
    (is (= [] (mapv Throwable->map @ex)))
    (is (= [{:meta nil :key 1 :value "42...." :topic topic}]
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
                                               {:deserialize-key deserialize-edn
                                                :exception-handler ex-handler})]
      (ottla/append *config* topic [{:key 1 :value 42}] {:serialize-key serialize-edn
                                                         :serialize-value serialize-edn})
      (is (not= :timed-out (deref r1 100 :timed-out)))
      (is (= :running (consumer/status consumer)))
      (ottla/append *config* topic [{:key 2 :value 42}] {:serialize-key serialize-edn
                                                         :serialize-value serialize-edn})
      (is (not= :timed-out (deref ex 100 :timed-out)))
      (is (= :running (consumer/status consumer)))
      (ottla/append *config* topic [{:key 3 :value 42}] {:serialize-key serialize-edn
                                                         :serialize-value serialize-edn})
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
      (ottla/append *config* topic [{:key 1 :value 42}] {:serialize-key serialize-edn
                                                         :serialize-value serialize-edn})
      (is (= :running (consumer/status consumer)))
      (is (not= :timed-out (deref ex 100 :timed-out)))
      (Thread/sleep 10)
      (is (not= :running (consumer/status consumer))))))
