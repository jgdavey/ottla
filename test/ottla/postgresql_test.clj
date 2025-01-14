(ns ottla.postgresql-test
  (:require [clojure.test :as test :refer [deftest is testing]]
            [ottla.test-helpers :as th :refer [*config*]]
            [ottla.postgresql :as postgres]
            [pg.core :as pg]))

(test/use-fixtures :each
  th/config-fixture)

(deftest topics-notify
  (let [topic "my-topic"
        messages (atom [])]
    (pg/with-connection [conn2 (assoc th/*conn-params*
                                      :fn-notification (fn [message]
                                                         (swap! messages conj message)))]
      (pg/listen conn2 topic)
      (postgres/create-topic *config* topic)
      (is (= {:inserted 1}
             (postgres/insert-records *config* topic [{:key (.getBytes "hi" "UTF-8")
                                                       :value (.getBytes "bye" "UTF-8")}])))
      (Thread/sleep 10)
      (pg/poll-notifications conn2)
      (is (= [{:message "1"
               :channel topic
               :msg :NotificationResponse
               :self? false}]
             (mapv #(dissoc % :pid) @messages))))))
