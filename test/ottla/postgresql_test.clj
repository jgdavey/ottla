(ns ottla.postgresql-test
  (:require [clojure.test :as test :refer [deftest is testing are]]
            [ottla.test-helpers :as th :refer [*config*]]
            [ottla.postgresql :as postgres]
            [matcher-combinators.test]
            [pg.core :as pg]))

(declare match?)

(test/use-fixtures :each
  th/config-fixture
  th/connection-fixture)

(deftest legal-identifier-test
  (are [id] (postgres/legal-identifier? id)
    "ottla"
    "_kinda_hidden"
    "events1"
    "snake_case"
    "PascalCase"
    "camelCase"
    "in_the_year_2000")
  (are [id] (not (postgres/legal-identifier? id))
    "this_is_a_string_that_would_be_legal_except_that_it_is_greater_than_63_characters"
    "1number"
    "kebab-case"
    "bang!"))

(deftest topics-notify
  (let [topic "my-topic"]
    (pg/with-connection [conn2 th/conn-params]
      (let [{:keys [topic]} (postgres/create-topic *config* topic)]
        (is (= topic "my-topic"))
        (pg/listen conn2 topic)
        (is (= {:inserted 1}
               (postgres/insert-records *config* topic [{:key (.getBytes "hi" "UTF-8")
                                                         :value (.getBytes "bye" "UTF-8")}])))
        (Thread/sleep 10)
        (is (pos? (pg/poll-notifications conn2)))
        (is (= [{:message "1"
                 :channel topic
                 :msg :NotificationResponse
                 :self? false}]
               (mapv #(dissoc % :pid) (pg/drain-notifications conn2))))))))

(deftest subscriptions-test
  (let [topic "topic"
        {:keys [topic]} (postgres/create-topic *config* topic
                                               :key-type :text
                                               :value-type :text)
        selection (postgres/normalize-selection topic)]
    (is (= true (postgres/ensure-subscription *config* selection)))
    (is (= false  (postgres/ensure-subscription *config* selection)))
    (is (= {:inserted 1}
           (postgres/insert-records *config* topic [{:key "hi"
                                                     :value "bye"
                                                     :meta {:x "b"}}])))

    (testing "In :auto commit-mode, fetches and commits"
      (is (match? [{:key "hi"
                    :value "bye"
                    :topic topic
                    :meta {:x "b"}}]
                  (postgres/fetch-records! *config* selection)))
      ;; Second fetch is empty
      (is (= [] (postgres/fetch-records! *config* selection))))
    (testing "In :manual commit-mode, only commits when told to"
      (is (= {:inserted 1}
             (postgres/insert-records *config* topic [{:key "yes"
                                                       :value "sir"
                                                       :meta {:x "b"}}])))
      (let [selection (assoc selection :commit-mode :manual)
            received (postgres/fetch-records! *config* selection)]
        (is (match? [{:key "yes"
                      :value "sir"
                      :topic topic
                      :eid number?
                      :meta {:x "b"}}]
                    received))
        (is (match? [{:key "yes"
                      :value "sir"
                      :topic topic
                      :meta {:x "b"}}]
                    (postgres/fetch-records! *config* selection)))
        (is (= {:updated 1} (postgres/commit-offset! *config* selection (-> received first :eid))))
        (is (= [] (postgres/fetch-records! *config* selection)))))
    (testing "Can rewind with reset-offset!"
      (is (= {:updated 1} (postgres/reset-offset! *config* selection 0)))
      (is (match? [{:key "hi"
                    :value "bye"
                    :topic topic}
                   {:key "yes"
                    :value "sir"
                    :topic topic}]
                  (postgres/fetch-records! *config* selection)))
      (is (= [] (postgres/fetch-records! *config* selection))))))

(deftest topic-subscriptions-test
  (let [topic-1 "topic-1"
        topic-2 "topic.2"
        _ (postgres/create-topic *config* topic-1 :key-type :text :value-type :text)
        _ (postgres/create-topic *config* topic-2 :key-type :text :value-type :text)
        selection-1 (postgres/normalize-selection topic-1)
        selection-2a (postgres/normalize-selection topic-2)
        selection-2b (postgres/normalize-selection {:topic topic-2 :group "nice"})]
    (is (= true (postgres/ensure-subscription *config* selection-2a)))
    (is (= true (postgres/ensure-subscription *config* selection-2b)))
    (is (= {:inserted 1}
           (postgres/insert-records *config* topic-2 [{:key "hi"
                                                       :value "bye"
                                                       :meta {:x "b"}}])))
    (postgres/fetch-records! *config* selection-2a)
    (is (match? [{:topic topic-1
                  :subscriptions []}
                 {:topic topic-2
                  :subscriptions [{:group "default"
                                   :offset 1}
                                  {:group "nice"
                                   :offset 0}]}]
                (postgres/topic-subscriptions *config*)))))
