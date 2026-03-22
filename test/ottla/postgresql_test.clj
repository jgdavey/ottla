(ns ottla.postgresql-test
  (:require [clojure.test :as test :refer [deftest is testing are]]
            [ottla.test-helpers :as th :refer [*config*]]
            [ottla.postgresql :as postgres]
            [matcher-combinators.test]
            [matcher-combinators.matchers :as m]
            [pg.core :as pg]))

(declare match?)

(test/use-fixtures :each
  th/instrument-fixture
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

(deftest topic-name-collision-test
  (postgres/create-topic *config* "my-topic")
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"collision"
                        (postgres/create-topic *config* "my_topic")))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"collision"
                        (postgres/ensure-topic *config* "my_topic"))))

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

(deftest list-subscriptions-test
  (let [topic "events"
        instant? (m/pred #(instance? java.time.Instant %) "Instant")
        _ (postgres/create-topic *config* topic :key-type :text :value-type :text)
        selection (postgres/normalize-selection topic)]
    (is (= [] (postgres/list-subscriptions *config*)))
    (postgres/ensure-subscription *config* selection)
    (postgres/insert-records *config* topic [{:key "a" :value "1"}
                                             {:key "b" :value "2"}])
    (postgres/insert-records *config* topic [{:key "c" :value "3"}])
    (let [result (postgres/list-subscriptions *config*)]
      (is (match? [{:topic topic
                    :group "default"
                    :offset 0
                    :updated-at nil
                    :processing-delay nil
                    :topic-eid 3
                    :lag 3
                    :timestamp nil
                    :topic-timestamp instant?
                    :timestamp-lag nil}]
                  result)))
    ;; Similate lag by only fetching a single record
    (postgres/fetch-records! *config* (assoc selection :limit 1))
    (let [result (postgres/list-subscriptions *config*)]
      (is (match? [{:topic topic
                    :group "default"
                    :offset 1
                    :updated-at instant?
                    :processing-delay (m/pred #(instance? java.time.Duration %) "Duration")
                    :topic-eid 3
                    :lag 2
                    :timestamp instant?
                    :topic-timestamp instant?
                    :timestamp-lag (m/via #(.toMillis %)
                                          ;; Between 0 and 10
                                          (m/within-delta 5 5))}]
                  result)))
    ;; "Catch up" with the topic
    (postgres/fetch-records! *config* selection)
    (let [result (postgres/list-subscriptions *config*)]
      (is (match? [{:topic topic
                    :group "default"
                    :offset 3
                    :updated-at instant?
                    :processing-delay (m/pred #(instance? java.time.Duration %) "Duration")
                    :topic-eid 3
                    :lag 0
                    :timestamp instant?
                    :topic-timestamp instant?
                    :timestamp-lag (m/pred #(and (instance? java.time.Duration %)
                                                 (.isZero %)) "Duration")}]
                  result)))))

(deftest list-subscriptions-filtering-test
  (let [t1 "alpha"
        t2 "beta"
        t3 "no-subs"
        _ (postgres/create-topic *config* t1 :key-type :text :value-type :text)
        _ (postgres/create-topic *config* t2 :key-type :text :value-type :text)
        _ (postgres/create-topic *config* t3 :key-type :text :value-type :text)]
    (postgres/fetch-records! *config* {:topic t1 :group "g1"})
    (postgres/fetch-records! *config* {:topic t1 :group "g2"})
    (postgres/fetch-records! *config* {:topic t2 :group "g1"})

    (testing "no filter returns all subscriptions (no topics that don't have subscriptions)"
      (is (= 3 (count (postgres/list-subscriptions *config*)))))

    (testing "filter by topic string returns all groups for that topic"
      (let [result (postgres/list-subscriptions *config* {:selections [t1]})]
        (is (= 2 (count result)))
        (is (every? #(= t1 (:topic %)) result))))

    (testing "filter by selection map with group returns only that group"
      (let [result (postgres/list-subscriptions *config* {:selections [{:topic t1 :group "g1"}]})]
        (is (= 1 (count result)))
        (is (= "g1" (:group (first result))))))

    (testing "multiple selections across topics"
      (let [result (postgres/list-subscriptions *config* {:selections [t1 {:topic t2 :group "g1"}]})]
        (is (= 3 (count result)))))

    (testing "topic-subscriptions with topic filter shows only specified topics"
      (let [result (postgres/topic-subscriptions *config* {:selections [t1]})]
        (is (= [t1] (mapv :topic result)))))

    (testing "topic-subscriptions with group filter narrows subscriptions"
      (let [result (postgres/topic-subscriptions *config* {:selections [{:topic t1 :group "g1"}]})]
        (is (= 1 (count result)))
        (is (= 1 (count (:subscriptions (first result)))))
        (is (= "g1" (:group (first (:subscriptions (first result))))))))))

(deftest ensure-and-create-subscription-test
  (let [topic "events"
        _ (postgres/create-topic *config* topic :key-type :text :value-type :text)
        _ (postgres/insert-records *config* topic (mapv (fn [k] {:key k :value k})
                                                        ["a" "b" "c" "d" "e"]))
        sel (postgres/normalize-selection topic)
        sel-group (postgres/normalize-selection {:topic topic :group "other"})]

    (testing "ensure-subscription: creates new subscription at :earliest by default"
      (is (= true (postgres/ensure-subscription *config* sel)))
      (is (= 5 (count (postgres/fetch-records! *config* sel)))))

    (testing "ensure-subscription: idempotent when subscription already exists"
      (is (= false (postgres/ensure-subscription *config* sel)))
      (is (= false (postgres/ensure-subscription *config* sel :from :latest))))

    (testing "ensure-subscription :from :latest starts at max eid"
      (is (= true (postgres/ensure-subscription *config* sel-group :from :latest)))
      (is (= [] (postgres/fetch-records! *config* sel-group))))

    (testing "ensure-subscription :from <eid> starts at specific eid"
      (let [sel-specific (postgres/normalize-selection {:topic topic :group "specific"})]
        (is (= true (postgres/ensure-subscription *config* sel-specific :from 3)))
        (is (= 2 (count (postgres/fetch-records! *config* sel-specific))))))

    (testing "create-subscription: creates new subscription"
      (let [sel-new (postgres/normalize-selection {:topic topic :group "new-group"})]
        (is (= true (postgres/create-subscription *config* sel-new :from :latest)))))

    (testing "create-subscription: throws if subscription already exists"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"already exists"
                            (postgres/create-subscription *config* sel))))))

(deftest delete-subscription-test
  (let [topic "events"
        _ (postgres/create-topic *config* topic :key-type :text :value-type :text)
        sel (postgres/normalize-selection topic)
        sel-other (postgres/normalize-selection {:topic topic :group "other"})]
    (postgres/ensure-subscription *config* sel)
    (postgres/ensure-subscription *config* sel-other)

    (testing "returns true when subscription existed and is removed"
      (is (= true (postgres/delete-subscription *config* sel))))

    (testing "subscription is gone from list-subscriptions"
      (is (= ["other"] (mapv :group (postgres/list-subscriptions *config*)))))

    (testing "returns false when subscription does not exist"
      (is (= false (postgres/delete-subscription *config* sel))))))

(deftest trim-topic-test
  (let [topic "events"
        _ (postgres/create-topic *config* topic :key-type :text :value-type :text)
        insert! #(postgres/insert-records *config* topic (mapv (fn [k] {:key k :value k}) %))
        ;; Helper: fetch all remaining keys using a dedicated group that resets each call
        all-keys (fn []
                   (let [sel (postgres/normalize-selection {:topic topic :group "check"})
                         sel (assoc sel :commit-mode :manual)]
                     (postgres/ensure-subscription *config* sel)
                     (postgres/reset-offset! *config* sel 0)
                     (mapv :key (postgres/fetch-records! *config* sel))))]

    (testing "requires exactly one mode"
      (is (thrown? IllegalArgumentException
                   (postgres/trim-topic *config* topic)))
      (is (thrown? IllegalArgumentException
                   (postgres/trim-topic *config* topic :before-eid 5 :all? true))))

    (testing "returns 0 for empty topic"
      (is (= 0 (postgres/trim-topic *config* topic :all? true :ignore-subscriptions? true))))

    ;; Insert records; eids are 1-5
    (insert! ["a" "b" "c" "d" "e"])

    (testing ":before-eid deletes records with eid strictly less than the given value"
      ;; Uses :ignore-subscriptions? to avoid interference from the check group at cursor 0
      (is (= 2 (postgres/trim-topic *config* topic :before-eid 3 :ignore-subscriptions? true)))
      (is (= ["c" "d" "e"] (all-keys))))

    (testing ":all? deletes all records before the current max eid"
      ;; MAX eid = 5 ("e"); deletes WHERE eid < 5 → removes "c" and "d" (2 records)
      ;; "e" (the max) is preserved
      (is (= 2 (postgres/trim-topic *config* topic :all? true :ignore-subscriptions? true)))
      (is (= ["e"] (all-keys))))

    (testing ":before-timestamp deletes records with timestamp before the given value"
      ;; "e" was inserted earlier; capture mid-point then insert two more
      (let [mid (java.time.Instant/now)
            _ (Thread/sleep 5)]
        (insert! ["f" "g"])
        (is (= 1 (postgres/trim-topic *config* topic :before-timestamp mid :ignore-subscriptions? true)))
        (is (= ["f" "g"] (all-keys)))))))

(deftest trim-topic-subscription-aware-test
  (let [topic "events"
        _ (postgres/create-topic *config* topic :key-type :text :value-type :text)
        insert! #(postgres/insert-records *config* topic (mapv (fn [k] {:key k :value k}) %))]

    (insert! ["a" "b" "c" "d" "e"])
    ;; eids 1-5

    (testing "no subscriptions: proceeds without restriction"
      (is (= 4 (postgres/trim-topic *config* topic :all? true))))

    ;; Re-insert to give the subscription tests a known state; eids 6-10
    (insert! ["f" "g" "h" "i" "j"])
    ;; eids 5-10 now exist

    (let [sel (postgres/normalize-selection topic)
          _ (postgres/ensure-subscription *config* sel)]

      (testing "cursor at 0: clamps cutoff to 0, deletes nothing"
        ;; MAX=10, sub-floor=0, DELETE WHERE eid < 0 → 0 records
        (is (= 0 (postgres/trim-topic *config* topic :all? true))))

      ;; Advance cursor by consuming 2 records (:auto commit); cursor → eid of "f" (6)
      (postgres/fetch-records! *config* (assoc sel :limit 2))

      (testing "subscription-aware: clamps cutoff to min cursor"
        ;; MAX=10, sub-floor=6, DELETE WHERE eid < 6 → deletes eid 5 ("e") only
        (is (= 1 (postgres/trim-topic *config* topic :all? true))))

      (testing ":ignore-subscriptions? bypasses the subscription floor"
        ;; eid 5 deleted above; remaining eids 6-10, MAX=10
        ;; DELETE WHERE eid < 10 → deletes eids 6,7,8,9 (4 records)
        (is (= 4 (postgres/trim-topic *config* topic :all? true :ignore-subscriptions? true)))))))

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
                                   :offset 1
                                   :lag 0}
                                  {:group "nice"
                                   :offset 0
                                   :lag 1}]}]
                (postgres/topic-subscriptions *config*)))))
