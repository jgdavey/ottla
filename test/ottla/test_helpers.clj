(ns ottla.test-helpers
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [ottla.core :as ottla]
            [clojure.string :as str]
            [clojure.spec.test.alpha :as st]
            [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [pg.core :as pg]))

(def config
  (aero/read-config
   (io/resource "test-config.edn")))

(def conn-params (:pg config))

(def ^:dynamic *conn* nil)

(def ^:dynamic *config* nil)

(def default-schema "ottla")

(defn reset-schema!
  [{:keys [conn conn-map schema] :as config}]
  (pg/with-connection [conn (or conn conn-map)]
    (pg/execute conn (str "drop schema if exists \"" schema "\" cascade"))
    (ottla/init! config)))

(defn config-fixture
  [f]
  (binding [*config* {:schema default-schema
                      :conn-map conn-params}]
    (f)))

(defn connection-fixture
  [f]
  (ottla/with-connected-config [cfg {:schema default-schema
                                     :conn-map conn-params}]
    (binding [*conn* (:conn cfg)
              *config* cfg]
      (reset-schema! *config*)
      (f))))

(defn instrument-fixture
  [f]
  (try
    (st/instrument)
    (f)
    (finally
      (st/unstrument))))

(defn check-sym
  [sym-or-syms & {:as opts}]
  (let [check-results (st/check sym-or-syms opts)
        checks-passed? (every? nil? (map :failure check-results))]
    (if checks-passed?
      (t/do-report {:type    :pass
                    :message (str "Generative tests pass for "
                                  (str/join ", " (map :sym check-results)))})
      (doseq [failed-check (filter :failure check-results)
              :let [r (st/abbrev-result failed-check)
                    failure (:failure r)]]
        (t/do-report
         {:type     :fail
          :message  (with-out-str (s/explain-out failure))
          :expected (->> failed-check :spec s/specize* :ret)
          :actual   (if (instance? Throwable failure)
                      failure
                      (::st/val failure))})))
    checks-passed?))
