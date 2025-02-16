(ns ottla.test-helpers
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [ottla.core :as ottla]
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
