(ns ottla.test-helpers
  (:require [ottla.postgresql :as postgres]
            [pg.core :as pg]))

(def ^:dynamic *conn-params*
  {:database "test"
   :user "jgdavey"})

(def ^:dynamic *conn* nil)

(def ^:dynamic *config* nil)

(def default-schema "ottla")

(defn reset-schema!
  [{:keys [conn schema] :as config}]
  (pg/execute conn (str "drop schema if exists \"" schema "\" cascade"))
  (postgres/ensure-schema config))

(defn config-fixture
  [f]
  (pg/with-connection [conn *conn-params*]
    (binding [*conn* conn
              *config* {:schema default-schema :conn conn}]
      (reset-schema! *config*)
      (f))))
