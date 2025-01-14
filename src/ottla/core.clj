(ns ottla.core
  (:require [ottla.postgresql :as postgres]))

(defn make-config
  [pg-connection & {:as opts}]
  (merge {:schema "ottla"}
         opts
         {:conn pg-connection}))

(defn init
  [config]
  (postgres/ensure-schema config))

(defn add-topic!
  [config topic]
  (postgres/create-topic config topic))

(defn remove-topic!
  [config topic]
  (postgres/delete-topic config topic))

(defn append
  [config topic records]
  (postgres/insert-records config topic records))
