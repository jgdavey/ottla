(ns ottla.core
  (:require [ottla.postgresql :as postgres]
            [ottla.consumer :as consumer]))

(defn make-config
  [conn-map & {:as opts}]
  (merge {:schema "ottla"}
         opts
         {:conn-map conn-map}))

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
  [config topic records & {:as opts}]
  (postgres/insert-records config topic records opts))

(defn start-consumer
  [config selection handler & {:as opts}]
  (consumer/start-consumer config selection handler opts))
