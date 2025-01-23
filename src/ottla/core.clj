(ns ottla.core
  (:require [ottla.postgresql :as postgres]
            [ottla.consumer :as consumer])
  (:import [java.lang AutoCloseable]))

(defn make-config
  [conn-map & {:as opts}]
  (merge {:schema "ottla"}
         opts
         {:conn-map conn-map}))

(def shutdown :ottla/shutdown)

(defmacro with-connected-config
  [[sym config] & body]
  `(let* [config# ~config]
     (if (:conn config#)
       (let* [~sym config#]
         ~@body)
       (let* [config# (postgres/connect-config config#)
              ~sym config#]
         (try
           ~@body
           (finally
             (.close ^AutoCloseable (:conn config#))))))))

(defn init!
  [config]
  (postgres/ensure-schema config))

(defn add-topic!
  [config topic]
  (postgres/create-topic config (name topic)))

(defn remove-topic!
  [config topic]
  (postgres/delete-topic config (name topic)))

(defn start-consumer
  [config selection handler & {:as opts}]
  (consumer/start-consumer config selection handler opts))

(defn append
  [config topic records & {:as opts}]
  (postgres/insert-records config (name topic) records opts))
