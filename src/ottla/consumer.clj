(ns ottla.consumer
  (:require [ottla.postgresql :as postgres]
            [ottla.serde.registry :refer [get-deserializer!]]
            [pg.core :as pg])
  (:import [java.lang AutoCloseable]
           [java.io Closeable]
           [java.util.concurrent
            Executors
            ScheduledExecutorService
            ExecutorService
            ThreadPoolExecutor
            ThreadPoolExecutor$DiscardOldestPolicy
            TimeUnit
            ArrayBlockingQueue]
           #_[org.pg Connection]))

(defprotocol IShutdown
  (graceful-shutdown [_])
  (shutdown-await [_ await-time-ms])
  (status [_]))

(deftype Consumer [^ExecutorService poller
                   ^ExecutorService worker
                   ^ExecutorService listener
                   ^AutoCloseable conn
                   await-close-ms]

  IShutdown
  (graceful-shutdown [_]
    (.shutdownNow poller)
    (.shutdownNow listener)
    (.shutdown worker))

  (shutdown-await [this await-time-ms]
    (graceful-shutdown this)
    (try
      (when-not (.awaitTermination worker await-time-ms TimeUnit/MILLISECONDS)
        (.shutdownNow worker))
      (.close conn)
      (catch InterruptedException _
        (.shutdownNow worker)
        (.close conn)
        (.interrupt ^Thread (Thread/currentThread)))))
  (status [_]
    (cond
      (.isTerminated worker) :terminated
      (.isShutdown worker) :shutdown
      :else :running))

  Closeable
  (close [this]
    (shutdown-await this await-close-ms))

  Object
  (toString [this]
    (format "Consumer[%s]" (str (status this)))))

(defn default-exception-handler
  [^Exception e]
  (println "Uncaught exception in consumer handler:" e))

(defmacro with-commit-mode [[conn commit-mode] & body]
  `(if (= :tx-wrap ~commit-mode)
     (pg/with-transaction [~conn ~conn]
       ~@body)
     (do ~@body)))

(defn- fetch-and-handle
  [{:keys [conn] :as config} {:keys [commit-mode] :as selection} handler]
  (pg/with-connection
   [conn conn]
   (with-commit-mode [conn commit-mode]
     (let [config (assoc config :conn conn)
           records (postgres/fetch-records* config selection)]
       (when (seq records)
         (handler config records))))))

(defn start-consumer
  ^Consumer
  [{:keys [conn-map] :as config}
   {:keys [topic] :as basic-selection}
   handler
   {:keys [poll-ms deserialize-key deserialize-value xform
           exception-handler await-close-ms listen-ms]
    :or {poll-ms 15000
         await-close-ms 1000
         listen-ms 2
         exception-handler default-exception-handler
         xform identity}
    :as opts}]
  (assert (map? conn-map) "conn-map must be a connection map")
  (assert (string? topic) "topic is required")
  (let [{:keys [conn] :as config} (postgres/connect-config config)
        {:keys [key-type value-type]} (postgres/fetch-topic config topic)
        deserialize-key (get-deserializer! (or deserialize-key identity) key-type)
        deserialize-value (get-deserializer! (or deserialize-value identity) value-type)
        xf (comp (map (fn [rec] (-> rec
                                    (update :key deserialize-key)
                                    (update :value deserialize-value))))
                 xform)
        selection (assoc (postgres/normalize-selection basic-selection) :xf xf)
        _ (assert (contains? postgres/commit-modes
                             (:commit-mode selection)) "unknown commit-mode")
        _ (postgres/ensure-subscription config selection)
        ^ScheduledExecutorService poller (Executors/newSingleThreadScheduledExecutor)
        ^ExecutorService worker (-> (ThreadPoolExecutor. 1 1 0 TimeUnit/MILLISECONDS
                                                         (ArrayBlockingQueue. 1)
                                                         (ThreadPoolExecutor$DiscardOldestPolicy.))
                                    (Executors/unconfigurableExecutorService))
        ^ExecutorService listener (Executors/newSingleThreadExecutor)
        consumer (Consumer. poller worker listener conn await-close-ms)
        do-work-fn (fn* [max] (.execute worker
                                        (fn* []
                                             (try
                                               (fetch-and-handle config (assoc selection :max max) handler)
                                               (catch Exception ex
                                                 (let [ex-result (exception-handler ex)]
                                                   (case ex-result
                                                     :ottla/shutdown (graceful-shutdown consumer)
                                                     nil)))))))

        _ (.scheduleAtFixedRate poller (fn* [] (do-work-fn nil)) 0 poll-ms TimeUnit/MILLISECONDS)
        listening? (promise)
        listen-fn (fn* []
                    (pg/with-connection [c conn-map]
                      (try
                        (pg/listen c topic)
                        (deliver listening? true)
                        (loop []
                          (pg/poll-notifications c)
                          (let [bail? (try (doseq [{:keys [message]} (pg/drain-notifications c)]
                                             (do-work-fn (parse-long message)))
                                           (catch org.pg.error.PGErrorIO _
                                             (Thread/interrupted))
                                           (catch Exception ex
                                             (println "Exception while listening: " (class ex) (ex-message ex))
                                             true))]
                            (when-not bail?
                              (Thread/sleep listen-ms)
                              (recur))))
                        (finally
                          (pg/unlisten c topic)))))
        _ (.submit listener ^Callable listen-fn)]
    (when-not (deref listening? 100 false)
      (println "Warning: Not listening after 100 ms"))
    consumer))
