(ns ottla.consumer
  (:require [ottla.postgresql :as postgres]
            [pg.core :as pg])
  (:import [org.pg Connection]
           [java.lang AutoCloseable]
           [java.io Closeable]
           [java.nio.channels ReadPendingException]
           [java.util.concurrent
            Executors
            ScheduledExecutorService
            ExecutorService
            ThreadPoolExecutor
            ThreadPoolExecutor$DiscardOldestPolicy
            TimeUnit
            ArrayBlockingQueue]))

(set! *warn-on-reflection* true)

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
        (.interrupt (Thread/currentThread)))))
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
  [{:keys [conn-map] :as config}
   {:keys [topic] :as basic-selection}
   handler
   {:keys [poll-ms deserialize-key deserialize-value xform
           exception-handler await-close-ms listen-ms]
    :or {poll-ms 15000
         await-close-ms 1000
         listen-ms 2
         deserialize-key identity
         deserialize-value identity
         exception-handler default-exception-handler
         xform identity}
    :as opts}]
  (assert (map? conn-map) "conn-map must be a connection map")
  (assert (string? topic) "topic is required")
  (let [{:keys [conn] :as config} (postgres/connect-config config)
        xf (comp (map (fn [rec] (-> rec
                                    (update :key deserialize-key)
                                    (update :value deserialize-value))))
                 xform)
        selection (assoc (postgres/normalize-selection basic-selection) :xf xf)
        _ (assert (contains? postgres/commit-modes
                             (:commit-mode selection)) "unknown commit-mode")
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
                                                     :ottla/shutdown (graceful-shutdown consumer))))))))

        _ (.scheduleAtFixedRate poller (fn* [] (do-work-fn nil)) 0 poll-ms TimeUnit/MILLISECONDS)
        _ (.submit listener ^Callable (fn* []
                                           (pg/with-connection [c conn-map]
                                             (try
                                               (pg/listen c topic)

                                               (loop []
                                                 (let [bail? (try (when (pg/notifications? c)
                                                                    (doseq [{:keys [message]} (pg/drain-notifications! c)]
                                                                      (do-work-fn (parse-long message)))
                                                                    (.isInterrupted (Thread/currentThread)))
                                                                  (catch org.pg.error.PGErrorIO _
                                                                    (.isInterrupted (Thread/currentThread)))
                                                                  (catch Exception ex
                                                                    (println "Exception while listening: " (class ex) (ex-message ex))
                                                                    true))]
                                                   (when-not bail?
                                                     (Thread/sleep (long listen-ms))
                                                     (recur))))
                                               (finally
                                                 #_(println "Listener has exited")
                                                 (pg/unlisten c topic))))))]
    consumer))
