(ns ottla.consumer
  (:require [ottla.postgresql :as postgres]
            [clojure.tools.logging :as log]
            [pg.core :as pg])
  (:import [java.lang AutoCloseable]
           [java.io Closeable]
           [java.time Instant]
           [java.util.concurrent
            Executors
            ScheduledExecutorService
            ExecutorService
            ThreadFactory
            ThreadPoolExecutor
            ThreadPoolExecutor$DiscardOldestPolicy
            TimeUnit
            ArrayBlockingQueue]
           #_[org.pg Connection]))

(set! *warn-on-reflection* true)

(defn sleep [^long ms]
  (Thread/sleep ms))

(defn- thread-factory
  ^ThreadFactory [^String name]
  (let [counter (java.util.concurrent.atomic.AtomicInteger.)]
    (reify ThreadFactory
      (newThread [_ r]
        (doto (Thread. r (str name "-" (.getAndIncrement counter)))
          (.setDaemon true))))))

(defn- ->thread-prefix [{:keys [topic group]}]
  (str "ottla-" (postgres/normalize-identifier-name topic) "-" (postgres/normalize-identifier-name group)))

(defprotocol IShutdown
  (graceful-shutdown [_])
  (shutdown-await [_ await-time-ms]))

(defprotocol IConsumerStatus
  (status [_]))

(deftype Consumer [^ExecutorService poller
                   ^ExecutorService worker
                   ^ExecutorService listener
                   ^AutoCloseable conn
                   await-close-ms
                   selection
                   stats]

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

  IConsumerStatus
  (status [_]
    (let [state (cond
                  (.isTerminated worker) :terminated
                  (.isShutdown worker) :shutdown
                  :else :running)]
      (merge {:state state
              :topic (:topic selection)
              :group (:group selection)}
             @stats)))

  Closeable
  (close [this]
    (shutdown-await this await-close-ms))

  Object
  (toString [this]
    (let [{:keys [state topic group]} (status this)]
      (str "#Consumer" (pr-str {:state state :topic topic :group group})))))

(defn default-exception-handler
  [^Exception e]
  (log/error e "Uncaught exception in consumer handler"))

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
         (handler config records))
       (count records)))))

(defn start-consumer
  ^Consumer
  [{:keys [conn-map] :as config}
   {:keys [topic] :as basic-selection}
   handler
   {:keys [poll-ms deserialize-key deserialize-value xform
           exception-handler await-close-ms listen-ms reconnect-ms max-records]
    :or {poll-ms 15000
         await-close-ms 1000
         listen-ms 2
         reconnect-ms 2000
         exception-handler default-exception-handler
         xform identity
         max-records 100}
    :as opts}]
  (assert (map? conn-map) "conn-map must be a connection map")
  (assert (string? topic) "topic is required")
  (let [{:keys [conn] :as config} (postgres/connect-config config)
        deserialized (postgres/deserializer-xf config topic opts)
        xf (comp deserialized xform)
        selection (assoc (postgres/normalize-selection basic-selection) :xf xf :limit max-records)
        _ (assert (contains? postgres/commit-modes
                             (:commit-mode selection)) "unknown commit-mode")
        _ (postgres/ensure-subscription config selection)
        thread-prefix (->thread-prefix selection)
        ^ScheduledExecutorService poller (Executors/newSingleThreadScheduledExecutor
                                          (thread-factory (str thread-prefix "-poller")))
        ^ExecutorService worker (-> (ThreadPoolExecutor. 1 1 0 TimeUnit/MILLISECONDS
                                                         (ArrayBlockingQueue. 1)
                                                         (thread-factory (str thread-prefix "-worker"))
                                                         (ThreadPoolExecutor$DiscardOldestPolicy.))
                                    (Executors/unconfigurableExecutorService))
        ^ExecutorService listener (Executors/newSingleThreadExecutor
                                    (thread-factory (str thread-prefix "-listener")))
        stats (atom {:record-count 0
                     :last-processed-at nil})
        consumer (Consumer. poller worker listener conn await-close-ms selection stats)
        do-work-fn (fn* do-work-fn [max]
                        (.execute worker
                                  (fn* []
                                       (try
                                         (let [num-handled (fetch-and-handle config (assoc selection :max max) handler)]
                                           (when (pos? num-handled)
                                             (swap! stats (fn [s]
                                                            (-> s
                                                                (update :record-count + num-handled)
                                                                (assoc :last-processed-at (Instant/now))))))
                                           (when (= max-records num-handled)
                                             ;; Immediate re-queue (like a poll) since there's likely more
                                             (do-work-fn max)))
                                         (catch Exception ex
                                           (let [ex-result (exception-handler ex)]
                                             (case ex-result
                                               :ottla/shutdown (graceful-shutdown consumer)
                                               nil)))))))

        _ (.scheduleAtFixedRate poller (fn* [] (do-work-fn nil)) 0 poll-ms TimeUnit/MILLISECONDS)
        listening? (promise)
        listen-fn (fn* []
                       (loop [reconnects 0]
                         (when-not (.isShutdown listener)
                           (try
                             (pg/with-connection [c conn-map]
                               (try
                                 (pg/listen c topic)
                                 (if (realized? listening?)
                                   ;; This is a reconnect
                                   (do-work-fn nil)
                                   ;; This is the first run
                                   (deliver listening? true))
                                 (loop []
                                   (pg/poll-notifications c)
                                   (let [bail? (try (doseq [{:keys [message]} (pg/drain-notifications c)]
                                                      (do-work-fn (try (parse-long message)
                                                                       (catch NumberFormatException _ nil))))
                                                    (catch org.pg.error.PGErrorIO _
                                                      true)
                                                    (catch Exception ex
                                                      (exception-handler ex)
                                                      true))]
                                     (when-not bail?
                                       (sleep listen-ms)
                                       (recur))))
                                 (finally
                                   (try (pg/unlisten c topic) (catch Exception _)))))
                             (catch InterruptedException _)
                             (catch Exception e
                               (log/error e "Error in Listener")))
                           (when-not (.isShutdown listener)
                             (log/warnf "Listener disconnected, reconnecting in %dms (%d previous attempts)" reconnect-ms reconnects)
                             (try (sleep reconnect-ms)
                                  (catch InterruptedException _))
                             (recur (inc reconnects))))))
        _ (.submit listener ^Callable listen-fn)]
    (when-not (deref listening? reconnect-ms false)
      (log/warnf "Not listening after %s ms" reconnect-ms))
    consumer))
