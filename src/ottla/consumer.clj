(ns ottla.consumer
  (:require [ottla.postgresql :as postgres]
            [pg.core :as pg])
  (:import [org.pg Connection]
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
  (shutdown [_ await-time-ms])
  (status [_]))


(deftype Consumer [^ExecutorService poller
                   ^ExecutorService worker
                   ^ExecutorService listener
                   ^Connection conn
                   await-close-ms]

  IShutdown
  (shutdown [_ await-time-ms]
    (.shutdownNow poller)
    (.shutdownNow listener)
    (.shutdown worker)
    (try
      (when-not (.awaitTermination worker await-time-ms TimeUnit/MILLISECONDS)
        (.shutdownNow worker))
      (catch InterruptedException _
        (.shutdownNow worker)
        (.interrupt (Thread/currentThread)))
      (finally
        (.close conn))))
  (status [_]
    (cond
      (.isTerminated worker) :terminated
      (.isShutdown worker) :shutdown
      :else :running))

  Closeable
  (close [this]
    (shutdown this await-close-ms))

  Object
  (toString [this]
    (format "Consumer[%s]" (str (status this)))))

(defn default-exception-handler
  [^Exception e]
  (prn e))

(defn- fetch-and-handle
  [{:keys [conn] :as config} selection handler]
  (pg/on-connection [conn conn]
                    (let [config (assoc config :conn conn)
                          records (postgres/fetch-records! config selection)]
                      (when (seq records)
                        (handler records)))))

(defn start-consumer
  [{:keys [conn-map] :as config}
   {:keys [topic] :as basic-selection}
   handler
   {:keys [poll-ms deserialize-key deserialize-value xform exception-handler await-close-ms]
    :or {poll-ms 15000
         await-close-ms 1000
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
                                                     :ottla/shutdown (.close consumer))))))))

        _ (.scheduleAtFixedRate poller (fn* [] (do-work-fn nil)) 0 poll-ms TimeUnit/MILLISECONDS)
        _ (.submit listener ^Callable (fn* []
                                           (pg/with-connection [c (assoc conn-map
                                                                         :fn-notification
                                                                         (fn [{:keys [message]}]
                                                                           (do-work-fn (parse-long message))))]
                                             (pg/listen c topic)
                                             (try
                                               (.loopNotifications c)
                                               (catch ReadPendingException _ nil)))))]
    consumer))
