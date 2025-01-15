(ns ottla.consumer
  (:require [ottla.postgresql :as postgres]
            [pg.core :as pg]
            [pg.pool :as pg-pool])
  (:import [org.pg Connection]
           [java.io Closeable]
           [java.util.concurrent
            Executors
            ScheduledExecutorService
            ExecutorService
            ThreadPoolExecutor
            ThreadPoolExecutor$DiscardOldestPolicy
            TimeUnit
            ArrayBlockingQueue]))

(defprotocol IShutdown
  (shutdown [_ await-time-ms])
  (status [_]))


(deftype Consumer [^ExecutorService poller
                   ^ExecutorService worker
                   ^Thread listener
                   ^Connection conn]

  IShutdown
  (shutdown [_ await-time-ms]
    (.shutdownNow poller)
    (.shutdown worker)
    (.interrupt listener)
    (try
      (when-not (.awaitTermination worker await-time-ms TimeUnit/MILLISECONDS)
        (.shutdownNow worker))
      (catch InterruptedException _
        (.shutdownNow worker)
        (.interrupt (Thread/currentThread)))))
  (status [_]
    (cond
      (.isTerminated worker) :terminated
      (.isShutdown worker) :shutdown
      :else :running))

  Closeable
  (close [this]
    (.close conn)
    (shutdown this 0))

  Object
  (toString [this]
    (format "Consumer[%s]" (str (status this)))))

(defn fetch-and-handle
  [{:keys [conn] :as config} selection handler]
  #_(println "DEBUG fetch-and-handle" selection)
  (pg/on-connection [conn conn]
    (let [config (assoc config :conn conn)
          records (postgres/fetch-records! config selection)]
      (when (seq records)
        (handler records)))))

(defn start-consumer
  [{:keys [conn-map] :as config}
   {:keys [topic] :as basic-selection}
   handler
   {:keys [poll-ms]
    :or {poll-ms 15000}
    :as opts}]
  (assert (map? conn-map) "conn-map must be a connection map")
  (assert (string? topic) "topic is required")
  (let [{:keys [conn] :as config} (postgres/connect-config config)
        selection (postgres/normalize-selection basic-selection)
        ^ScheduledExecutorService poller (Executors/newSingleThreadScheduledExecutor)
        ^ExecutorService worker (-> (ThreadPoolExecutor. 1 1 0 TimeUnit/MILLISECONDS
                                                         (ArrayBlockingQueue. 1)
                                                         (ThreadPoolExecutor$DiscardOldestPolicy.))
                                    (Executors/unconfigurableExecutorService))
        do-work-fn (fn* [max] (.execute worker
                                        (fn* []
                                             (fetch-and-handle config (assoc selection :max max) handler))))
        _ (.scheduleAtFixedRate poller (fn* [] (do-work-fn nil)) 0 poll-ms TimeUnit/MILLISECONDS)
        ^Thread listen-thread (doto (Thread.
                                     (fn* []
                                          (pg/with-connection [c (assoc conn-map
                                                                        :fn-notification
                                                                        (fn [{:keys [message]}]
                                                                          (do-work-fn (parse-long message))))]
                                            (pg/listen c topic)
                                            (.loopNotifications c))))
                                (.setDaemon true)
                                (.start))]
    (Consumer. poller worker listen-thread conn)))
