(ns sabre.core
  (:require [clojure.tools.logging :as logging]
            [clojure.core.async :as async :refer [go go-loop]]
            [clojure.edn :as edn]
            [lackd.core :as lackd]
            [sabre.status :as status]
            [sabre.util :refer [task-key]]))

(defn dispatch!
  [server {:keys [request args] :as task}]
  (when server
    (let [db (:db server)
          args (:args task)
          key (task-key (:request task))
          queue-key (:current-queue-key server)
          queue (lackd/get-sequence! db queue-key)]
      (lackd/put-entry! db queue-key (conj queue key))
      (lackd/put-entry! db key (pr-str task))
      key)))

(defn halt!
  [server key]
  (when server
    (let [db (:db server)
          queue-key (:abort-queue-key server)
          queue (lackd/get-sequence! db queue-key)]
      (lackd/put-entry! db queue-key (conj queue key))
      key)))

(defn start-server!
  ([name handlers]
   (start-server! name handlers {}))
  ([name handlers {:keys [on-finished
                          num-workers
                          data-dir
                          sleep]
                   :or {on-finished (fn [key request args meta])
                        num-workers 1
                        data-dir "/tmp/server"
                        sleep 1000}
                   :as params}]
   (let [env (lackd/open-env! data-dir)
         db (lackd/open-db! env name)
         running? (atom true)
         workers (atom #{})
         current-queue-key (str "current-" name)
         running-queue-key (str "running-" name)
         abort-queue-key (str "abort-" name)]

     ;; Start server main loop
     (go-loop []
       (when @running?

         (let [current (lackd/get-sequence! db current-queue-key)
               running (lackd/get-sequence! db running-queue-key)
               abort (set (lackd/get-sequence! db abort-queue-key))
               aborting-workers (if (empty? abort)
                                  []
                                  (filter #(contains? abort (:key %)) @workers))
               finished-workers (filter #(future-done? (:future %)) @workers)
               all-keys (set (map :key @workers))
               finished-keys (set (map :key finished-workers))
               rest-running (filter #(and (not (contains? finished-keys %))
                                          (contains? all-keys %)) running)
               num-starts (- num-workers (count rest-running))
               waits (take num-starts current)
               new-running (concat rest-running waits)
               new-current (drop num-starts current)]

           ;; Update workers
           (lackd/put-entry! db running-queue-key new-running)
           (doseq [w finished-workers]
             (on-finished (:key w)
                          (get-in w [:params :request])
                          (get-in w [:params :args])
                          (get-in w [:params :meta]))
             (logging/info (str "Finished task " (:key w)))
             (swap! workers disj w))

           ;; Start new workers
           (when-not (empty? waits)
             (doseq [k waits]
               (let [t (edn/read-string (lackd/get-entry! db k))
                     task-fn (get handlers (:request t))]
                 (logging/info (str "Start task " k " : " (:request t) " " (:args t)))
                 (swap! workers conj {:future (future
                                                (try (apply task-fn (:args t))
                                                     (catch Exception e (do (.printStackTrace e)
                                                                            nil))))
                                      :key k
                                      :params {:request (:request t)
                                               :args (:args t)
                                               :meta (:meta t)}}))))
           ;; Cancel workerks
           (when-not (empty? aborting-workers)
             (doseq [w aborting-workers]
               (future-cancel (:future w))))

           ;; Update queue
           (lackd/put-entry! db current-queue-key new-current))

         (Thread/sleep sleep)
         (recur)))

     ;; Return server instance
     {:name name
      :current-queue-key current-queue-key
      :running-queue-key running-queue-key
      :abort-queue-key abort-queue-key
      :running? running?
      :handlers handlers
      :params params
      :env env
      :db db})))

(defn stop-server!
  [server]
  (reset! (:running? server) false)
  ;; Swap task queue
  (let [db (:db server)
        current-queue-key (:current-queue-key server)
        running-queue-key (:running-queue-key server)
        current (lackd/get-sequence! db current-queue-key)
        running (lackd/get-sequence! db running-queue-key)]
    (lackd/put-entry! db current-queue-key (concat running current))
    (lackd/put-entry! db running-queue-key []))
  (lackd/close-db! (:db server))
  (lackd/close-env! (:env server))
  nil)
