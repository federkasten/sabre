(ns sabre.core
  (:require [clojure.tools.logging :as logging]
            [lackd.core :as lackd]
            [sabre.status :as status]
            [sabre.util :refer [task-key]]))

(defn dispatch!
  [server {:keys [request args] :as task}]
  (when server
    (let [db (:db server)
          queue (:queue server)
          args (:args task)
          key (task-key (:request task))]
      (lackd/push-item! queue key)
      (lackd/put-entry! db key task)
      key)))

(defn halt!
  [server key]
  (when server
    (let [db (:db server)
          abort-queue (:abort-queue server)]
      (lackd/push-item! abort-queue key)
      key)))

(defn start-server!
  ([name handlers]
   (start-server! name handlers {}))
  ([name handlers {:keys [on-finished
                          num-workers
                          data-dir
                          sleep]
                   :or {on-finished (fn [args {:keys [key request meta]}])
                        num-workers 1
                        data-dir "/tmp/server"
                        sleep 1000}
                   :as params}]
   (let [env (lackd/open-env! data-dir)
         db (lackd/open-db! env name)
         queue (lackd/open-queue! env (str name "-queue"))
         abort-queue (lackd/open-queue! env (str name "-abort-queue"))
         available? (atom true)
         workers (atom {})
         running-set-key (str "running-" name)]

     ;; Initialize
     (lackd/put-entry! db running-set-key #{})

     ;; Start server main loop
     (future
       (while @available?

         (try
           (let [running (lackd/get-entry! db running-set-key)
                 finished-workers (filter #(future-done? (:future %))
                                          (vals @workers))
                 finished-keys (set (map :key finished-workers))
                 rest-running (filter #(not (contains? finished-keys %)) running)
                 num-starts (- num-workers (count rest-running))]

             ;; When finished
             (doseq [w finished-workers]
               (on-finished (get-in w [:params :args])
                            {:key (:key w)
                             :meta (get-in w [:params :meta])
                             :request (get-in w [:params :request])})
               (logging/info (str "Finished task " (:key w)))
               (lackd/update-entry! db running-set-key #(disj % (:key w)))
               (swap! workers dissoc (:key w)))

             ;; Start new task
             (loop [n num-starts]
               (when (pos? n)
                 (when-let [k (lackd/pop-item! queue)]
                   (let [t (lackd/get-entry! db k)
                         task-fn (get handlers (:request t))]
                     (logging/info (str "Start task " k " : " (:request t) " " (:args t)))
                     (lackd/update-entry! db running-set-key #(conj % k))
                     (swap! workers
                            assoc
                            k
                            {:future (future
                                       (try (task-fn (:args t) {:key k
                                                                :meta (:meta t)})
                                            (catch InterruptedException e
                                              (logging/info (str "Interrupted task " k " : " (:request t) " " (:args t))))
                                            (catch Exception e (do (.printStackTrace e)
                                                                   nil))))
                             :key k
                             :params {:request (:request t)
                                      :args (:args t)
                                      :meta (:meta t)}})
                     (recur (dec n))))))

             ;; Cancel workerks
             (loop [key (lackd/pop-item! abort-queue)]
               (when key
                 (when-let [f (get-in @workers [key :future])]
                   (future-cancel f))
                 (recur (lackd/pop-item! abort-queue)))))

           (catch Exception e (do (.printStackTrace e)
                                  nil)))

         (Thread/sleep sleep)))

     ;; Return server instance
     {:name name
      :env env
      :db db
      :queue queue
      :abort-queue abort-queue
      :running-set-key running-set-key
      :available? available?
      :handlers handlers
      :params params})))

(defn stop-server!
  [server]
  (reset! (:available? server) false)
  ;; Swap and close all
  (let [running (lackd/get-entry! (:db server) (:running-set-key server))]
    (doseq [k running]
      (lackd/insert-item! (:queue server) k)))
  (lackd/close-db! (:db server))
  (lackd/close-queue! (:queue server))
  (lackd/close-queue! (:abort-queue server))
  (lackd/close-env! (:env server))
  nil)
