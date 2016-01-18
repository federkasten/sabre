(ns sabre.status
  (:require [clojure.set :refer [map-invert]]))

(def ^:const status-keyword-mapping
  {0 :waiting
   5 :running
   10 :finished
   90 :timeout
   99 :error})

(def ^:const keyword-status-mapping
  (map-invert status-keyword-mapping))

(def status->status-keyword #(get status-keyword-mapping %))
(def status-keyword->status #(get keyword-status-mapping %))
