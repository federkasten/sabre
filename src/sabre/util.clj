(ns sabre.util
  (:import [java.util UUID]))

(defn task-key
  [request-key]
  {:pre [(not (nil? request-key))]}
  (str (name request-key)
       "-"
       (.toString (UUID/randomUUID))))
