(defproject sabre "0.1.0"
  :description "A framework to implement robust multi-threading servers for Clojure"
  :url "https://github.com/federkasten/sabre"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/tools.logging "0.3.1"]
                 [lackd "0.2.0"]]
  :profiles {:dev {:global-vars {*warn-on-reflection* true
                                 *assert* true}
                   :dependencies [[org.clojure/clojure "1.8.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.0"]]}
             :1.6 {:dependencies [[org.clojure/clojure "1.6.0"]]}
             :1.7 {:dependencies [[org.clojure/clojure "1.7.0"]]}
             :1.8 {:dependencies [[org.clojure/clojure "1.8.0"]]}})
