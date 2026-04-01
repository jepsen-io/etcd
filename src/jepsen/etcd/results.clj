(ns jepsen.etcd.results
  (:require [jepsen [core :as jepsen]
                     [store :as store]]))

(defn -main
  [& [store-dir]]
  (when-not store-dir
    (binding [*out* *err*]
      (println "usage: lein run -m jepsen.etcd.results <store-dir>"))
    (System/exit 1))
  (-> store-dir
      store/test
      jepsen/log-results)
  (shutdown-agents))
