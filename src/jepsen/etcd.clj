(ns jepsen.etcd
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [generator :as gen]
                    [independent :as independent]
                    [tests :as tests]
                    [util :as util :refer [parse-long]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.etcd [db :as db]
                         [client :as ec]
                         [lock :as lock]
                         [nemesis :as nemesis]
                         [register :as register]
                         [set :as set]
                         [support :as s]]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]))

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {"lock"           lock/workload
   "lock-set"       lock/set-workload
   "lock-etcd-set"  lock/etcd-set-workload
   "set"            set/workload
   "register"       register/workload})

(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map. Special options:

      :quorum       Whether to use quorum reads
      :rate         Approximate number of requests per second, per thread
      :ops-per-key  Maximum number of operations allowed on any given key.
      :workload     Name of the workload to run."
  [opts]
  (let [serializable  (boolean (:serializable opts))
        workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (db/db)
        nemesis       (nemesis/nemesis-package
                        {:db        db
                         :faults    [:pause]
                         :partition {:targets [:primaries]}
                         :pause     {:targets [:primaries]}
                         :interval  5})]
    (merge tests/noop-test
           opts
           {:name       (str "etcd " workload-name " s=" serializable)
            :serializable serializable
            :os         debian/os
            :db         (db/db)
            :nemesis    (:nemesis nemesis)
            :checker    (checker/compose
                          {:perf        (checker/perf {:nemeses (:perf nemesis)})
                           :stats       (checker/stats)
                           :exceptions  (checker/unhandled-exceptions)
                           :workload    (:checker workload)})
            :client    (:client workload)
            :generator (gen/phases
                         (->> (:generator workload)
                              (gen/stagger (/ (:rate opts)))
                              (gen/nemesis (:generator nemesis))
                              (gen/time-limit (:time-limit opts)))
                         (gen/log "Healing cluster")
                         (gen/nemesis (:final-generator nemesis))
                         (gen/log "Waiting for recovery")
                         (gen/sleep 10)
                         (gen/clients (:final-generator workload)))})))

(def cli-opts
  "Additional command line options."
  [["-v" "--version STRING" "What version of etcd should we install?"
    :default "3.4.3"]
   ["-w" "--workload NAME" "What workload should we run?"
    :missing  (str "--workload " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   ["-s" "--serializable" "Use serializable reads, instead of going through consensus."]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  200
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  etcd-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
