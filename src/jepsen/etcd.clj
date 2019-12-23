(ns jepsen.etcd
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [core :as jepsen]
                    [control :as c]
                    [generator :as gen]
                    [independent :as independent]
                    [store :as store]
                    [tests :as tests]
                    [util :as util :refer [parse-long map-vals]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.etcd [append :as append]
                         [db :as db]
                         [client :as ec]
                         [lock :as lock]
                         [nemesis :as nemesis]
                         [register :as register]
                         [set :as set]
                         [support :as s]
                         [watch :as watch]]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]))

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {:append         append/workload
   :lock           lock/workload
   :lock-set       lock/set-workload
   :lock-etcd-set  lock/etcd-set-workload
   :none           (fn [_] tests/noop-test)
   :set            set/workload
   :register       register/workload
   :watch          watch/workload})

(def all-workloads
  "A collection of workloads we run by default."
  (remove #{:none} (keys workloads)))

(def workloads-expected-to-pass
  "A collection of workload names which we expect should actually pass."
  (remove #{:lock :lock-set} all-workloads))

(def all-nemeses
  "Combinations of nemeses for tests"
  [[]
   [:pause :kill :partition :clock :member]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :clock :member]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map. Special options:

      :quorum       Whether to use quorum reads
      :rate         Approximate number of requests per second, per thread
      :ops-per-key  Maximum number of operations allowed on any given key.
      :workload     Name of the workload to run."
  [opts]
  (info "Test opts\n" (with-out-str (pprint opts)))
  (let [serializable  (boolean (:serializable opts))
        workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (db/db)
        nemesis       (nemesis/nemesis-package
                        {:db        db
                         :nodes     (:nodes opts)
                         :faults    (:nemesis opts)
                         :partition {:targets [:primaries :majority :majorities-ring]}
                         :pause     {:targets [:primaries :all]}
                         :kill      {:targets [:primaries :all]}
                         :interval  5})]
    (merge tests/noop-test
           opts
           {:name       (str "etcd " (name workload-name)
                             " " (str/join "," (map name (:nemesis opts)))
                            (when serializable " serializable"))
            :serializable serializable
            :initialized? (atom false)
            :members    (atom (into (sorted-set) (:nodes opts)))
            :os         debian/os
            :db         (db/db)
            :nemesis    (:nemesis nemesis)
            :checker    (checker/compose
                          {:perf        (checker/perf {:nemeses (:perf nemesis)})
                           :clock       (checker/clock-plot)
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
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]
   ["-s" "--serializable" "Use serializable reads, instead of going through consensus."]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  200
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]
   [nil "--only-workloads-expected-to-pass" "Don't run tests which we know fail."
    :default false]
   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :partition :clock :member})
               "Faults must be pause, kill, partition, clock, or member, or the special faults all or none."]]])

(defn all-test-options
  "Takes base cli options, a collection of nemeses, workloads, and a test count,
  and constructs a sequence of test options."
  [cli nemeses workloads]
  (for [n nemeses, w workloads, i (range (:test-count cli))]
    (assoc cli
           :nemesis   n
           :workload  w)))

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [test-fn cli]
  (let [nemeses   (if-let [n (:nemesis cli)] [n]  all-nemeses)
        workloads (if-let [w (:workload cli)] [w]
                    (if (:only-workloads-expected-to-pass cli)
                      workloads-expected-to-pass
                      all-workloads))]
    (->> (all-test-options cli nemeses workloads)
         (map test-fn))))

(defn run-tests!
  "Runs a sequence of tests and returns a map of outcomes (e.g. true, :unknown,
  :crashed, false) to collections of test folders with that outcome."
  [tests]
  (->> tests
       (map-indexed
         (fn [i test]
           (try
             (let [test' (jepsen/run! test)]
               [(:valid? (:results test'))
                (.getPath (store/path test'))])
             (catch Exception e
               (warn e "Test crashed")
               [:crashed (:name test)]))))
       (group-by first)
       (map-vals (partial map second))))

(defn print-summary!
  "Prints a summary of test outcomes. Takes a map of statuses (e.g. :crashed,
  true, false, :unknown), to test files. Returns results."
  [results]
  (println "\n")

  (when (seq (results true))
    (println "\n# Successful tests\n")
    (dorun (map println (results true))))

  (when (seq (results :unknown))
    (println "\n# Indeterminate tests\n")
    (dorun (map println (results :unknown))))

  (when (seq (results :crashed))
    (println "\n# Crashed tests\n")
    (dorun (map println (results :crashed))))

  (when (seq (results false))
    (println "\n# Failed tests\n")
    (dorun (map println (results false))))

  (println)
  (println (count (results true)) "successes")
  (println (count (results :unknown)) "unknown")
  (println (count (results :crashed)) "crashed")
  (println (count (results false)) "failures")

  results)

(defn exit!
  "Takes a map of statuses and exits with an appropriate error code: 3 if any
  crashed, 2 if any were invaliud, 1 if any were unknown, 0 if all passed."
  [results]
  (System/exit (cond
                 (:crashed results)   3
                 (:unknown results)   2
                 (get results false)  1
                 true                 0)))

(defn test-all-cmd
  "A command that runs a whole suite of tests in one go."
  [{:keys [test-fn] :as opts}]
  {"test-all"
   {:opt-spec (concat cli/test-opt-spec cli-opts)
    :opt-fn   cli/test-opt-fn
    :usage    "Runs all tests"
    :run      (fn run [{:keys [options]}]
                (info "CLI options:\n" (with-out-str (pprint options)))
                (->> options
                     (all-tests test-fn)
                     run-tests!
                     print-summary!
                     exit!))}})

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  etcd-test
                                         :opt-spec cli-opts})
                   (test-all-cmd {:test-fn  etcd-test
                                  :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
