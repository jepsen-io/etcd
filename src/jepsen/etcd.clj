(ns jepsen.etcd
  (:gen-class)
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [dom-top.core :refer [loopr]]
            [jepsen [antithesis :as antithesis]
                    [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [core :as jepsen]
                    [control :as c]
                    [generator :as gen]
                    [history :as h]
                    [independent :as independent]
                    [nemesis]
                    [store :as store]
                    [tests :as tests]
                    [util :as util :refer [map-vals]]]
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
                         [watch :as watch]
                         [wr :as wr]]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]))

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {:append         append/workload
   ; The lock workload used some Weird Stateful Hacks that don't work in Jepsen
   ; 0.2.x.
   ; :lock           lock/workload
   :lock-set       lock/set-workload
   :lock-etcd-set  lock/etcd-set-workload
   :none           (fn [_] tests/noop-test)
   :set            set/workload
   :register       register/workload
   :watch          watch/workload
   :wr             wr/workload})

(def all-workloads
  "A collection of workloads we run by default."
  (remove #{:none} (keys workloads)))

(def workloads-expected-to-pass
  "A collection of workload names which we expect should actually pass."
  (remove #{:lock :lock-set} all-workloads))

(def nemeses
  "All nemeses"
  #{:admin :bitflip-wal :bitflip-snap :truncate-wal :pause :kill :partition
    :clock :member})

(def all-nemeses
  "Combinations of nemeses for tests"
  [[:admin]
   [:pause     :admin]
   [:kill      :admin]
   [:partition :admin]
   [:member    :admin]
   ; Truncates are fairly boring because the WAL is zeroed out to a fixed size
   ; at creation time.
   ;[:truncate-wal :admin]
   ;[:truncate-wal :kill]
   [:bitflip-wal :bitflip-snap :admin]
   [:bitflip-wal :bitflip-snap :kill]
   [:admin :bitflip-snap :bitflip-wal :pause :kill :partition :clock :member]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none             []
   :corrupt          [:bitflip-wal :bitflip-snap :truncate-wal]
   :all              [:admin :pause :kill :bitflip-wal :bitflip-snap
                      :truncate-wal :partition :clock :member]})

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

      :rate         Approximate number of requests per second
      :ops-per-key  Maximum number of operations allowed on any given key.
      :workload     Name of the workload to run."
  [opts]
  (s/check-thread-leaks)
  (let [serializable? (boolean (:serializable? opts))
        workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (db/db opts)
        nemesis       (nemesis/nemesis-package
                        {:db        db
                         :nodes     (:nodes opts)
                         :faults    (:nemesis opts)
                         :partition {:targets [:primaries :majority :majorities-ring]}
                         :pause     {:targets [:primaries :all]}
                         :kill      {:targets [:primaries :all]}
                         :interval  (:nemesis-interval opts)})]
    (info :antithesis? (:antithesis? opts))
    (-> tests/noop-test
        (merge opts
          {:name       (str "etcd " (:version opts)
                            " " (name workload-name)
                            " " (name (:client-type opts))
                            " " (str/join "," (map name (:nemesis opts)))
                            (when (< (or (:retry-max-attempts opts) 2) 1)
                              " no-retry")
                            (when (:lazyfs opts) " lazyfs")
                            (when serializable? " serializable"))
           :pure-generators true
           :serializable? serializable?
           :initialized? (atom false)
           :members    (atom (into (sorted-set) (:nodes opts)))
           :os         debian/os
           :db         db
           :nemesis    (:nemesis nemesis)
           :checker
           (checker/compose
             {:perf        (checker/perf {:nemeses (:perf nemesis)})
              :clock       (checker/clock-plot)
              :stats       (checker/stats)
              :exceptions  (checker/unhandled-exceptions)
              :crash       (checker/log-file-pattern
                             ; Ignore matches like "couldn't find local name
                             ; "n1" in initial cluster; we get these when we
                             ; restart nodes that don't belong in the group
                             ; due to membership changes.
                             #"(\"level\":\"fatal|panic\"(?!.*couldn't find local name))|(panic:)|(^signal SIG)"
                             "etcd.log")
              :workload    (:checker workload)})
           :client    (:client workload)
           :generator (gen/phases
                        (->> (:generator workload)
                             (gen/stagger (/ (:rate opts)))
                             (gen/nemesis
                               (gen/phases
                                 (gen/sleep 5)
                                 (:generator nemesis)))
                             (gen/time-limit (:time-limit opts)))
                        (gen/log "Healing cluster")
                        (gen/nemesis (:final-generator nemesis))
                        (gen/log "Waiting for recovery")
                        (gen/sleep 10)
                        (gen/clients (:final-generator workload)))})
        (cond-> (:antithesis? opts) (assoc :nemesis jepsen.nemesis/noop))
        antithesis/test)))

(def cli-opts
  "Additional command line options."
  [["-a" "--antithesis" "If set, runs in Antithesis mode. Disables the OS and DB, and SSH, expecting these to be provided by the Antithesis environment."
    :id :antithesis?]

   [nil "--client-type TYPE" "What kind of client should we use? Either jetcd or etcdctl. Etcdctl is an experiment and is definitely buggy--in particular, it has a habit of getting stuck while running commands and accidentally leaking operations into the *next* test run."
    :default :jetcd
    :parse-fn keyword
    :validate [#{:etcdctl :jetcd} (cli/one-of #{:etcdctl :jetcd})]]

    [nil "--corrupt-check" "If set, enables etcd's experimental corruption checking options"]

    [nil "--debug" "If set, enables additional (somewhat expensive) debug logging; for instance, txn-list-append will includethe intermediate transactions it executes as a part of each operation."]

   [nil "--lazyfs" "Mounts etcd in a lazyfs, and causes the kill nemesis to also wipe our unfsynced data files."]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (fn [nem]
                                 (or (nemeses nem)
                                     (special-nemeses nem))))
               (cli/one-of (concat nemeses (keys special-nemeses)))]]

   [nil "--nemesis-interval SECONDS" "How long between nemesis operations for each class of fault"
    :default  5
    :parse-fn read-string
    :validate [pos? "Must be positive"]]

   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  200
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--only-workloads-expected-to-pass" "Don't run tests which we know fail."
    :default false]

   ["-r" "--rate HZ" "Approximate number of requests per second"
    :default  200
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]

   [nil "--retry-max-attempts COUNT" "Number of retry attempts we allow."
    :default nil
    :parse-fn parse-long]

   ["-s" "--serializable" "Use serializable reads, instead of going through consensus."
    :id :serializable?]

   [nil "--snapshot-count COUNT" "Number of committed transactions to trigger a snapshot to disk. Passed to etcd."
    :default 100
    :parse-fn parse-long
    :validate [(complement neg?) "Must not be negative."]]

   [nil "--tcpdump" "If set, tracks client traffic using tcpdump."]

   [nil "--unsafe-no-fsync" "Asks etcd not to fsync."]

   ["-v" "--version STRING" "What version of etcd should we install?"
    :default "3.5.15"]

   ])

(def test-cli-opts
  "CLI options just for test"
   [["-w" "--workload NAME" "What workload should we run?"
    :default :append
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]
   ])

(def test-all-cli-opts
  "CLI options just for test-all"
   [["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]
   ])

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

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (antithesis/with-rng
    (cli/run! (merge (cli/single-test-cmd {:test-fn  etcd-test
                                           :opt-spec (into cli-opts
                                                           test-cli-opts)})
                     (cli/test-all-cmd {:tests-fn (partial all-tests etcd-test)
                                        :opt-spec (into cli-opts
                                                        test-all-cli-opts)})
                     (cli/serve-cmd))
              args)))

;; Repl stuff

;; List-append investigation! This is to help track down an issue with
;; list-append tests using the etcdctl client accidentally leaking state
;; between test runs

(defn txn-dirs
  "Extracts a set of test directories from every transaction's reads in a
  history"
  [history]
  (let [xf (comp (keep :debug)
                 (map :read-res)
                 (mapcat :results)
                 (map :kvs)
                 (mapcat vals)
                 (map :value)
                 (map :dir))]
    (into #{} xf history)))

(defn all-txns-dirs
  "Maps test start times to their directories."
  []
  (loopr [m (sorted-map)]
         [test (next (reverse (store/all-tests)))]
         (let [test @test
               _    (prn :checking (:start-time test))
               dirs (txn-dirs (:history test))]
           (prn dirs)
           (if (seq dirs)
             (recur (assoc m (:start-time test) dirs))
             m))))

(defn ops-involving
  "Filters a history to just those ops involving the given key."
  [k history]
  (->> history
       (filter (fn [op]
                 (and (= :txn (:f op))
                      (->> op
                           :value
                           (map second)
                           (some #{k})))))))

(defn wr-op-revisions
  "Takes a wr txn op and returns a sequence of maps from it like:

    {:index         the op index
     :key           a key
     :value         the value of that key
     :mod-revision  the mod revision of that key}

  Helpful for finding duplicate mod revisions."
  [op]
  (->> op
       :debug
       :txn-res
       :results
       (keep (fn [r]
               (if-let [[k p] (:prev-kv r)]
                 ; write
                 (when k
                   {:type :w
                    :index (:index op)
                    :key k
                    :value (:value (:value p))
                    :mod-revision (:mod-revision p)})
                 ; read
                 (when-let [[k p] (first (:kvs r))]
                   {:type :r
                    :index (:index op)
                    :key k
                    :value (:value (:value p))
                    :mod-revision (:mod-revision p)}))))))

(defn wr-ops-revisions
  "All revision maps from multile wr ops."
  [ops]
  (mapcat wr-op-revisions ops))

(defn duplicate-revisions
  "Takes operations and returns a map of [key value] to revision maps where
  more than one revision exists for that key and value."
  [ops]
  (->> (wr-ops-revisions ops)
       (group-by (juxt :key :value))
       (filter (fn [[_ vs]]
                 (< 1 (->> vs (map :mod-revision) set count))))
       (into (sorted-map))))
