(ns jepsen.etcd.specs
  "Practical runtime specs for the etcd Jepsen test constructors.

  This is not static type checking. Instead, it gives us runtime validation and
  instrumentation for key workload/test entrypoints, plus a small validation
  runner we can execute from the CLI."
  (:require [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [jepsen [checker :as checker]
                    [client :as client]]
            [jepsen.generator :as gen]
            [jepsen.db :as db]
            [jepsen.etcd :as etcd]
            [jepsen.etcd.append :as append]
            [jepsen.etcd.lock :as lock]
            [jepsen.etcd.register :as register]
            [jepsen.etcd.set :as set]
            [jepsen.etcd.watch :as watch]
            [jepsen.etcd.wr :as wr]))

(defn client-instance?
  [x]
  (satisfies? client/Client x))

(defn checker-instance?
  [x]
  (satisfies? checker/Checker x))

(defn generator-instance?
  [x]
  (satisfies? gen/Generator x))

(defn db-instance?
  [x]
  (satisfies? db/DB x))

(s/def ::node string?)
(s/def ::nodes (s/and vector? (s/coll-of ::node :kind vector? :min-count 1)))
(s/def ::version string?)
(s/def ::workload-name (set (keys etcd/workloads)))
(s/def ::client-type #{:jetcd :etcdctl})
(s/def ::nemesis-fault etcd/nemeses)
(s/def ::nemesis (s/and vector?
                        (s/coll-of ::nemesis-fault :kind vector?)))
(s/def ::nemesis-interval pos?)
(s/def ::rate pos?)
(s/def ::time-limit pos?)
(s/def ::ops-per-key pos-int?)
(s/def ::concurrency pos-int?)
(s/def ::serializable? boolean?)
(s/def ::lazyfs boolean?)
(s/def ::debug boolean?)
(s/def ::history-only? boolean?)

(s/def ::opts
  (s/keys :req-un [::workload-name
                   ::nodes
                   ::version
                   ::client-type
                   ::nemesis
                   ::nemesis-interval
                   ::rate
                   ::time-limit
                   ::ops-per-key
                   ::concurrency]
          :opt-un [::serializable?
                   ::lazyfs
                   ::debug
                   ::history-only?]))

(s/def ::client client-instance?)
(s/def ::checker checker-instance?)
(s/def ::generator generator-instance?)
(s/def ::final-generator generator-instance?)
(s/def ::workload-map
  (s/keys :req-un [::client ::checker ::generator]
          :opt-un [::final-generator]))

(defn lock-set-op?
  [op]
  (and (map? op)
       (= :invoke (:type op))
       (contains? op :process)
       (contains? op :index)
       (contains? op :time)
       (integer? (:process op))
       (integer? (:index op))
       (integer? (:time op))
       (case (:f op)
         :read (nil? (:value op))
         :add  (and (integer? (:value op))
                    (not (neg? (:value op))))
         false)))

(s/def ::lock-set-op lock-set-op?)

(s/def ::db db-instance?)
(s/def ::name string?)
(s/def ::test-map
  (s/keys :req-un [::name
                   ::client
                   ::checker
                   ::generator
                   ::db
                   ::nodes]))

(s/fdef etcd/parse-nemesis-spec
  :args (s/cat :spec string?)
  :ret ::nemesis)

(s/fdef append/workload
  :args (s/cat :opts ::opts)
  :ret ::workload-map)

(s/fdef set/workload
  :args (s/cat :opts ::opts)
  :ret ::workload-map)

(s/fdef lock/set-workload
  :args (s/cat :opts ::opts)
  :ret ::workload-map)

(s/fdef register/workload
  :args (s/cat :opts ::opts)
  :ret ::workload-map)

(s/fdef watch/workload
  :args (s/cat :opts ::opts)
  :ret ::workload-map)

(s/fdef wr/workload
  :args (s/cat :opts ::opts)
  :ret ::workload-map)

(s/fdef etcd/etcd-test
  :args (s/cat :opts ::opts)
  :ret ::test-map)

(def sample-opts
  {:workload :lock-set
   :nodes ["n1" "n2" "n3" "n4" "n5"]
   :version "3.5.15"
   :client-type :jetcd
   :nemesis [:pause]
   :nemesis-interval 5
   :rate 200
   :time-limit 30
   :ops-per-key 10
   :concurrency 5
   :serializable? false
   :lazyfs false
   :debug false
   :history-only? false})

(def workload-constructors
  [[:append append/workload]
   [:lock-set lock/set-workload]
   [:set set/workload]
   [:register register/workload]
   [:watch watch/workload]
   [:wr wr/workload]])

(def instrumented-vars
  [#'etcd/parse-nemesis-spec
   #'append/workload
   #'lock/set-workload
   #'set/workload
   #'register/workload
   #'watch/workload
   #'wr/workload
   #'etcd/etcd-test])

(defn explain-or-throw!
  [spec value label]
  (when-not (s/valid? spec value)
    (println "Spec failure for" label)
    (pprint (s/explain-data spec value))
    (throw (ex-info (str "Spec failure for " label)
                    {:label label :spec spec}))))

(defn validate-workloads!
  []
  (doseq [[workload-name workload-fn] workload-constructors]
    (let [opts (assoc sample-opts :workload workload-name)
          workload (workload-fn opts)]
      (explain-or-throw! ::workload-map workload
                         (str workload-name " workload map"))
      (println "OK workload" workload-name))))

(defn sample-generator-ops!
  [generator test n]
  (loop [generator generator
         ctx       (gen/context test)
         ops       []]
    (if (= n (count ops))
      ops
      (let [step (gen/op generator test ctx)
            [op generator' ctx']
            (cond
              (and (vector? step) (= 2 (count step)))
              [(first step) (second step) ctx]

              (and (vector? step) (= 3 (count step)))
              step

              :else
              (throw (ex-info "Unexpected generator step shape"
                              {:step step})))]
        (cond
          (nil? op)
          (throw (ex-info "Generator ended unexpectedly while sampling"
                          {:sample-size n
                           :ops-sampled (count ops)}))

          (= :pending op)
          (throw (ex-info "Generator returned :pending while sampling"
                          {:sample-size n
                           :ops-sampled (count ops)}))

          :else
          (recur generator' ctx' (conj ops op)))))))

(defn validate-lock-set-generator!
  []
  (let [test     (assoc sample-opts :pure-generators true)
        workload (lock/set-workload sample-opts)
        ops      (sample-generator-ops! (:generator workload) test 40)
        fs       (set (map :f ops))]
    (doseq [op ops]
      (explain-or-throw! ::lock-set-op op "lock-set sampled op"))
    (when-not (= #{:read :add} fs)
      (throw (ex-info "Expected sampled lock-set ops to include both :read and :add"
                      {:f-values fs
                       :ops ops})))
    (println "OK lock-set generator ops")))

(defn validate-etcd-test!
  []
  (let [test (etcd/etcd-test sample-opts)]
    (explain-or-throw! ::test-map test "etcd-test map")
    (println "OK etcd-test map")))

(defn instrument!
  []
  (stest/instrument instrumented-vars))

(defn run-spec-validation!
  []
  (println "Instrumenting etcd constructor specs...")
  (instrument!)
  (println "Validating workload constructors...")
  (validate-workloads!)
  (println "Validating lock-set generator output...")
  (validate-lock-set-generator!)
  (println "Validating etcd-test entrypoint...")
  (validate-etcd-test!)
  (println)
  (println "Spec validation passed.")
  (println)
  (println "Notes:")
  (println "- This is runtime spec validation/instrumentation, not static type checking.")
  (println "- It checks args/returns for the key etcd workload constructors and etcd-test."))

(defn -main
  [& _args]
  (run-spec-validation!))
