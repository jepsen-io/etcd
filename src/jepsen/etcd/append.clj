(ns jepsen.etcd.append
  "Tests append/read transactions over lists. In order to provide append
  transactions, we need to read the current states, then perform a second
  transaction to perform all writes (and reads)."
  (:refer-clojure :exclude [read])
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [store :as store]
                    [util :as util :refer [map-vals pprint-str]]]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as append]
            [jepsen.etcd [client :as c]
                         [support :as s]]
            [jepsen.etcd.client [etcdctl :as etcdctl]
                                [txn :as t]]
            [slingshot.slingshot :refer [try+]]))

(defn preprocess
  "In this phase, we take a test map, a connection, and an operation, and
  extract its transaction, returning a transaction map with keys:

    :test
    :client
    :op
    :txn
    :read-only?  For read-only transactions, we can skip the write phase."
  [test client op]
  {:test       test
   :client     client
   :op         op
   :txn        (:value op)
   :read-only? (every? (comp #{:r} first) (:value op))})

(defn encode-put
  "Takes a transaction map and a value to write, and transforms the value
  somehow. This is identity normally, but when we put something to etcd in
  debug mode we also include extra information about the txn that generated
  that value."
  [t v]
  (if (:debug (:test t))
    {:time (str (util/local-time))
     :dir  (.getName (store/path (:test t)))
     :txn (:txn t),
     :value v}
    v))

(defn decode-get
  "Takes a transaction map and a value read from etc, and transforms the value
  back into what the Jepsen test expects--stripping off debugging information,
  for instance."
  [t v]
  (if (:debug (:test t))
    (:value v)
    v))

(defn written-keys
  "Takes a transaction and returns the set of keys being written."
  [t]
  (->> (:txn t)
       (filter (comp #{:append} first)) ; Take only appends
       (map second)                     ; [f k v] -> k
       set))

(defn read
  "In the read phase, we take a txn map and perform a transaction which reads
  all the keys we need to write, adding two new keys to our txn map:

    :reads          is a map of keys to {:value x, :version y, ...}
    :read-revision  is the revision we read at, used for absent keys
    :read-res       Contains the raw results of the read txn, used for
                    debugging"
  [t]
  (let [res (->> (written-keys t)
                 (map t/get)
                 (c/txn! (:client t)))
        reads (->> (:results res)
                   (map :kvs)
                   (reduce merge))]
        ;_   (info :reads res)]
    (assoc t
           :reads         reads
           :read-res      res
           :read-revision (:revision (:header res)))))

(defn guards
  "Takes a transaction with reads, and constructs a collection of guards
  verifying each read's revision is intact."
  [t]
  (map (fn [k]
         (if-let [v (get (:reads t) k)]
           ; If the key existed, we go by its modification revision
           (t/= k (t/mod-revision (:mod-revision v)))
           ; If the key is missing, use the read revision to ensure that the
           ; key hasn't been modified more recently. The semantics here don't
           ; seem well-defined, but I think it passes for absent keys.
           (t/< k (t/mod-revision (:read-revision t)))))
       (written-keys t)))

(defn write-txn
  "We take a transaction with reads, and iterate through all its :append
  operations, converting each to an etcd txn AST op like [:put k v]. To do
  this, we use the value initially read, and conj on the appended element.
  Returns a sequence of etd gets and puts, rather than reads and appends."
  [t]
  (->> (:txn t)
       ; `state` maps keys to currently expected values; we play forward
       ; simulated effects of multiple appends in a single txn, turning them
       ; into multiple puts. This lets us catch intermediate read or other txn
       ; anomalies involving multiple writes.
       (reduce (fn [[state txn] [f k v :as mop]]
                 (case f
                   :r       [state (conj txn (t/get k))]
                   :append  (let [v' (conj (get state k []) v)]
                              [(assoc state k v')
                               (conj txn (t/put k (encode-put t v')))])))
               [(->> t :reads
                     (map-vals (comp (partial decode-get t) :value)))
                []])
       second))

(defn apply-rw!
  "Takes a transaction map and applies reads and writes, rewriting :txn to
  include the results of reads. Returns a completed operation, including extra
  debug information under a :debug key:

    :reads          The read map
    :read-revision  The revision we read at
    :read-res       The result of the read phase
    :write-txn      The write txn we executed, as a vector of
                    [guards true-branch]
    :write-res      The result of the write txn
  "
  [t]
  (let [reads   (:reads t)
        guards  (guards t)
        ; _       (info :writes (appends->writes t))
        ; Construct final etcd transaction
        write-txn (write-txn t)
        res       (c/txn! (:client t) guards write-txn)
        ;_ (info :res res)
        ; Rewrite Jepsen txn with read results
        txn'    (mapv (fn [[f k v :as mop] r]
                        (case f
                          :append mop
                          :r [f k (decode-get t (-> r :kvs (get k) :value))]))
                      (:txn t)
                      (:results res))
        ; Add debugging info
        op (if (:debug (:test t))
             (assoc (:op t) :debug
                    {:reads          reads
                     :read-res       (:read-res t)
                     :read-revision  (:read-revision t)
                     :write-txn      [guards write-txn]
                     :write-txn-res  res})
             (:op t))]
    (if (:succeeded? res)
      (assoc op :type :ok, :value txn')
      (assoc op :type :fail, :error :didn't-succeed))))

(defn apply-ro!
  "Takes a transaction map for a read only transaction, and returns the
  completed operation, with extra information under a :debug key:

    :reads          The read map
    :read-revision  The revision we read at
    :read-res       The result of the read phase"
  [{:keys [txn client op test] :as t}]
  (let [; Read every key
        res (->> txn
                 (map second)
                 distinct
                 (map t/get)
                 (c/txn! client))
        reads (->> (:results res)
                   (map :kvs)
                   (reduce merge))
        ; Rewrite txn
        txn' (mapv (fn [[f k v :as mop]]
                     (case f
                       :r [f k (decode-get t (:value (reads k)))]
                       :append mop))
                   txn)
        op (if (:debug test)
             (assoc op :debug
                    {:reads reads
                     :read-res res
                     :read-revision (:revision (:header res))})
             op)]
    (if (:succeeded? res)
      (assoc op :type :ok, :value txn')
      (assoc op :type :fail, :error :didn't-succeed))))

(defn apply-singleton-ro!
  "Takes a transaction map for a read-only transaction with a single operation,
  and returns the completed operation."
  [{:keys [txn client op test] :as t}]
  (let [[f k _ :as mop] (first txn)
        r               (c/get client k test)
        txn'            [[f k (decode-get t (:value r))]]]
    (assoc op :type :ok, :value txn')))

(defrecord TxnClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/client test node)))

  (setup! [_ test])

  (invoke! [_ test op]
    (when (and (:debug test)
               (satisfies? etcdctl/Log conn))
      (etcdctl/log conn (str "=================================================\nOp: " (pr-str op))))

    (c/with-errors op #{}
      (let [t (preprocess test conn op)]
        (if (:read-only? t)
          (if (= 1 (count (:txn t)))
            (apply-singleton-ro! t)
            (apply-ro! t))
          (-> t read apply-rw!)))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn workload
  "A generator, client, and checker for a list-append test."
  [opts]
  (assoc (append/test {:key-count         3
                       :max-txn-length    4
                       :consistency-models [:strict-serializable]})
         :client (TxnClient. nil)))
