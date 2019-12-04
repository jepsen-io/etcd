(ns jepsen.etcd.append
  "Tests append/read transactions over lists. In order to provide append
  transactions, we need to read the current states, then perform a second
  transaction to perform all writes (and reads)."
  (:refer-clojure :exclude [read])
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util :refer [map-vals]]]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as append]
            [jepsen.etcd [client :as c]
                         [support :as s]]
            [jepsen.etcd.client.txn :as t]
            [slingshot.slingshot :refer [try+]]))

(defn preprocess
  "In this phase, we take an connection and an operation, extract its
  transaction, returning a map with keys:

    :client
    :op
    :txn"
  [client op]
  {:client  client
   :op      op
   :txn     (:value op)})

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
    :read-revision  is the revision we read at, used for absent keys"
  [t]
  (let [res (->> (written-keys t)
                 (map t/get)
                 (c/txn! (:client t)))
        reads (->> (:results res)
                   (map :kvs)
                   (reduce merge))]
        ;_   (info :reads res)]
    (assoc t :reads reads, :read-revision (:revision (:header res)))))

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

(defn appends->writes
  "We take a transaction with reads, and iterate through all its :append
  operations, converting each to a :w write. To do this, we use the value
  initially read, and conj on the appended element. Returns :txn rewritten in
  terms of reads and writes, rather than reads and appends."
  [t]
  (->> (:txn t)
       (reduce (fn [[state txn] [f k v :as mop]]
                 (case f
                   :r       [state (conj txn mop)]
                   :append  (let [v' (conj (get state k []) v)]
                              [(assoc state k v')
                               (conj txn [:w k v'])])))
               [(map-vals :value (:reads t)) []])
       second))

(defn apply!
  "Takes a transaction map and applies reads and writes, rewriting :txn to
  include the results of reads."
  [t]
  (let [reads   (:reads t)
        guards  (guards t)
        ; _       (info :writes (appends->writes t))
        ; Construct final transaction
        txn     (->> (appends->writes t)
                   (map (fn [[f k v]]
                          (case f
                            :r (t/get k)
                            :w (t/put k v)))))
        res     (c/txn! (:client t) guards txn)
        ;_ (info :res res)
        ; Rewrite txn with read results
        txn'    (mapv (fn [[f k v :as mop] r]
                        (case f
                          :append mop
                          :r      [f k (-> r :kvs (get k) :value)]))
                      (:txn t)
                      (:results res))]
    (if (:succeeded? res)
      (assoc (:op t) :type :ok, :value txn')
      (assoc (:op t) :type :fail))))

(defrecord TxnClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/client node)))

  (setup! [_ test])

  (invoke! [_ test op]
    (c/with-errors op #{}
      (-> (preprocess conn op) read apply!)))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  (assoc (append/test {:key-count         3
                       :max-txn-length    4
                       :anomalies         [:G0 :G1 :G2]
                       :additional-graphs [cycle/realtime-graph]})
         :client (TxnClient. nil)))
