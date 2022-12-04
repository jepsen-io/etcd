(ns jepsen.etcd.wr
  "Tests transactional writes and reads to registers using Elle."
  (:refer-clojure :exclude [read])
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [store :as store]
                    [util :as util :refer [map-vals pprint-str]]]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.wr :as wr]
            [jepsen.etcd [client :as c]
                         [support :as s]]
            [jepsen.etcd.client [etcdctl :as etcdctl]
                                [txn :as t]]
            [slingshot.slingshot :refer [try+]]))

(defn etcd-txn
  "We take an Elle transaction sequence like [[:w k v]] and convert it to a
  series of etcd txn AST ops like [[:put k v]]."
  [t]
  (mapv (fn [[f k v]]
          (case f
            :r (t/get k)
            :w (t/put k v)))
        t))

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
      (let [txn (:value op)
            res (c/txn! conn (etcd-txn txn))
            ; Stitch reads back in to elle txn
            txn' (mapv (fn [[f k v :as mop] r]
                         (case f
                           :w mop
                           :r [f k (-> r :kvs (get k) :value)]))
                       txn
                       (:results res))
            op' (if (:debug test)
                  (assoc op :debug
                         {:txn-res (dissoc res :gets :puts)})
                  op)
            op' (if (:succeeded? res)
                  (assoc op' :type :ok, :value txn')
                  (assoc op' :type :fail, :error :didn't-succeed))]
        op')))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  (assoc (wr/test {:key-count         3
                   :max-txn-length    4
                   :consistency-models [:strict-serializable]
                   ; Expensive
                   ;:linearizable-keys? true
                   :wfr-keys         true})
         :client (TxnClient. nil)))
