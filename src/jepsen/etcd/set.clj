(ns jepsen.etcd.set
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.etcd [client :as c]
                         [support :as s]]
            [slingshot.slingshot :refer [try+]]))

(defrecord SetClient [k conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/client node)))

  (setup! [_ test]
    (c/put! conn k #{}))

  (invoke! [_ test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :read (assoc op
                     :type :ok,
                     :value (:value (c/get conn k {:serializable?
                                                   (:serializable test)})))

        :add (do (c/swap! conn k conj (:value op))
                 (assoc op :type :ok)))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn w
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       gen/seq))

(defn r
  []
  {:type :invoke, :f :read, :value nil})

(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client    (SetClient. "a-set" nil)
   :checker   (checker/set-full {:linearizable? true})
   :generator (gen/reserve 5 (r) (w))})
