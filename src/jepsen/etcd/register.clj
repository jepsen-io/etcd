(ns jepsen.etcd.register
  "Tests for single registers."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.etcd [client :as ec]]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (ec/client node)))

  (setup! [this test])

  (invoke! [_ test op]
    (let [[k v] (:value op)]
      (ec/with-errors op #{:read}
        (case (:f op)
          :read (let [value (-> conn (ec/get k) :value)]
                  (assoc op :type :ok, :value (independent/tuple k value)))

          :write (do (ec/put! conn k v)
                     (assoc op :type :ok))

          :cas (let [[old new] v]
                 (assoc op :type (if (ec/cas! conn k old new)
                                   :ok
                                   :fail)))))))

  (teardown! [this test])

  (close! [_ test]
    (ec/close! conn)))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn workload
  "Tests linearizable reads, writes, and compare-and-set operations on
  independent keys."
  [opts]
  {:client    (Client. nil)
   :checker   (independent/checker
                (checker/compose
                  {:linear   (checker/linearizable
                               {:model (model/cas-register)})
                   :timeline (timeline/html)}))
   :generator (independent/concurrent-generator
                10
                (range)
                (fn [k]
                  (->> (gen/mix [r w cas])
                       (gen/limit (:ops-per-key opts)))))})
