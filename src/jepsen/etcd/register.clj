(ns jepsen.etcd.register
  "Tests for single registers, using knossos for linearizability checking."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.etcd [client :as c]]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]])
  (:import (knossos.model Model)))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/client test node)))

  (setup! [this test])

  (invoke! [_ test op]
    (let [[k [version value]] (:value op)]
      (c/with-errors op #{:read}
        (case (:f op)
          :read (let [r (c/get conn k test)
                      v [(:version r) (:value r)]]
                  (assoc op :type :ok, :value (independent/tuple k v)))

          :write (let [r        (c/put! conn k value)
                       version  (-> r :prev-kv val :version inc)]
                   (assoc op
                          :type :ok
                          :value (independent/tuple k [version value])))

          :cas (let [[old new]  value
                     r          (c/cas*! conn k old new)
                     version    (some-> r :puts first :prev-kv val :version
                                        inc)]
                 (if (:succeeded? r)
                   (assoc op
                          :type  :ok
                          :value (independent/tuple k [version value]))
                   (assoc op :type :fail, :error :did-not-succeed)))))))

  (teardown! [this test])

  (close! [_ test]
    (c/close! conn)))

; A versioned register takes operation :values which are [version value] pairs,
; where version is a monotonically advancing value which increments with every
; update. Version reflects the version *resulting* from an update, or, for
; reads, the version read.
(defrecord VersionedRegister [version value]
  Object
  (toString [this] (str "v" version ": " value))

  Model
  (step [model op]
    (let [[op-version op-value] (:value op)
          version' (inc version)]
      (condp = (:f op)
        :write (if (and (not (nil? op-version))
                        (not= version' op-version))
                 (model/inconsistent
                   (str "can't go from version " version " to " op-version))
                 (VersionedRegister. version' op-value))

        :cas   (let [[v v'] op-value]
                 (cond (and (not (nil? op-version))
                            (not= version' op-version))
                       (model/inconsistent
                         (str "can't go from version " version " to "
                              op-version))

                       (not= value v)
                       (model/inconsistent (str "can't CAS " value " from " v
                                                " to " v'))

                       true
                       (VersionedRegister. version' v')))

        :read (cond (and (not (nil? op-version))
                         (not= version op-version))
                    (model/inconsistent
                      (str "can't read version " op-version " from version "
                           version))

                    (and (not (nil? op-value))
                         (not= value op-value))
                    (model/inconsistent
                      (str "can't read " op-value " from register " value))

                    true
                    model)))))

(defn r   [_ _] {:type :invoke, :f :read, :value [nil nil]})
(defn w   [_ _] {:type :invoke, :f :write, :value [nil (rand-int 5)]})
(defn cas [_ _] {:type :invoke, :f :cas, :value [nil [(rand-int 5) (rand-int 5)]]})

(defn workload
  "Tests linearizable reads, writes, and compare-and-set operations on
  independent keys."
  [opts]
  (let [n (count (:nodes opts))]
    {:client    (Client. nil)
     :checker   (independent/checker
                  (checker/compose
                    {:linear   (checker/linearizable
                                 {:model (->VersionedRegister 0 nil)})
                     :timeline (timeline/html)}))
     :generator (independent/concurrent-generator
                  (* 2 n)
                  (range)
                  (fn [k]
                    (->> (gen/mix [w cas])
                         (gen/reserve n r)
                         (gen/limit (:ops-per-key opts)))))}))
