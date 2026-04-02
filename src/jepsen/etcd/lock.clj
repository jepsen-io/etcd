(ns jepsen.etcd.lock
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.etcd [client :as c]]
            [slingshot.slingshot :refer [try+]]))

(def lease-ttl-seconds 2)
(def critical-section-delay-ms (* 2 1000))
(def lock-name "lock-set")

(defn read-op
  []
  {:type :invoke, :f :read, :value nil})

(defn add-op
  [value]
  {:type :invoke, :f :add, :value value})

(defn cleanup-step!
  [label f]
  (try+
    (f)
    (catch Object e
      (warn "lock-set cleanup failed during" label (pr-str e)))))

(defn invoke-add!
  [conn shared-state op]
  (let [lease-id  (volatile! nil)
        keepalive (volatile! nil)
        lock-key  (volatile! nil)
        result    (try+
                    (let [lease (c/grant-lease! conn lease-ttl-seconds)
                          _     (vreset! lease-id (:id lease))
                          ka    (c/keep-lease-alive! conn (:id lease))
                          _     (vreset! keepalive ka)
                          lock  (c/acquire-lock! conn lock-name (:id lease))
                          _     (vreset! lock-key (:key lock))
                          seen  @shared-state]
                      ; Model a stale-read read-modify-write against external
                      ; shared state while the etcd lease may expire.
                      (Thread/sleep (rand-int critical-section-delay-ms))
                      (reset! shared-state (conj seen (:value op)))
                      (assoc op :type :ok))
                    (catch c/client-error? e
                      (assoc op :type :fail :error [(:type e) (:description e)]))
                    (catch InterruptedException e
                      (.interrupt (Thread/currentThread))
                      (assoc op :type :fail :error [:interrupted (.getMessage e)]))
                    (catch Throwable t
                      (assoc op :type :fail
                                :error [:unexpected-exception
                                        (.getName (class t))
                                        (.getMessage t)])))]
    (cleanup-step! :release-lock
                   #(when-let [key @lock-key]
                      (c/release-lock! conn key)))
    (cleanup-step! :close-keepalive
                   #(when-let [stream @keepalive]
                      (c/close! stream)))
    (cleanup-step! :revoke-lease
                   #(when-let [id @lease-id]
                      (c/revoke-lease! conn id)))
    result))

(defrecord LockSetClient [conn shared-state]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/client test node)))

  (setup! [_ _test]
    (reset! shared-state #{}))

  (invoke! [_ _test op]
    (case (:f op)
      :read (assoc op :type :ok :value @shared-state)
      :add  (invoke-add! conn shared-state op)))

  (teardown! [_ _test])

  (close! [_ _test]
    (c/close! conn)))

(defn set-workload
  [_opts]
  (let [shared-state (atom #{})]
    {:client    (LockSetClient. nil shared-state)
     :checker   (checker/compose
                  {:set      (checker/set-full {:linearizable? true})
                   :timeline (timeline/html)})
     :generator (gen/mix [(repeat (read-op))
                          (map add-op (range))])}))
