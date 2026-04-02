(ns jepsen.etcd.lock
  "An etcd-backed mutex test for Jepsen-local shared state.

  etcd is only the lock service here. The protected collection lives in-process
  inside Jepsen itself, so acknowledged :add operations are about whether that
  local state changed, not whether every etcd cleanup step succeeded."
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen [checker :as checker]
                    [client :as client]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.etcd [client :as c]]
            [slingshot.slingshot :refer [try+]]))

(def lease-ttl-seconds 2)
(def lock-name "lock-set")

(defn read-op
  []
  {:type :invoke, :f :read, :value nil})

(defn add-ops
  []
  (map (fn [x]
         {:type :invoke, :f :add, :value x})
       (range)))

(defn op-stream
  []
  (interleave (add-ops) (repeatedly read-op)))

(defn cleanup-step!
  [f message]
  (try+
    (c/remap-errors (f))
    (catch c/client-error? e
      (warn e message))
    (catch Throwable t
      (warn t message))))

(defn cleanup!
  [conn {:keys [keepalive lock-key lease-id]}]
  (when lock-key
    (cleanup-step! #(c/release-lock! conn lock-key)
                   "Failed releasing lock-set lock"))
  (when keepalive
    (cleanup-step! #(c/close! keepalive)
                   "Failed stopping lock-set keepalive"))
  (when lease-id
    (cleanup-step! #(c/revoke-lease! conn lease-id)
                   "Failed revoking lock-set lease")))

(defrecord SetClient [conn shared]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/client test node)))

  (setup! [_ test]
    (reset! shared #{}))

  (invoke! [_ test op]
    (case (:f op)
      :read (assoc op :type :ok, :value @shared)

      :add (let [cleanup-state (atom {:lease-id  nil
                                      :keepalive nil
                                      :lock-key  nil})]
             (try+
               (c/remap-errors
                 (let [lease     (c/grant-lease! conn lease-ttl-seconds)
                       lease-id  (:id lease)
                       _         (swap! cleanup-state assoc :lease-id lease-id)
                       keepalive (c/keep-lease-alive! conn lease-id)
                       _         (swap! cleanup-state assoc :keepalive keepalive)
                       lock      (c/acquire-lock! conn lock-name lease-id)
                       _         (swap! cleanup-state assoc :lock-key (:key lock))
                       snapshot  @shared]
                   (Thread/sleep (rand-int (* lease-ttl-seconds 1000)))
                   ; Preserve the stale read-modify-write shape the workload is testing.
                   (reset! shared (conj snapshot (:value op)))
                   (assoc op :type :ok)))
               (catch c/client-error? e
                 (assoc op :type :fail, :error [(:type e) (:description e)]))
               (finally
                 (cleanup! conn @cleanup-state)))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn set-workload
  [_opts]
  {:client    (SetClient. nil (atom #{}))
   :checker   (checker/compose
                {:set      (checker/set-full {:linearizable? true})
                 :timeline (timeline/html)})
   :generator (op-stream)})
