(ns jepsen.etcd.lock
  "Tests for locks!"
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen [core :as jc]
                    [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]
                    [util :as util :refer [meh relative-time-nanos]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.etcd [client :as c]]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]])
  (:import (knossos.model Model)))

(defn acquire!
  "Lock aquisition in etcd requires acquiring and preserving a lease, and
  maintaining the lease out-of-band with the use of the lock; e.g. via a
  separate keep-alive thread. This function obtains a lease, spawns a thread to
  keep it alive, and acquires the given named lock using that lease, in one
  fell swoop. Returns a map of:

  {:lease-id
   :listener
   :lock-key
   :process}"
  [conn lock-name process]
  (let [lease-id (-> conn (c/grant-lease! 10) :id)
        listener (c/keep-lease-alive! conn lease-id)]
    (info :lease-id lease-id :listener listener)
    (try
      (let [lock (c/acquire-lock! conn lock-name lease-id)]
        (info :lock lock)
        {:lease-id lease-id
         :listener listener
         :lock-key (:key lock)
         :process process})
      (catch Throwable t
        ; We don't want to leak listeners if lock acquisition fails.
        (c/close! listener)
        ; If we timed out, our lock request might still be outstanding, and
        ; could complete later, which would then result in us holding the lock
        ; (and denying others from making progress) until the lease naturally
        ; expires. I *think* this might be responsible for a phenomenon where
        ; locks go permanently (?) unavailable because every lock request times
        ; out. To avoid this problem, we *also* issue a lease release message.
        ; Of course, this could fail too, but if it goes through, it might
        ; improve availability.
        (meh (c/revoke-lease! conn lease-id))
        (throw t)))))

(defn release!
  "Takes a map from acquire! and releases the lock and lease."
  [conn lease+lock]
  (info :closing lease+lock)
  (c/close! (:listener lease+lock))
  (c/release-lock! conn (:lock-key lease+lock))
  (c/revoke-lease! conn (:lease-id lease+lock)))

(defmacro with-errors
  "Takes an operation and a body. Evaluates body, catching exceptions (using
  client/with-errors), and returning appropriate :info/:fail ops. Lock release
  with known errors *always* succeeds."
  [op & body]
  ; We flag both operation types here as idempotent because in any case where
  ; we don't successfully acquire or release the lock, we *know* we don't
  ; hold the lock, and can refrain from "using" it to do tasks. The only case
  ; where the lock would be held is if we get definite confirmation, and
  ; that's when we return :ok.
  `(let [res# (c/with-errors ~op #{:acquire :release}
                ~@body)]
     (if (and (= :release (:f ~op))
              (= :fail (:type res#))
              ; Except we DO want to fail when we already release the lock,
              ; otherwise we'll double-release.
              (not= :not-held (:error res#)))
       ; Known failures we convert to :oks, because we still know the lock is
       ; no longer held. Our critical section is over no matter what.
       (assoc res# :type :ok)
       res#)))

; Conn is our connection to etcd.
; Lock-name is the string name of the lock.
; Lease+lock is an atom containing our lease and lock data.
(defrecord LinearizableLockClient [conn lock-name lease+lock]
  client/Client
  (open! [this test node]
    (assoc this
           :conn (c/client node)
           :lease+lock (atom nil)))

  (setup! [this test])

  (invoke! [_ test op]
    (with-errors op
      (case (:f op)
        :acquire (if @lease+lock
                   (assoc op :type :fail, :error :already-held)
                   (do (reset! lease+lock
                               (acquire! conn lock-name (:process op)))
                       (assoc op :type :ok)))

        :release (if-let [ll @lease+lock]
                   (try (release! conn ll)
                        (assoc op :type :ok)
                        (finally
                          ; Our release might have failed, but we're not
                          ; renewing the lease any more, so we won't try again.
                          (reset! lease+lock nil)))
                   (assoc op :type :fail, :error :not-held)))))

  (teardown! [this test])

  (close! [_ test]
    (when-let [ll @lease+lock]
      ; Since this process is terminating, it won't hold the lock any more.
      (jc/conj-op! test {:type     :invoke
                         :process  (:process ll)
                         :f        :release
                         :time     (relative-time-nanos)})
      (c/close! (:listener ll))
      (jc/conj-op! test {:type    :ok
                         :process (:process ll)
                         :f       :release
                         :time    (relative-time-nanos)}))
    (c/close! conn)))

(defn acquires
  []
  {:type :invoke, :f :acquire})

(defn releases
  []
  {:type :invoke, :f :release})

(defn workload
  "Tests linearizable reads, writes, and compare-and-set operations on
  independent keys."
  [opts]
  {:client    (map->LinearizableLockClient
                {:conn            nil
                 :lock-name       "foo"
                 :lease+lock      nil})
   :checker   (checker/compose
                {:linear   (checker/linearizable {:model (model/mutex)})
                 :timeline (timeline/html)})
   :generator (gen/mix [(acquires) (releases)])})
