(ns jepsen.etcd.watch
  "Tests for watches. We have processes watch a changing key for a while, and
  return a list of the versions it went through. At the end of the test, we
  confirm that everyone observed all the writes in the correct order."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [clj-diff.core :as diff]
            [knossos.op :as op]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [util :as util :refer [map-vals]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.etcd [client :as c]]
            [slingshot.slingshot :refer [try+]]))

(defn watch!
  "Watches key k, streaming updates into atom vector results, and updating
  revision with each new event."
  [conn k results revision]
  ; Revision is inclusive, so we want to start just after we left off
  ; Definitely don't pass 0, or you'll get wherever it is currently
  (c/watch conn k (inc @revision)
           (fn [event]
             (->> (:events event)
                  ; (map (juxt :type (comp :value val :kv))) ; [:put 2]
                  (map (comp :value val :kv))
                  (swap! results into))
             (reset! revision (:revision (:header event)))
             (info "observed up to" @revision))))

(defrecord Client [conn k revision]
  client/Client
  (open! [this test node]
    (assoc this
           :conn (c/client node)
           :revision (atom 0)))

  (setup! [this test])

  (invoke! [_ test op]
    (c/with-errors op #{:watch}
      (case (:f op)
        :write (do (c/put! conn k (:value op))
                   (assoc op :type :ok))

        :watch (let [results (atom [])]
                 (with-open [w (watch! conn k results revision)]
                   (Thread/sleep (rand-int 5000)))
                 (assoc op :type :ok, :value {:revision @revision
                                              :log      @results})))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn per-thread-watches
  "Takes a test and a history, and computes a map of threads to ok watch
  operations."
  [test history]
  (let [concurrency (:concurrency test)]
    (->> history
         (filter op/ok?)
         (filter (comp #{:watch} :f))
         (group-by (fn [op] (mod (:process op) concurrency))))))

(defn per-thread-logs
  "Takes a test and a history, and computes a map of threads to the sequence of
  values they observed."
  [test history]
  (->> (per-thread-watches test history)
       (map-vals (fn [ops]
                   (->> ops
                        (mapcat (comp :log :value)))))))

(defn per-thread-revisions
  "Takes a test and a history, and computes a map of threads to the highest
  revision they observed."
  [test history]
  (->> (per-thread-watches test history)
       (map-vals (fn [ops]
                   (->> ops
                        (map (comp :revision :value))
                        (reduce max 0))))))

(defn mode
  "Returns the most common x in coll, if one exists."
  [coll]
  (let [[x freq] (first (reverse (sort-by val (frequencies coll))))]
    (when (< 1 freq) x)))

(defn longest
  "Returns the longest x in coll."
  [coll]
  (util/max-by count coll))

(defn canonical-log
  "Given a collection of logs, chooses one as the basis of comparison. We pick
  the mode log, or any log if none is more common."
  [logs]
  (or (mode logs) (longest logs)))

(defn checker
  "We reconstruct the order of values as seen by each individual process, then
  compare them for equality."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [logs (per-thread-logs test history)
            revisions (per-thread-revisions test history)
            _         (info :logs logs)
            canonical (canonical-log (vals logs))
            deltas (->> logs
                        (keep (fn [[thread log]]
                                (let [diff (diff/diff canonical log)
                                      ed   (diff/edit-distance canonical log)]
                                  (when-not (zero? ed)
                                    {:thread        thread
                                     :edit-distance ed
                                     :diff          diff}))))
                        (sort-by (comp - :edit-distance)))
            valid? (cond (not (apply = (vals revisions))) :unknown
                         (seq deltas)                     false
                         :else                            true)]
        (cond-> {:valid? valid?
                 :revisions revisions}
          (not valid?) (assoc :logs       logs
                              :canonical  canonical
                              :deltas     deltas))))))

(defn workload
  [opts]
  {:client    (Client. nil "w" nil)
   :checker   (checker)
   :generator (let [write (->> (range)
                               (map (fn [x] {:type :invoke, :f :write, :value x}))
                               gen/seq)
                    watch {:type :invoke, :f :watch}]
                (gen/reserve (count (:nodes opts)) write
                             watch))
   :final-generator (gen/phases (gen/sleep 30)
                                (gen/reserve (count (:nodes opts)) nil
                                 (gen/each
                                   (gen/once {:type :invoke, :f :watch}))))})
