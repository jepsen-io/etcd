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
                    [util :as util :refer [meh map-vals]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.etcd [client :as c]]
            [slingshot.slingshot :refer [throw+ try+]])
  (:import (java.util.concurrent BrokenBarrierException)
           (java.util.concurrent.locks LockSupport)))

(defn converger
  "Generates a convergence context for n threads, where values are converged
  when (converged? values) returns true."
  [n converged?]
  (atom {; The convergence function
         :converged? converged?
         ; What threads are involved?
         :threads []
         ; And what values did they most recently come to?
         :values (vec (repeat n ::init))}))

(defn await-converger-change
  "Wait for a converger's state to change. Might return for no reason."
  [c]
  (LockSupport/park c))

(defn signal-converger-change!
  "Let a converger know that its state has changed."
  [c]
  (doseq [t (:threads @c)]
    (LockSupport/unpark t)))

(defn stable?
  "A converger state is stable when no value is initial or evolving."
  [c]
  (not-any? #{::init ::evolving} (:values c)))

(defn divergent?
  "A converger state is divergent when any values are initial, or some
  non-evolving values don't pass the convergence test."
  [c]
  (or (some #{::init} (:values c))
      (let [vs (remove #{::evolving} (:values c))]
        (and (seq vs)
             (not ((:converged? c) vs))))))

(defn converged?
  "A converger state is converged when it is stable and non-divergent."
  [c]
  (and (stable? c)
       (not (divergent? c))))

(defn evolve!
  "Takes a converger, an evolve function, an initial value, and a thread index
  i. Takes the previous value for this thread (or the initial value), clears it
  from the converger, and calls (evolve value) to generate value', finally
  updating the converger with value'."
  [converger evolve init i]
  (let [value (atom nil)]
    ; Get the current value and clear it from the converger
    (swap! converger (fn [c]
                       (let [v (nth (:values c) i)
                             v (if (= ::init v) init v)]
                         (reset! value v))
                       (assoc-in c [:values i] ::evolving)))

    ; Evolve
    (try
      (let [value' (evolve @value)]
        (swap! converger #(assoc-in % [:values i] value')))
      (catch Throwable t
        ; If we throw here, all bets are off. We can't safely return, since we
        ; haven't converged. We set :crashed? to true in the converger, and let
        ; all threads wake up.
        (swap! converger assoc :crashed? true))
      (finally
        ; Let other threads know we've done some work and they can wake up
        (signal-converger-change! converger)))
      nil))

(defn converge!
  "Takes a converger, an initial value, and a function which evolves values
  over time.

  When `converge` is called, evaluates `evolve` repeatedly, starting with the
  initial value; each successive invocation receives the result of the previous
  invocation. Returns a value only when (converged? [v1 v2 ...]) returns
  truthy.

  Always invokes evolve at least once; the initial state doesn't count as
  converged.

  If evolve throws, horrible things will happen. Probably deadlock or livelock.
  I should fix this later."
  [converger init evolve]
  ; Acquire our unique thread index
  (let [i (-> (swap! converger update :threads conj (Thread/currentThread))
              :threads
              count
              dec)]
    (loop []
      (let [c @converger]
        (cond ; If we've crashed, throw a BrokenBarrierException. We're not
              ; really using a cyclic barrier, but this is *basically* a
              ; concurrency barrier, and it *is* broken.
              (:crashed? c)
              (throw (BrokenBarrierException. "Convergence failed"))

              ; If we're converged, return our value
              (converged? c)
              (-> converger deref :values (get i))

              ; If we're divergent, evolve
              (divergent? c)
              (do (evolve! converger evolve init i)
                  (recur))

              ; Otherwise, wait for something to change
              true
              (do (await-converger-change converger)
                  (recur)))))))

(defn watch
  "Watches key k from revision on, returning a deref-able which, when
  dereferenced, stops watching and returns results."
  [conn process k revision]
  ; State is a map of {:revision x, :log y}.
  (let [state   (atom {:revision revision
                       :events   []
                       :log      []})
        error   (promise)
        results (promise)
        ; Revision is inclusive, so we want to start just after we left off
        ; Definitely don't pass 0, or you'll get wherever it is currently
        w (c/watch conn k (inc revision)
                   ; With every update, we append to the log and advance our
                   ; revision in state.
                   (fn update [event]
                     ;(info "process" process "got" (pr-str event))
                     (let [events (->> (:events event)
                                       (map (comp :value val :kv)))
                           rev' (->> (:events event)
                                     (map (comp :mod-revision val :kv))
                                     (reduce max))]
                       (swap! state (fn advance [{:keys [revision log] :as s}]
                                      ; This should never happen, but when we
                                      ; used the response header's revision
                                      ; instead of taking the max of
                                      ; mod-revisions, it sometimes did!
                                      (when-not (< revision rev')
                                        (throw+ {:type      :nonmonotonic-watch
                                                 :definite? true
                                                 :description
                                                 (str "got event with revision "
                                                      rev'
                                                      " but we last saw "
                                                      revision ":\n"
                                                      (pr-str event))
                                                 :revision revision
                                                 :revision' rev'
                                                 :event     event}))
                                      {:revision rev'
                                       :events   (conj (:events s) event)
                                       :log      (into log events)}))
                       ;(info "process" process "observed up to" rev')
                       ))

                   ; Log errors and save them to return later.
                   (fn errors [ex]
                     ;(warn ex process "error during watch")
                     (deliver error ex))

                   ; Once the watch tells us we're all done, we package up our
                   ; error, or state, and hand it off to the results promise.
                   (fn complete []
                     (if (realized? error)
                       (do (info process "error watching from" revision "to"
                                 (:revision @state))
                           (deliver results @error))
                       (do (info process "completed up to revision"
                                 (:revision @state))
                           (deliver results @state)))))]
    ; When deref'ed, we kill the watch and wait for results, which are
    ; delivered on completion.
    (delay (meh (.close w)) ; this can throw??? why!?
           (let [r @results]
             (if (instance? Throwable r)
               (throw r)
               r)))))

(defn watch-for
  "Watches key `k` from `revision` for `ms` milliseconds."
  [conn process k revision ms]
  (let [w (watch conn process k revision)]
    (Thread/sleep ms)
    @w))

(defrecord Client [conn k max-revision revision converger]
  client/Client
  (open! [this test node]
    (assoc this
           :conn (c/client node)
           :revision (atom 0)))

  (setup! [this test])

  (invoke! [_ test op]
    ; It's important that watch and final watch always return definite errors.
    ; If they don't, we'd spin up a fresh client with a new revision, and see
    ; duplicate elements in the log for that thread.
    (c/with-errors op #{:watch :final-watch}
      (case (:f op)
        :write (let [res (c/put! conn k (:value op))]
                 (->> res :header
                      :revision
                      (swap! max-revision max))
                 (assoc op :type :ok))

        :watch
        (let [res (watch-for conn (:process op) k @revision (rand-int 5000))]
          ; Advance our revision counter for the next watch
          (reset! revision (:revision res))
          ; And the global revision counter
          (swap! max-revision max (:revision res))
          (assoc op :type :ok, :value res))

        :final-watch
        (let [v (converge!
                  converger
                  {:revision @revision, :log []}
                  (fn [v]
                    (let [rev (:revision v)
                          log (:log v)]
                      (try+ (c/remap-errors
                              (let [_ (info "at rev" rev "catching up to"
                                            @max-revision)
                                    w (watch-for conn (:process op) k rev
                                                 (rand-int 5000))]
                                ; Advance our revisions
                                (reset! revision (:revision w))
                                (swap! max-revision max (:revision w))
                                (assoc w :log (into (:log v) (:log w)))))
                            (catch c/client-error? e
                              (warn e "caught during final-watch; retrying")
                              (Thread/sleep 1000)
                              v)))))]
          (assoc op :type :ok, :value v)))))

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
         (filter (comp #{:watch :final-watch} :f))
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

(defn nonmonotonic-errors
  "Takes a history and extracts any nonmonotonic error messages."
  [history]
  (->> history
       (keep :error)
       (filter (comp #{:nonmonotonic-watch} first))
       (map second)))

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
            nm-errors (nonmonotonic-errors history)
            valid? (cond (seq nm-errors)                  false
                         (not (apply = (vals revisions))) :unknown
                         (seq deltas)                     false
                         :else                            true)]
        (cond-> {:valid?    valid?
                 :revisions revisions}
          (not valid?) (assoc :logs       logs
                              :canonical  canonical
                              :deltas     deltas
                              :nonmonotonic-errors nm-errors))))))

(defn workload
  [opts]
  {:client    (Client. nil "w" (atom 0) nil
                       (converger (count (:nodes opts))
                                  (fn [ms] (apply = (map :revision ms)))))
   :checker   (checker)
   :generator (let [write (->> (range)
                               (map (fn [x] {:type :invoke, :f :write, :value x})))
                    watch (repeat {:type :invoke, :f :watch})]
                (gen/reserve (count (:nodes opts)) write
                             watch))
   :final-generator (gen/phases (gen/reserve (count (:nodes opts)) nil
                                 (gen/each-thread
                                   {:type :invoke
                                    :f    :final-watch})))})
