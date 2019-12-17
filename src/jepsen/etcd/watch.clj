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
            [slingshot.slingshot :refer [throw+ try+]]))

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
    (delay (.close w)
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

(defrecord Client [conn k max-revision revision]
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
        (loop [rev @revision
               log []]
          (if (<= @max-revision rev)
            ; We're caught up
            (assoc op :type :ok, :value {:revision rev, :log log})

            ; More to go!
            (let [[rev log]
                  (try+ (c/remap-errors
                          (let [_ (info "at rev" rev " catching up to "
                                        @max-revision)
                                w (watch-for conn (:process op) k rev
                                             (rand-int 5000))]
                            ; We don't need to advance this state any more if
                            ; we're only called once, but it feels polite to
                            ; do so.
                            (reset! revision (:revision w))
                            ; We don't update the global revision counter--I
                            ; think we'd race if it changed
                            [(:revision w) (into log (:log w))]))
                        (catch c/client-error? e
                          (warn e "caught during final-watch; retrying")
                          (Thread/sleep 1000)
                          [rev log]))]
              (recur rev log)))))))

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
  {:client    (Client. nil "w" (atom 0) nil)
   :checker   (checker)
   :generator (let [write (->> (range)
                               (map (fn [x] {:type :invoke, :f :write, :value x}))
                               gen/seq)
                    watch {:type :invoke, :f :watch}]
                (gen/reserve (count (:nodes opts)) write
                             watch))
   :final-generator (gen/phases (gen/reserve (count (:nodes opts)) nil
                                 (gen/each
                                   (gen/once {:type :invoke
                                              :f    :final-watch}))))})
