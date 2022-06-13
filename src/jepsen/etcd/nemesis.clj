(ns jepsen.etcd.nemesis
  "Nemeses for etcd"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [nemesis :as n]
                    [generator :as gen]
                    [net :as net]
                    [util :as util]]
            [jepsen.control [util :as cu]]
            [jepsen.nemesis [combined :as nc]
                            [time :as nt]]
            [jepsen.etcd [client :as client]
                         [db :as db]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn member-nemesis
  "A nemesis for adding and removing nodes from the cluster."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assoc op :value
             (try+
               (case (:f op)
                 :grow     (db/grow! test)
                 :shrink   (db/shrink! test))
               (catch [:type :jepsen.etcd.db/blank-member-name] e
                 :blank-member-name)
               (catch [:type :unhealthy-cluster] e
                 :unhealthy-cluster))))

    (teardown! [this test])

    n/Reflection
    (fs [_] #{:grow :shrink})))

(defn member-generator
  "A generator for membership operations."
  [opts]
  (->> (gen/mix [(repeat {:type :info, :f :grow})
                 (repeat {:type :info, :f :shrink})])
       (gen/stagger (:interval opts))))

(defn member-final-generator
  "Until the cluster is full, emit grow events."
  [test context]
  (when (seq (db/addable-nodes test))
    {:type :info, :f :grow}))

(defn member-package
  "A combined nemesis package for adding and removing nodes."
  [opts]
  (when ((:faults opts) :member)
    {:nemesis   (member-nemesis opts)
     :generator (member-generator opts)
     :final-generator (->> member-final-generator
                           (gen/delay 1)
                           ; It's possible for the cluster to get stuck in a
                           ; way that can't be grown--for instance, with
                           ; permanently blank member names.
                           (gen/time-limit 60))
     :perf      #{{:name  "grow"
                   :fs    [:grow]
                   :color "#E9A0E6"}
                  {:name  "shrink"
                   :fs    [:shrink]
                   :color "#ACA0E9"}}}))

(defrecord AdminNemesis [clients]
  n/Nemesis
  (setup! [this test]
    (assoc this :clients
           (->> (:nodes test)
                (map (fn [node]
                       [node (client/client node)]))
                (into {}))))

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      :compact
      (try+ (client/remap-errors
              (let [r (client/compact! (rand-nth (vals clients)))]
                (assoc op :value r)))
            (catch client/client-error? e
              (assoc op :value :compact-failed, :error e)))

      :defrag
      (->> (c/on-nodes test value
                       (fn [_ _]
                         (info "Defragmenting")
                         (try+
                           (db/etcdctl! :defrag)
                           :defragged
                           (catch [:exit 1] e
                             (condp re-find (:err e)
                               #"deadline exceeded" :deadline-exceeded
                               (:err e))))))
           (assoc op :value))))

  (teardown! [this test]
    (->> (vals clients)
         (mapv client/close!)))

  n/Reflection
  (fs [_] #{:compact :defrag}))

(defn admin-generator
  "Generates periodic compact/defrag ops."
  [opts]
  (->> (gen/mix [(fn [test ctx]
                   (let [nodes (if (< 0.5 (rand))
                                 (:nodes test)
                                 (util/random-nonempty-subset (:nodes test)))]
                     {:type :info, :f :defrag, :value nodes}))
                 (repeat {:type :info, :f :compact})])
       (gen/stagger (:interval opts))))

(defn admin-final-generator
  "During recovery, compact and defrag."
  []
  [{:type :info, :f :compact}
   {:type :info, :f :defrag}])

(defn admin-package
  "A combined nemesis package for administrative operations."
  [opts]
  (when (contains? (:faults opts) :admin)
    {:nemesis         (AdminNemesis. nil)
     :generator       (admin-generator opts)
     :final-generator (admin-final-generator)
     :perf            #{{:name  "compact"
                         :fs    #{:compact}
                         :start #{}
                         :stop  #{}
                         :color "#2021CC"}
                        {:name  "defrag"
                         :fs    #{:defrag}
                         :start #{}
                         :stop  #{}
                         :color "#BE20CC"}}}))

(defn rand-data-file
  "Picks a random etcd data file on the given node. Type can be either :wal or
  :snap."
  [test node type]
  (let [data  (db/data-dir node)
        dir  (case type
               :wal  (str data "/member/wal")
               :snap (str data "/member/snap"))]
    (get (c/on-nodes test [node]
                     (fn [_ _]
                       (rand-nth (cu/ls-full dir))))
         node)))

(defn corrupt-generator
  "Generator of file bitflip/truncation operations. We restrict these to a
  minority of nodes to avoid breaking the whole cluster."
  [{:keys [faults]}]
  (let [bitflip-wal?  (contains? faults :bitflip-wal)
        bitflip-snap? (contains? faults :bitflip-snap)
        truncate-wal? (contains? faults :truncate-wal)
        ; Possible types of faults and types of files to mess with
        fault+types   (cond-> []
                        bitflip-wal?  (conj [:bitflip :wal])
                        bitflip-snap? (conj [:bitflip :snap])
                        truncate-wal? (conj [:truncate :wal]))]
    (when (seq fault+types)
      (fn gen [test context]
        (let [nodes        (:nodes test)
              n            (count nodes)
              targets      (take (dec (util/majority n)) nodes)
              node         (rand-nth targets)
              [fault type] (rand-nth fault+types)
              file         (rand-data-file test node type)]
          {:type  :info
           :f     (keyword (str (name fault) "-" (name type)))
           :value {node (cond-> {:file file}
                          (= fault :truncate) (assoc :drop (rand-int 1024))
                          (= fault :bitflip)  (assoc :probability
                                                     (rand-nth [1e-3 1e-4 1e-5])))}})))))

(defn corrupt-package
  "A nemesis package for datafile corruption"
  [opts]
  {:nemesis   (n/compose
                {{:bitflip-wal :bitflip
                  :bitflip-snap :bitflip}  (n/bitflip)
                 {:truncate-wal :truncate} (n/truncate-file)})
   :generator (->> (corrupt-generator opts)
                   (gen/stagger (:interval opts)))
   :perf      #{{:name "corrupt"
                 :fs   #{:bitflip-wal :bitflip-snap
                         :truncate-wal :truncate-snap}
                 :color "#99F2E2"}}})

(defn nemesis-package
  "Constructs a nemesis and generators for etcd."
  [opts]
  (let [opts (update opts :faults set)]
    (-> (nc/nemesis-packages opts)
        (concat [(member-package opts)
                 (corrupt-package opts)
                 (admin-package opts)])
        (->> (remove nil?))
        nc/compose-packages)))
