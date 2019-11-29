(ns jepsen.etcd.nemesis
  "Nemeses for etcd"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [nemesis :as n]
                    [generator :as gen]
                    [net :as net]
                    [util :as util]]
            [jepsen.nemesis.time :as nt]
            [jepsen.nemesis.combined :as nc]
            [jepsen.etcd.db :as db]))

(defn member-nemesis
  "A nemesis for adding and removing nodes from the cluster."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assoc op :value
             (case (:f op)
               :grow     (db/grow! test)
               :shrink   (db/shrink! test))))

    (teardown! [this test])

    n/Reflection
    (fs [_] [:grow :shrink])))

(defn member-generator
  "A generator for membership operations."
  [opts]
  (->> (gen/mix [{:type :info, :f :grow}
                 {:type :info, :f :shrink}])
       (gen/delay (:interval opts))))

(defn member-package
  "A combined nemesis package for adding and removing nodes."
  [opts]
  {:nemesis   (member-nemesis opts)
   :generator (member-generator opts)
   :perf      #{{:name  "grow"
                 :fs    [:grow]
                 :color "#E9A0E6"}
                {:name  "shrink"
                 :fs    [:shrink]
                 :color "#ACA0E9"}}})

(defn nemesis-package
  "Constructs a nemesis and generators for etcd."
  [opts]
  (let [faults (set (:faults opts))]
    (nc/compose-packages
      (cond-> []
        (some faults [:kill :pause :partition]) (conj (nc/nemesis-package opts))
        (some faults [:member])                 (conj (member-package opts))))))
