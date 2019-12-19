(ns jepsen.etcd.watch-test
  (:require [clojure.test :refer :all]
            [dom-top.core :as dt]
            [jepsen.etcd.watch :refer :all]
            [clojure.tools.logging :refer [info]]))

(info "hi")

(deftest converge-test
  ; We're going to append random numbers to arrays until their final number is the same
  (let [n 3
        c (converger n (fn [colls] (apply = (map peek colls))))
        vs (dt/real-pmap (fn [i]
                           (converge! c [i] (fn [coll]
                                              (Thread/sleep (rand-int 2))
                                              (conj coll (rand-int 2)))))
                         (range n))]
    (prn :vs vs)
    ; Starts with initial values
    (is (= (range n) (map first vs)))
    ; Ends with same value
    (is (apply = (map peek vs)))))
