(ns shinri.core-test
  (:require [clojure.test :refer :all]
            [shinri.core :refer :all]
            [loom.alg :as la]))

(letfn [(exec [m fun]
          (fun m))]
  (defn thrush-t
    "The Thrush Combinator
  More info : http://blog.fogus.me/2010/09/28/thrush-in-clojure-redux/"
    [zmap & fns]
    (reduce exec zmap fns)))

(deftest make-graph-test
  (let [zmap {:object-id 1}
        data {:pre/input [(constantly zmap)]
              :pre/populate-object [(fn [zmap]
                                     {:authorize? true})
                                   [:pre/input]]
              :pre/authorize-object [(fn [zmap]
                                      (get-in zmap
                                              [:pre/populate-object
                                               :authorize?]))
                                    [:pre/populate-object]]}
        g (make-graph data)]
    (is (= (keys data)
           (la/topsort g))
        "Building simple graph using hashmap"))

  (let [zmap {:object-id 1}
        data [:pre/input [(constantly zmap)]
              :pre/populate-object (fn [zmap]
                                    {:authorize? true})
              :pre/authorize-object (fn [zmap]
                                     (get-in zmap
                                             [:pre/populate-object
                                              :authorize?]))]
        g (make-graph data)]
    (is (= (map first (partition 2 data))
           (la/topsort g))
        "Building simple graph using vector"))

  (let [zmap {:object-id 1}
        data {:pre/input [(constantly zmap)
                          [:pre/authorize-object]]
              :pre/populate-object [(fn [zmap]
                                     {:authorize? true})
                                   [:pre/input]]
              :pre/authorize-object [(fn [zmap]
                                      (get-in zmap
                                              [:pre/populate-object
                                               :authorize?]))
                                    [:pre/populate-object]]}]
    (is (thrown? java.lang.Exception
                 (make-graph data))
        "Building cyclic graph should fail")))


(deftest execute-graph-test
  (let [zmap {:object-id 1}
        data {:pre/input [(constantly zmap)]
              :pre/populate-object [(fn [zmap]
                                     {:authorize? true})
                                   [:pre/input]]
              :pre/authorize-object [(fn [zmap]
                                      (get-in zmap
                                              [:pre/populate-object
                                               :authorize?]))
                                    [:pre/populate-object]]}
        g (make-graph data)]
    (is (= (execute-graph g)
           #:shinri.core{:result #:pre{:input {:object-id 1}
                                       :populate-object {:authorize? true}
                                       :authorize-object true}})
        "Executing graph with simple shape"))

  (let [zmap {:object-id 1}
        data {:pre/input [(constantly zmap)]
              :pre/populate-object [(fn [zmap]
                                     (/ 1 0))
                                   [:pre/input]]
              :pre/authorize-object [(fn [zmap]
                                      (get-in zmap
                                              [:pre/populate-object
                                               :authorize?]))
                                    [:pre/populate-object]]}
        g (make-graph data)]
    (is (= (class (get-in (execute-graph g)
                          [:shinri.core/error :pre/populate-object]))
           java.lang.ArithmeticException)
        "Executing graph with error in execution step"))

  (let [zmap {:object-id 1}
        data {:pre/input [(constantly zmap)]
              :pre/populate-object [(fn [zmap]
                                     (/ 1 0))
                                   [:pre/input]]
              :pre/authorize-object [(fn [zmap]
                                      (get-in zmap
                                              [:pre/populate-object
                                               :authorize?]))
                                    [:pre/populate-object]]}
        g (make-graph data)]
    (is (thrown? java.lang.ArithmeticException
                 (execute-graph-ff g))
        "Executing graph in fail-fast mode with error in execution step")))
