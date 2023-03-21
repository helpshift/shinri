(ns shinri.core
  (:require [loom.graph :as lg]
            [loom.io :as li]
            [loom.alg :as la]))

(defn- in-ns?
  [ns node]
  (= (namespace node)
     (str ns)))

(defn- filter-nodes-by-ns
  [ns nodes]
  (filter (partial in-ns? ns)
          nodes))

(defn- massage-node
  [all-nodes node]
  (cond
    (= "*" (name node)) (filter-nodes-by-ns (namespace node)
                                            all-nodes)
    :else [node]))

(defn- seq->map
  [data]
  (let [nodes-xs (partition 2
                            data)
        [[first-k first-v] & rest-nodes] nodes-xs]
    (-> (reduce conj
                {}
                (map (fn [[k v] [dep _]]
                       [k [v
                           [dep]]])
                     rest-nodes
                     (drop-last nodes-xs)))
        (assoc first-k [first-v]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-graph
  "Create a graph of all actions based on dependencies declared
  Ex.
  (make-graph (let [zmap {:issue-id 1}]
                {:pre/input [(constantly zmap)]
                 :pre/populate-issue [(fn [zmap]
                                        {:authorize? true})
                                      [:pre/input]]
                 :pre/authorize-issue [(fn [zmap]
                                         (get-in zmap
                                                 [:pre/populate-issue
                                                  :authorize?]))
                                       [:pre/populate-issue]]}))
  Input data looks like this. Here each step is an entry in a hashmap
  key is an unique identifier for the action
  value is a tuple of a function and a vector of dependencies on other actions

  Function is a single arity function which expects a zmap. Zmap will have entries
  of all the actions processed with their output against action's unique identifier

  :pre/authorize-issue's action zmap will have following entry in it,

   {:pre/input {:issue-id 1}
    :pre/populate-issue {:issue-id 1
                         :body \"title\"
                         :authorize? true}}

  This function only constructs a graph data structure but does not execute the actions."
  [data]
  (let [data (if (map? data)
               data
               (seq->map data))
        all-nodes (keys data)
        g (lg/digraph (reduce (partial merge-with into)
                              {}
                              (mapcat (fn [[parent [_ children]]]
                                        (let [m-children (mapcat (partial massage-node
                                                                          all-nodes)
                                                                 children)]
                                          (when-let [deps (seq (filter (complement (set all-nodes))
                                                                       m-children))]
                                            (throw (Exception. (str "Deps for action "
                                                                    parent
                                                                    " not found "
                                                                    deps))))
                                          (map #(hash-map % [parent] )
                                               m-children)))
                                      data)))]
    (if (la/dag? g)
      (with-meta g {:data data})
      (do
        (li/view g)
        (throw (Exception. "Cyclic graph is not allowed"))))))


(defn view-graph
  [g]
  (li/view g))


(defn execute-graph
  "For given data create a graph and execute all actions"
  [g & {:keys [fail-fast?]
        :or {fail-fast? false}}]
  (let [nodes (la/topsort g)
        data (get (meta g) :data)]
    (reduce (fn [op node]
              (let [node-data (get data node)
                    action (first node-data)
                    [state result]
                    (try [::success (action (get op ::result))]
                         (catch Throwable e
                           (if (= (count node-data) 3)
                             (let [tbhrow-pred (if fail-fast?
                                                (complement (nth node-data 2))
                                                (nth node-data 2))]
                               (when (throw-pred e)
                                 (throw e)))
                             (when fail-fast?
                               (throw e)))
                           [::failure e]))]
                (if (= state ::success)
                  (assoc-in op [::result node] result)
                  (assoc-in op [::error node] result))))
            {}
            nodes)))


(defmacro ->
  "Macro version of (execute-graph-ff (make-graph k*))

  Ex.

  (-> {:issue-id 1}
      [:prepare_write/op-1 (fn [{:keys [issue-id]}]
                             {:action \"insert\"
                              :collection \"db\"
                              :doc {:id issue-id
                                    :body \"test\"}})]
      [:prepare_write/op-2 (fn [{:keys [issue-id]}]
                             {:action \"insert\"
                              :collection \"db\"
                              :doc {:id issue-id
                                    :body \"test\"}})]
      [:commit_write/op (fn [data]
                          ;; Commit to mongo

                          (vals (select-keys data
                                             [:prepare_write/op-1
                                              :prepare_write/op-2])))])



  Preferrable since it works well with Clojure debugging tooling"
  [i & forms]
  (let [forms (if (map? (first forms))
                (first forms)
                (seq->map (mapcat identity forms)))
        nodes (la/topsort (make-graph forms))
        z-sym (gensym "zmap")
        x `(~z-sym)
        res (loop [x x, nodes (reverse nodes)]
              (if nodes
                (let [node (first nodes)
                      form (get forms node)
                      [k [v _]] [node form]
                      m (meta v)
                      threaded (list
                                (concat
                                 `(let [~z-sym (try
                                                 (assoc-in ~z-sym
                                                           [::result ~k]
                                                           (~v (get ~z-sym
                                                                    ::result)))
                                                 (catch
                                                     Exception
                                                     e#
                                                   (assoc-in ~z-sym
                                                             [::error ~k]
                                                             e#)))])
                                 x))]
                  (recur threaded (next nodes)))
                x))]
    (concat `(let [~z-sym {::result ~i}])
            res)))


(defn execute-graph-ff
  "For given data create a graph and execute all actions"
  [g]
  (execute-graph g :fail-fast? true))

(defn errors?
  [output]
  (boolean (seq (get output ::error))))

(comment


  ;;; Simple example

  (def k* (let [input {:foo :bar}]
            {:stage-1 [(constantly input)]
             :stage-2 [(fn [{:keys [stage-1]}]
                         (get stage-1 :foo))
                       [:stage-1]]}))
  (li/view (make-graph k*))
  (execute-graph-ff (make-graph k*))

  ;;; Multiple dependencies
  (def l* (let [zmap {:issue-id 1}]
            {:pre/input [(fn [_]
                           {:issue-id 1})]
             :pre/populate-issue [(fn [_]
                                    {:body "title"})
                                  [:pre/input]]
             :pre/authorize-issue [(fn [zmap]
                                     (get-in zmap [:pre/populate-issue :authorize?]))
                                   [:pre/populate-issue]]
             :ml/add-predict-label [(fn [zmap]
                                      {:meta {:label (when (= (get-in zmap [:pre/populate-issue :body])
                                                              "title")
                                                       "lol")}})
                                    [:pre/authorize-issue]]
             :ml/lang-detect [(fn [zmap]
                                {:meta {:lang (when (= (get-in zmap [:pre/populate-issue :body])
                                                       "title")
                                                "eng")}})
                              [:pre/authorize-issue]]
             :db/update-issue [(fn [zmap]
                                 ;; (println "Update issue in mongo "
                                 ;;          {:id (get-in zmap [:pre/populate-issue :id])}
                                 ;;          (merge (get-in zmap [:ml/add-predict-label])
                                 ;;                 (get-in zmap [:ml/lang-detect])))
                                 (update-issue (:authorize? zmap)))
                               [:pre/authorize-issue
                                :ml/* ;; Supports * under namespace of keys
                                ]]}))

  (li/view (make-graph l*))

  (defn predict-handler
    [zmap]
    (execute-graph (make-graph l*)))


  (letfn [(exec [m fun]
            (fun m))]
    (defn thrush
      "The Thrush Combinator
  More info : http://blog.fogus.me/2010/09/28/thrush-in-clojure-redux/"
      [zmap & fns]
      (reduce exec zmap fns)))

  ;; Acc db actions
  (thrush {:db-actions []}
          (fn [zmap]
            (update zmap :db-actions conj {:action "insert"
                                           :collection "db"
                                           :doc {:id 1
                                                 :body "test"}}))

          (fn [zmap]
            (update zmap :db-actions conj {:action "insert"
                                           :collection "db"
                                           :doc {:id 2
                                                 :body "test"}}))
          (fn [zmap]
            ;;commit to mongo
            (println (:db-actions zmap))))



  (def l* (let [zmap {:issue-id 1}]
            {:pre/input [(fn [_]
                           {:issue-id 1})]
             :prepare_write/op-1 [(fn [_]
                                    {:action "insert"
                                     :collection "db"
                                     :doc {:id 1
                                           :body "test"}})
                                  [:pre/input]]
             :prepare_write/op-2 [(fn [_]
                                    {:action "insert"
                                     :collection "db"
                                     :doc {:id 2
                                           :body "test"}})
                                  [:pre/input]]
             :commit_write/op [(fn [zmap]
                                 ;; Commit to mongo
                                 (println (vals (select-keys zmap
                                                             [:prepare_write/op-1
                                                              :prepare_write/op-2]))))
                               [:prepare_write/*]]}))
  (li/view (make-graph l*))

  ;;; Error cases

  ;;; Cyclic graph is not allowed
  (def x* (let [zmap {:issue-id 1}]
            {:pre/input [(constantly zmap)
                         [:db/create-side-effects]]
             :db/create-side-effects [(fn [{:keys [input]}]
                                        (println input)
                                        :foo)
                                      [:pre/input]]}))
  (make-graph x*)

  ;;; Missing dependency
  (def y* (let [zmap {:issue-id 1}]
            {:pre/input [(constantly zmap)]
             :db/create-side-effects [(fn [{:keys [input]}]
                                        (println input)
                                        :foo)
                                      [:pre/input1]]}))
  (make-graph y*)



  (def k* (let [zmap {:issue-id 1}]
            {:pre/input [(constantly zmap)]
             :db/create-side-effects [(fn [{:keys [pre/input]}]
                                        (/ 1 0))
                                      [:pre/input]]}))
  (execute-graph (make-graph k*))

  (class (get-in (execute-graph (make-graph k*))
                 [::error :db/create-side-effects]))

  (def k* (let [zmap {:issue-id 1}]
            {:pre/input [(constantly zmap)]
             :db/create-side-effects [(fn [{:keys [pre/input]}]
                                        (/ 1 0))
                                      [:pre/input]
                                      (fn [e]
                                        (= java.lang.ArithmeticException
                                           (class e)))]}))

  (execute-graph (make-graph k*))

  (def k* (let [zmap {:issue-id 1}]
            {:pre/input [(constantly zmap)]
             :db/create-side-effects [(fn [{:keys [pre/input]}]
                                        (/ 1 0))
                                      [:pre/input]

                                      (fn [e]
                                        (= (class e)
                                           java.lang.ArithmeticException))]}))

  (execute-graph-ff (make-graph k*))


  ;; Benchmarking results


  (def l* (make-graph (let [zmap {:issue-id 1}]
                        {:pre/input [(fn [_]
                                       {:issue-id 1})]
                         :prepare_write/op-1 [(fn [_]
                                                {:action "insert"
                                                 :collection "db"
                                                 :doc {:id 1
                                                       :body "test"}})
                                              [:pre/input]]
                         :prepare_write/op-2 [(fn [_]
                                                {:action "insert"
                                                 :collection "db"
                                                 :doc {:id 2
                                                       :body "test"}})
                                              [:pre/input]]
                         :commit_write/op [(fn [zmap]
                                             ;; Commit to mongo
                                             (vals (select-keys zmap
                                                                [:prepare_write/op-1
                                                                 :prepare_write/op-2])))
                                           [:prepare_write/*]]})))
  (require '[criterium.core :as cc])

  (defn test-exec-graph
    []
    (execute-graph l*))

  ;; 10.490653 µs
  (cc/with-progress-reporting (cc/bench (test-exec-graph)))

  (defn test-thread-macro
    []
    (-> {:issue-id 1}
        [:prepare_write/op-1 (fn [{:keys [issue-id]}]
                               {:action "insert"
                                :collection "db"
                                :doc {:id issue-id
                                      :body "test"}})]
        [:prepare_write/op-2 (fn [{:keys [issue-id]}]
                               {:action "insert"
                                :collection "db"
                                :doc {:id issue-id
                                      :body "test"}})]
        [:commit_write/op (fn [data]
                            ;; Commit to mongo

                            (vals (select-keys data
                                               [:prepare_write/op-1
                                                :prepare_write/op-2])))]))

  ;; 1.093498 µs
  (cc/with-progress-reporting (cc/bench (test-thread-macro))))
