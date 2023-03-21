# shinri

Shinri is a small library that let's you glue together side effects

<img src="helpshift-logo.png" alt="drawing" width="200" height="200"/>

```
Shinri - A pantheistic and semi-cerebral God-like being who tauntingly regulates all alchemy (side effects) use.
```


## Usage

### Simple case

```clojure
(require '[shinri.core :as sc])

(defn gen-g
  [input]
  {:stage-1 [(constantly input)]
   :stage-2 [(fn [{:keys [stage-1]}]
               stage-1)
             [:stage-1]]})

(def g (make-graph (gen-g {:foo :bar})))

(view-graph g)

(execut-graph) => #:shinri.core{:result {:stage-1 {:foo :bar} :stage-2 :bar}}
```

### Detailed case

```clojure
(require '[shinri.core :as sc])

(defn gen-g
  [input]
  {:pre/input [(constantly input)]
   :db/read-1 [(fn [{:keys [pre/input]}]
                 :foo-1)
               [:pre/input]]
   :db/read-2 [(fn [{:keys [pre/input]}]
                 :foo-2)
               [:pre/input]]
   :db/read-3 [(fn [{:keys [pre/input]}]
                 :foo-3)
               [:pre/input]]
   :db/update [(fn [{:keys [db/read-1 db/read-2 db/read-3]}]
                 (println db/read-1 db/read-2 db/read-3))
               [:pre/input]]})

(def g (make-graph (gen-g {:foo :bar})))

(view-graph g)

(execut-graph) => {::sc/result {:pre/input {:foo :bar}
                                :db/read-1 :foo-1
                                :db/read-2 :foo-2
                                :db/read-3 :foo-3
                                :db/update nil}}
```

## Similar projects

### Plumatic Plumbing

https://github.com/plumatic/plumbing

## License

Copyright © Helpshift Inc. 2020

EPL (See [LICENSE](https://github.com/helpshift/tardigrade/blob/master/LICENSE))
