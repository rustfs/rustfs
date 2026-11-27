(ns rustfs.workload
  "Workloads. Elle sees single-key transactions because RustFS does not offer a
  multi-key transaction. Knossos sees an independent compare-and-set register
  per key, using If-Match and If-None-Match, which 1.0.7 implements."
  (:require [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.tests.cycle.wr :as wr]
            [jepsen.tests.linearizable-register :as linreg]
            [rustfs.checker :as rustfs-checker]))

(defn elle
  [test]
  (let [base (wr/test {:key-count (:key-count test)
                       :min-txn-length 1
                       :max-txn-length (:max-txn-length test)
                       :consistency-models [:strict-serializable]})]
    (assoc base :checker (rustfs-checker/elle-checker))))

(defn register
  [test]
  (linreg/test {:nodes (:nodes test)
                :per-key-limit (:per-key-limit test)}))

(defn versioned
  [test]
  (let [base (register test)]
    (update base :checker
            (fn [inner]
              (checker/compose {:register inner
                                :versions (rustfs-checker/version-checker)})))))

(defn list-gen
  [test]
  (let [n (count (:nodes test))]
    (gen/reserve
     1 (repeat {:type :invoke :f :list})
     (independent/concurrent-generator
      n
      (range (:key-count test))
      (fn [_k]
        (gen/mix
         [(fn [_ _] {:type :invoke :f :read})
          (fn [_ _] {:type :invoke :f :write :value (rand-int 1000000)})]))))))

(defn list-workload
  [test]
  (let [base (register (assoc test :per-key-limit (:per-key-limit test)))]
    {:generator (list-gen test)
     :final-generator nil
     :checker (checker/compose
               {:register (rustfs-checker/filter-checker
                           #(#{:read :write :cas} (:f %))
                           (:checker base))
                :listing (rustfs-checker/listing-checker)})}))

(defn workload
  [test]
  (case (:workload test)
    :elle (elle test)
    :register (register test)
    :versioned (versioned test)
    :list (list-workload test)
    (throw (ex-info "Unknown workload."
                    {:workload (:workload test)
                     :known [:elle :register :versioned :list]}))))
