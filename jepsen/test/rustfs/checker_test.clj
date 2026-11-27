(ns rustfs.checker-test
  (:require [clojure.test :refer [deftest is]]
            [elle.rw-register :as elle-rw]
            [jepsen.checker :as checker]
            [jepsen.history :as history]
            [knossos.model :as model]
            [rustfs.checker :as c]
            [rustfs.db :as db]
            [rustfs.s3 :as s3]))

(def knossos-test
  {:name "knossos-unit"
   :start-time "unit"})

(defn- check-linearizable [ops]
  (checker/check (checker/linearizable {:model (model/cas-register)})
                 knossos-test
                 ops
                 {}))

(deftest knossos-accepts-a-real-time-write-then-read
  (let [result (check-linearizable
                [{:index 0 :process 0 :type :invoke :f :write :value 1}
                 {:index 1 :process 0 :type :ok :f :write :value 1}
                 {:index 2 :process 1 :type :invoke :f :read :value nil}
                 {:index 3 :process 1 :type :ok :f :read :value 1}])]
    (is (:valid? result))))

(deftest knossos-rejects-a-read-that-finishes-before-its-write
  (let [result (check-linearizable
                [{:index 0 :process 0 :type :invoke :f :read :value nil}
                 {:index 1 :process 0 :type :ok :f :read :value 1}
                 {:index 2 :process 1 :type :invoke :f :write :value 1}
                 {:index 3 :process 1 :type :ok :f :write :value 1}])]
    (is (false? (:valid? result)))))

(defn- txn [index process type value time]
  {:index index :process process :type type :f :txn :value value :time time})

(deftest elle-accepts-a-single-key-write-then-read
  (let [history (history/history
                 [(txn 0 0 :invoke [[:w :x 1]] 0)
                  (txn 1 0 :ok [[:w :x 1]] 1)
                  (txn 2 1 :invoke [[:r :x nil]] 2)
                  (txn 3 1 :ok [[:r :x 1]] 3)])
        result (elle-rw/check {:consistency-models [:strict-serializable]} history)]
    (is (:valid? result) (pr-str (select-keys result [:valid? :anomaly-types])))))

(deftest elle-rejects-a-read-of-an-aborted-write
  (let [history (history/history
                 [(txn 0 0 :invoke [[:w :x 1]] 0)
                  (txn 1 0 :fail [[:w :x 1]] 1)
                  (txn 2 1 :invoke [[:r :x nil]] 2)
                  (txn 3 1 :ok [[:r :x 1]] 3)])
        result (elle-rw/check {:consistency-models [:strict-serializable]} history)]
    (is (false? (:valid? result)) (pr-str (:anomaly-types result)))))

(deftest knossos-rejects-a-stale-read-after-a-later-write
  (let [result (check-linearizable
                [{:index 0 :process 0 :type :invoke :f :write :value 1}
                 {:index 1 :process 0 :type :ok :f :write :value 1}
                 {:index 2 :process 1 :type :invoke :f :write :value 2}
                 {:index 3 :process 1 :type :ok :f :write :value 2}
                 {:index 4 :process 2 :type :invoke :f :read :value nil}
                 {:index 5 :process 2 :type :ok :f :read :value 1}])]
    (is (false? (:valid? result)))))

(deftest listing-checker-allows-a-known-etag-and-rejects-a-fabricated-one
  (let [history [{:index 0 :type :ok :f :write :value [1 7] :etag "\"abc\"" :size 1}
                 {:index 1 :type :ok :f :list
                  :value [{:key "k-1" :etag "\"abc\"" :size 1}
                          {:key "k-2" :etag "\"nope\"" :size 1}]}]
        result (checker/check (c/listing-checker) {} history {})]
    (is (false? (:valid? result)))
    (is (= [:fabricated] (map :type (:errors result))))))

(deftest listing-checker-accepts-the-etag-that-was-written
  (let [history [{:index 0 :type :ok :f :write :value [1 7] :etag "\"abc\"" :size 1}
                 {:index 1 :type :ok :f :list
                  :value [{:key "k-1" :etag "abc" :size 1}]}]
        result (checker/check (c/listing-checker) {} history {})]
    (is (:valid? result) (pr-str result))))

(deftest version-ids-must-exist-and-be-unique
  (is (:valid? (checker/check (c/version-checker) {}
                              [{:type :ok :f :write :value [1 2] :version-id "v1"}
                               {:type :ok :f :read :value [1 2] :version-id "v1"}
                               {:type :ok :f :write :value [1 3] :version-id "v2"}]
                              {})))
  (let [missing (checker/check (c/version-checker) {}
                               [{:type :ok :f :write :value [1 2]}]
                               {})]
    (is (false? (:valid? missing))))
  (let [dup (checker/check (c/version-checker) {}
                           [{:type :ok :f :write :value [1 2] :version-id "v1"}
                            {:type :ok :f :write :value [1 3] :version-id "v1"}]
                           {})]
    (is (false? (:valid? dup)))))

(deftest etag-normalization
  (is (= "abc" (s3/etag-identity "\"abc\"")))
  (is (= "abc-1" (s3/etag-identity "W/\"abc-1\"")))
  (is (= "\"abc\"" (s3/quoted-etag "abc")))
  (is (= "\"abc\"" (s3/quoted-etag "\"abc\""))))

(deftest pinned-aarch64-musl-zip
  (let [got (db/artifact {:version "1.0.7" :arch "aarch64" :libc "musl"})]
    (is (= "e2c6d45105dd05df2f35547fbebfacf40dfba6ec4f8c31b86e30934faea79e76"
           (:sha256 got)))
    (is (re-find #"rustfs-linux-aarch64-musl-v1\.0\.7\.zip$" (:url got)))))
