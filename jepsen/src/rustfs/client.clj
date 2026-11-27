(ns rustfs.client
  "Jepsen client. Each process talks to one node over path-style S3.

  A lost response is `:info`. A precondition rejection is `:fail` only when no
  earlier operation in the transaction has already committed. Reads of a
  missing key are successful reads of nil."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [rustfs.s3 :as s3]))

(def bucket "jepsen")

(defn- endpoint [test node]
  (str "http://" node ":" (:port test)))

(defn- definite? [ex] (:definite? (ex-data ex)))

(defn- fail [op error]
  (assoc op :type :fail :error error))

(defn- info-op [op error]
  (assoc op :type :info :error error))

(defn- with-meta-fields
  [op meta]
  (assoc op
         :etag (:etag meta)
         :version-id (:version-id meta)
         :size (:size meta)))

(defn- invoke-register
  [conn test op]
  (let [[k v] (:value op)]
    (case (:f op)
      :read
      (let [got (s3/read-value conn bucket k)]
        (-> op
            (assoc :type :ok :value [k (:value got)])
            (with-meta-fields got)))

      :write
      (let [multipart? (< (rand) (:multipart-fraction test 0))
            wrote (s3/write-value! conn bucket k v {:multipart? multipart?})]
        (-> op
            (assoc :type :ok)
            (with-meta-fields wrote)))

      :cas
      (let [[expected desired] v
            current (s3/read-value conn bucket k)]
        (cond
          (not= (:value current) expected)
          (fail op :precondition)

          (and (nil? expected) (some? desired))
          (let [wrote (s3/write-value! conn bucket k desired {:if-none-match "*"})]
            (-> op (assoc :type :ok) (with-meta-fields wrote)))

          (some? expected)
          (let [wrote (s3/write-value! conn bucket k desired {:if-match (:etag current)})]
            (-> op (assoc :type :ok) (with-meta-fields wrote)))

          :else
          (let [wrote (s3/write-value! conn bucket k nil {})]
            (-> op (assoc :type :ok) (with-meta-fields wrote))))))))

(defn- apply-mop
  [conn test [f k v]]
  (case f
    :r (let [got (s3/read-value conn bucket k)]
         [:r k (:value got)])
    :w (do
         (s3/write-value! conn bucket k v
                          {:multipart? (< (rand) (:multipart-fraction test 0))})
         [:w k v])))

(defn- invoke-txn
  [conn test op]
  (loop [done [] mops (:value op)]
    (if-not (seq mops)
      (assoc op :type :ok :value done)
      (let [mop (first mops)
            result (try
                     [:ok (apply-mop conn test mop)]
                     (catch clojure.lang.ExceptionInfo e
                       [:err e]))]
        (if (= :ok (first result))
          (recur (conj done (second result)) (next mops))
          (let [e (second result)
                error (:error (ex-data e) :unknown)]
            (if (and (definite? e) (empty? done))
              (assoc op :type :fail :error error)
              (assoc op :type :info :error error :value (vec (conj done mop))))))))))

(defn- handle
  [op f]
  (try
    (f)
    (catch clojure.lang.ExceptionInfo e
      (if (definite? e)
        (fail op (:error (ex-data e) :rejected))
        (info-op op (:error (ex-data e) :indeterminate))))))

(defrecord S3Client [conn node]
  client/Client
  (open! [this test node]
    (assoc this
           :node node
           :conn (s3/connect {:endpoint (endpoint test node)
                              :access-key (:access-key test)
                              :secret-key (:secret-key test)})))

  (setup! [_ test]
    (let [deadline (+ (System/currentTimeMillis) 180000)]
      (loop []
        (let [outcome (try
                        (s3/ensure-bucket! conn bucket)
                        (when (= :versioned (:workload test))
                          (s3/enable-versioning! conn bucket))
                        :ready
                        (catch Exception e
                          e))]
          (cond
            (= :ready outcome)
            (info "S3 client ready on" node "bucket" bucket)

            (< (System/currentTimeMillis) deadline)
            (do
              (warn "Waiting for RustFS on" node ":" (.getMessage ^Exception outcome))
              (Thread/sleep 1000)
              (recur))

            :else
            (throw outcome))))))

  (invoke! [_ test op]
    (case (:f op)
      :txn (invoke-txn conn test op)
      :list (handle op #(assoc op :type :ok :value (s3/list-prefix conn bucket)))
      (:read :write :cas) (handle op #(invoke-register conn test op))
      (info-op op :unsupported)))

  (teardown! [_ _test])

  (close! [_ _test]
    (s3/close! conn)))

(defn client [] (S3Client. nil nil))
