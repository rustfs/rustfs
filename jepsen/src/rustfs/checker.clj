(ns rustfs.checker
  "Elle and Knossos wrappers, plus the listing and version-id invariants those
  libraries cannot state.

  Listings are not fed to Elle or Knossos. In-tree comments record that a
  listing is not a linearizable read (`rustfs/src/app/bucket_list_through.rs`,
  `crates/ecstore/src/data_usage/mod.rs`). The listing checker only rejects an
  observation that invents an ETag no acknowledged or indeterminate write can
  explain. A stale list is allowed."
  (:require [clojure.string :as str]
            [elle.rw-register :as elle-rw]
            [jepsen.checker :as checker]
            [knossos.model :as model]
            [rustfs.s3 :as s3]))

(defn txn->register-ops
  "Project single-micro-op transactions into Knossos register operations.
  A transaction with several micro-ops is left to Elle; splitting it would
  invent an order the client did not record."
  [history]
  (keep (fn [op]
          (when (and (= :txn (:f op))
                     (#{:invoke :ok :info :fail} (:type op))
                     (= 1 (count (:value op))))
            (let [[f k v] (first (:value op))]
              (assoc op
                     :f (case f :r :read :w :write f)
                     :value v
                     :key k))))
        history))

(defn register-projection-checker
  "Knossos linearizability for each key in a single-micro-op Elle history."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [projected (group-by :key (txn->register-ops history))
            results (mapv (fn [[k ops]]
                            (let [ops (mapv #(dissoc % :key) ops)
                                  result (checker/check
                                          (checker/linearizable {:model (model/cas-register)})
                                          test
                                          ops
                                          {})]
                              (assoc result :key k)))
                          projected)
            valids (map :valid? results)]
        {:valid? (cond
                   (some false? valids) false
                   (every? true? valids) true
                   :else :unknown)
         :key-count (count results)
         :results (vec (take 20 results))}))))

(defn elle-checker
  "Strict serializability from Elle, plus Knossos on each single-micro-op key.

  Elle's register cycle search does not order versions of one key from real
  time unless `:linearizable-keys?` is set, and Elle's own notes say that flag
  can report a cycle that is not there. Knossos checks each key exactly.
  Multi-micro-op transactions stay with Elle only."
  []
  (checker/compose
   {:elle (reify checker/Checker
            (check [_ _test history opts]
              (elle-rw/check {:consistency-models [:strict-serializable]
                              :directory (:directory opts)
                              :plot-timeout 10000}
                             history)))
    :linearizable (register-projection-checker)}))

(defn knossos-checker
  "Linearizability of one register. Callers that use `jepsen.independent`
  should wrap this so each key is its own register."
  []
  (checker/linearizable {:model (model/cas-register)}))

(defn- op-ok? [op] (= :ok (:type op)))
(defn- op-info? [op] (= :info (:type op)))

(defn- register-key
  "Key from an independent `[k v]` value, or from a list entry's object key."
  [value]
  (when (sequential? value)
    (first value)))

(defn- writes-by-key
  [history]
  (reduce (fn [acc op]
            (if-not (#{:write :cas} (:f op))
              acc
              (let [k (register-key (:value op))
                    etag (s3/etag-identity (:etag op))]
                (cond
                  (and (op-ok? op) etag)
                  (update-in acc [k :ok] (fnil conj []) {:etag etag :size (:size op)})

                  (op-info? op)
                  (assoc-in acc [k :indeterminate?] true)

                  :else acc))))
          {}
          history))

(defn- logical-list-key
  "S3 keys are `k-<register>`. Listing rows are compared on the register key."
  [object-key]
  (when (and (string? object-key) (str/starts-with? object-key "k-"))
    (let [suffix (subs object-key 2)]
      (or (parse-long suffix) suffix))))

(defn listing-errors
  "Fabricated listing entries. Returns a vector of error maps."
  [history]
  (let [writes (writes-by-key history)]
    (vec
     (for [op history
           :when (and (op-ok? op) (= :list (:f op)))
           :let [rows (or (:value op) [])
                 seen (volatile! #{})]
           row rows
           :let [identity (s3/etag-identity (:etag row))
                 key (logical-list-key (:key row))
                 dup? (contains? @seen key)
                 _ (vswap! seen conj key)
                 known (get writes key)
                 matched (some #(and (= identity (:etag %))
                                     (= (:size row) (:size %)))
                               (:ok known))]]
       (cond
         dup?
         {:type :duplicate-key :key key :op-index (:index op)}

         (str/blank? identity)
         {:type :blank-etag :key key :op-index (:index op)}

         matched
         nil

         (:indeterminate? known)
         nil

         :else
         {:type :fabricated
          :key key
          :etag identity
          :size (:size row)
          :op-index (:index op)})))))

(defn listing-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [errors (remove nil? (listing-errors history))]
        {:valid? (empty? errors)
         :error-count (count errors)
         :errors (vec (take 20 errors))}))))

(defn version-id-errors
  "Every acknowledged create or overwrite in a versioned bucket must return a
  version id, and those ids are unique."
  [history]
  (let [present? (fn [op] (not (str/blank? (:version-id op))))
        required (filter (fn [op]
                           (and (op-ok? op)
                                (or (and (= :write (:f op))
                                         (some? (second (:value op))))
                                    (= :cas (:f op)))))
                         history)
        missing (remove present? required)
        ids (->> history
                 (filter #(and (op-ok? %) (#{:write :cas} (:f %)) (present? %)))
                 (map :version-id))
        dupes (->> ids frequencies (filter #(> (val %) 1)) (map key))]
    (concat
     (map (fn [op] {:type :missing-version-id :op-index (:index op) :f (:f op)}) missing)
     (map (fn [id] {:type :duplicate-version-id :version-id id}) dupes))))

(defn version-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [errors (vec (version-id-errors history))]
        {:valid? (empty? errors)
         :error-count (count errors)
         :errors (vec (take 20 errors))}))))

(defn filter-checker
  "Run `inner` on the history restricted by `pred`."
  [pred inner]
  (reify checker/Checker
    (check [_ test history opts]
      (checker/check inner test (filter pred history) opts))))

(defn workload-checker
  "Compose perf, stats, and the workload's own Elle or Knossos checker.
  Nemesis intervals are drawn when the test map has `:plot {:nemeses ...}`."
  [workload-checker]
  (checker/compose
   {:stats (checker/stats)
    :perf (checker/perf)
    :workload workload-checker}))
