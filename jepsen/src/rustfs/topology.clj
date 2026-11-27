(ns rustfs.topology
  "Official RustFS Jepsen topologies and the volume arguments they require.

  RustFS expands `{start...end}` in `RUSTFS_VOLUMES` the way MinIO does: every
  brace is a dimension of one pool, and the dimensions multiply. That is the
  layout exercised by `DisksLayout` (for example
  `http://rustfs{0...15}.mydomain.net/data{0...1}` is 32 endpoints). A single
  argument with no second pool is one pool. Set width is then chosen from the
  divisors of the endpoint count, preferring the widest symmetric set of at
  most 16 drives.

  `http://n{1...4}.orb.local:9000/var/lib/rustfs/data{1...4}` is therefore
  4×4 = 16 endpoints and one set of 16. `...data{1...2}` on three hosts is
  3×2 = 6 endpoints and one set of 6. The path after the host is a directory
  on that node, not an HTTP prefix.

  Default parity comes from `default_parity_count`: 1→0, 2–3→1, 4–5→2, 6–7→3,
  8 or more→4. Read quorum is `drives - parity`. Write quorum is the data
  count, plus one when that count equals parity (`default_write_quorum`).")

(def default-jepsen-nodes
  "Node list Jepsen supplies when the command does not pass `-n`."
  ["n1" "n2" "n3" "n4" "n5"])

(def topologies
  {:4x4 {:nodes 4
         :drives-per-node 4
         :drives 16
         :set-size 16
         :parity 4
         :read-quorum 12
         :write-quorum 12
         :survives-one-node-loss? true
         :majority-partition "2-2, eight drives on each side, neither side has quorum 12"}
   :3x2 {:nodes 3
         :drives-per-node 2
         :drives 6
         :set-size 6
         :parity 3
         :read-quorum 3
         :write-quorum 4
         :survives-one-node-loss? true
         :majority-partition "2-1. The two-node side has four drives and can write. The single node has two drives and cannot read or write."}})

(defn node-name
  [node]
  (cond
    (keyword? node) (name node)
    (symbol? node) (name node)
    :else (str node)))

(defn parse-numbered-host
  "Split `n12.orb.local` into prefix `n`, number 12, suffix `.orb.local`."
  [host]
  (let [host (node-name host)]
    (if-let [[_ prefix number suffix] (re-matches #"^(.*?)(\d+)([^\d]*)$" host)]
      {:host host
       :prefix prefix
       :number (Long/parseLong number)
       :suffix suffix}
      (throw (ex-info "Node name must end in a number so RUSTFS_VOLUMES can use {start...end}."
                      {:node host})))))

(defn default-nodes
  [topology template]
  (let [{:keys [nodes]} (topologies topology)]
    (mapv #(format template %) (range 1 (inc nodes)))))

(defn resolve-nodes
  "Use an explicit `-n` list when it matches the topology. The untouched Jepsen
  default of five nodes is replaced by the topology's own names."
  [requested topology template]
  (let [names (mapv node-name requested)
        spec (topologies topology)
        official (default-nodes topology template)]
    (cond
      (nil? spec)
      (throw (ex-info "Unknown topology. Use 4x4 or 3x2."
                      {:topology topology}))

      (= names default-jepsen-nodes)
      official

      (= (count names) (:nodes spec))
      names

      :else
      (throw (ex-info "Node count does not match the topology."
                      {:topology topology
                       :expected (:nodes spec)
                       :nodes names})))))

(defn ellipsis-host
  "Turn [n1.orb.local n2.orb.local ...] into `n{1...4}.orb.local`."
  [nodes]
  (let [parsed (mapv parse-numbered-host nodes)
        prefixes (map :prefix parsed)
        suffixes (map :suffix parsed)
        numbers (mapv :number parsed)
        expected (vec (range 1 (inc (count parsed))))]
    (when-not (and (= 1 (count (set prefixes)))
                   (= 1 (count (set suffixes))))
      (throw (ex-info "Every node must share one prefix and one suffix."
                      {:nodes (map :host parsed)})))
    (when (not= numbers expected)
      (throw (ex-info "Node numbers must be 1..N with no gaps, in order."
                      {:nodes (map :host parsed)
                       :numbers numbers})))
    (str (first prefixes) "{" (first numbers) "..." (peek numbers) "}" (first suffixes))))

(defn data-dir
  "On-disk directory for drive `i` (1-based). The same path is embedded in the volume URL."
  [i]
  (str "/var/lib/rustfs/data" i))

(defn volume-arg
  "The single `RUSTFS_VOLUMES` value, identical on every node."
  [{:keys [nodes port topology]}]
  (let [spec (topologies topology)
        host (ellipsis-host nodes)]
    (when-not spec
      (throw (ex-info "Unknown topology." {:topology topology})))
    (format "http://%s:%s/var/lib/rustfs/data{1...%d}"
            host
            (or port 9000)
            (:drives-per-node spec))))

(defn describe
  [topology]
  (let [spec (topologies topology)]
    (format "%s: %d nodes x %d drives = %d endpoints, one erasure set of %d, parity %d, read quorum %d, write quorum %d. %s"
            (name topology)
            (:nodes spec)
            (:drives-per-node spec)
            (:drives spec)
            (:set-size spec)
            (:parity spec)
            (:read-quorum spec)
            (:write-quorum spec)
            (:majority-partition spec))))

(defn quorum
  "Mirror `default_parity_count` and `default_write_quorum` for a set of `drives`."
  [drives]
  (let [parity (cond
                 (<= drives 1) 0
                 (<= drives 3) 1
                 (<= drives 5) 2
                 (<= drives 7) 3
                 :else 4)
        data (- drives parity)
        write (if (= data parity) (inc data) data)]
    {:drives drives
     :parity parity
     :data data
     :read-quorum data
     :write-quorum write}))

(defn official-quorum-matches?
  [topology]
  (let [spec (topologies topology)
        got (quorum (:drives spec))]
    (and (= (:parity spec) (:parity got))
         (= (:read-quorum spec) (:read-quorum got))
         (= (:write-quorum spec) (:write-quorum got)))))

(defn info-lines
  [test]
  [(describe (:topology test))
   (str "RUSTFS_VOLUMES=" (volume-arg test))])
