(ns rustfs.core
  "Entry point. Two official topologies, run one at a time:

  4x4  four nodes, four drives each, one 16-drive set
  3x2  three nodes, two drives each, one 6-drive set"
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen.cli :as cli]
            [jepsen.generator :as gen]
            [jepsen.tests :as tests]
            [rustfs.checker :as rustfs-checker]
            [rustfs.client :as rustfs-client]
            [rustfs.db :as rustfs-db]
            [rustfs.nemesis :as rustfs-nemesis]
            [rustfs.topology :as topo]
            [rustfs.workload :as workload])
  (:import [org.slf4j LoggerFactory]))

(def ^:private log (LoggerFactory/getLogger "rustfs.core"))

(def cli-opts
  [[nil "--topology NAME" "4x4 or 3x2"
    :default :4x4
    :parse-fn keyword
    :validate [#{:4x4 :3x2} "must be 4x4 or 3x2"]]

   [nil "--node-template TEMPLATE" "Format for the topology's default node names. %d is 1..N."
    :default "n%d.orb.local"]

   [nil "--rustfs-version VERSION" "Release tag whose official zip is installed."
    :default "1.0.7"]

   [nil "--libc LIBC" "musl (default, static) or gnu."
    :default "musl"
    :validate [#{"musl" "gnu"} "musl or gnu"]]

   [nil "--rustfs-url URL" "Override the release zip URL. Requires --rustfs-sha256."]

   [nil "--rustfs-sha256 HEX" "sha256 of --rustfs-url."]

   [nil "--port PORT" "RustFS S3 and internode port."
    :default 9000
    :parse-fn parse-long]

   [nil "--access-key KEY" "Root access key. Test default only."
    :default "rustfsadmin"]

   [nil "--secret-key KEY" "Root secret key. Test default only; at least 8 bytes."
    :default "rustfsadmin"]

   [nil "--faults FAULTS" "Comma-separated: partition,kill,pause,packet,clock,file-corruption. Clock is for separate VMs, not Docker containers."
    :default [:partition :kill :pause :packet]
    :parse-fn #(mapv keyword (remove str/blank? (str/split % #",")))]

   [nil "--nemesis-interval SECONDS"
    :default 15
    :parse-fn parse-long]

   [nil "--rate HZ" "Approximate client op rate passed to gen/stagger as 1/HZ."
    :default 10
    :parse-fn parse-double]

   [nil "--key-count N"
    :default 8
    :parse-fn parse-long]

   [nil "--max-txn-length N" "Elle only. 1 matches the single-key S3 claim. Larger values ask for a transaction RustFS does not provide."
    :default 1
    :parse-fn parse-long]

   [nil "--per-key-limit N" "Knossos bound on operations per key."
    :default 20
    :parse-fn parse-long]

   [nil "--multipart-fraction P" "Fraction of blind writes performed as a one-part multipart upload."
    :default 0.25
    :parse-fn parse-double
    :validate [#(<= 0 % 1) "must be between 0 and 1"]]

   [nil "--workload NAME" "elle, register, versioned, or list."
    :default :elle
    :parse-fn keyword
    :validate [#{:elle :register :versioned :list} "unknown workload"]]])

(defn generator
  [test nemesis-pkg work]
  (let [nemesis-gen (:generator nemesis-pkg)
        final-nemesis (:final-generator nemesis-pkg)
        client (->> (:generator work)
                    (gen/stagger (/ 1.0 (:rate test))))
        during (if nemesis-gen
                 (gen/nemesis nemesis-gen client)
                 client)
        during (gen/time-limit (:time-limit test) during)
        during (if-let [wrap (:wrap-generator work)]
                 (wrap during)
                 during)]
    (gen/phases
     during
     (gen/nemesis final-nemesis)
     (gen/log "Nemesis stopped; quiet period before final reads")
     (gen/sleep 10)
     (:final-generator work))))

(defn test
  [opts]
  (let [topology (:topology opts)
        nodes (topo/resolve-nodes (:nodes opts) topology (:node-template opts))
        opts (assoc opts
                    :nodes nodes
                    :topology topology
                    :port (or (:port opts) 9000))
        db (rustfs-db/db)
        nemesis-pkg (rustfs-nemesis/package (assoc opts :db db))
        work (workload/workload opts)
        test (merge tests/noop-test
                    opts
                    {:name (str "rustfs-" (name topology) "-" (name (:workload opts)))
                     :os :noop
                     :db db
                     :client (rustfs-client/client)
                     :nemesis (:nemesis nemesis-pkg)
                     :pure-generators true
                     :plot {:nemeses (:perf nemesis-pkg)}
                     :checker (rustfs-checker/workload-checker (:checker work))
                     :generator (generator opts nemesis-pkg work)})]
    (doseq [line (topo/info-lines test)]
      (.info log line))
    (update test :ssh #(merge {:strict-host-key-checking :no} %))))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn test
                                          :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
