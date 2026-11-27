(ns rustfs.db
  "Install and supervise the official RustFS 1.0.7 binary on Debian nodes.

  The process is started with start-stop-daemon, not the packaged systemd unit.
  That unit sets ProtectClock=true and Restart=always, which would hide clock
  skew and restart a process the kill nemesis had stopped."
  (:require [clojure.string :as str]
            [clj-commons.slingshot :refer [try+]]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.db :as db]
            [jepsen.os.debian :as debian]
            [rustfs.topology :as topo])
  (:import [org.slf4j LoggerFactory]))

(def ^:private log (LoggerFactory/getLogger "rustfs.db"))

(def binary "/usr/local/bin/rustfs")
(def pidfile "/var/run/rustfs.pid")
(def logfile "/var/log/rustfs/rustfs.log")

(def artifact-sha256
  "sha256 of the GitHub release zip, keyed by [version arch libc]."
  {["1.0.7" "aarch64" "musl"] "e2c6d45105dd05df2f35547fbebfacf40dfba6ec4f8c31b86e30934faea79e76"
   ["1.0.7" "aarch64" "gnu"] "147ef56079be712a242c4db43866ab269a6441a14214d66bac7ed22ef5db419e"
   ["1.0.7" "x86_64" "musl"] "c90a40ba8cdc1468c71029a4d65fdd13449720183d3a8e00bc61ab09fcd448a5"
   ["1.0.7" "x86_64" "gnu"] "4cd2ee55b75f7cb7c935eeb20f8c22f17e1453009cf1af1ff8b42f7986cba201"})

(defn node-arch
  []
  (let [machine (str/trim (c/exec :uname :-m))]
    (case machine
      "aarch64" "aarch64"
      "arm64" "aarch64"
      "x86_64" "x86_64"
      (throw (ex-info "Unsupported node architecture for the 1.0.7 release zips."
                      {:uname machine})))))

(defn artifact
  [{:keys [version libc arch url sha256]}]
  (if url
    (do
      (when-not sha256
        (throw (ex-info "An overridden RustFS URL requires --rustfs-sha256."
                        {:url url})))
      {:url url :sha256 sha256})
    (let [arch (or arch "unresolved")
          pin (get artifact-sha256 [version arch libc])]
      (when-not pin
        (throw (ex-info "No pinned RustFS 1.0.7 zip for this architecture. Pass --rustfs-url and --rustfs-sha256."
                        {:version version :arch arch :libc libc})))
      {:url (format "https://github.com/rustfs/rustfs/releases/download/%s/rustfs-linux-%s-%s-v%s.zip"
                    version arch libc version)
       :sha256 pin})))

(defn install-packages!
  []
  (debian/install [:wget :unzip :curl :ca-certificates :iptables :iproute2
                   :procps :netcat-openbsd :coreutils :findutils]))

(defn install-binary!
  [test]
  (let [arch (node-arch)
        {:keys [url sha256]} (artifact (assoc test :arch arch))
        zip (cu/cached-wget! url)
        got (-> (c/exec :sha256sum zip) (str/split #"\s+") first)]
    (when-not (= sha256 got)
      (c/exec :rm :-f zip)
      (throw (ex-info "RustFS release zip checksum mismatch."
                      {:url url :expected sha256 :actual got})))
    (let [dir (cu/tmp-dir!)]
      (try
        (c/exec :unzip :-qo zip :-d dir)
        (let [paths (cu/ls dir {:recursive? true :full-path? true})
              bin (or (first (filter #(= "rustfs" (last (str/split % #"/"))) paths))
                      (throw (ex-info "The release zip does not contain a rustfs binary."
                                      {:url url :paths paths})))]
          (c/exec :install :-m "0755" bin binary))
        (finally
          (c/exec :rm :-rf dir))))
    (.info log "Installed {} from {}" binary url)))

(defn prepare-dirs!
  [test]
  (let [drives (:drives-per-node (topo/topologies (:topology test)))]
    (c/exec :mkdir :-p "/var/log/rustfs" "/var/lib/rustfs")
    (doseq [i (range 1 (inc drives))]
      (let [dir (topo/data-dir i)]
        (c/exec :mkdir :-p dir)
        (c/exec :find dir :-mindepth 1 :-delete)))))

(defn stop-timesync!
  []
  (try+ (c/exec :systemctl :stop :systemd-timesyncd) (catch Object _))
  (try+ (c/exec :systemctl :stop :chronyd) (catch Object _))
  (try+ (c/exec :timedatectl :set-ntp :false) (catch Object _)))

(defn write-credentials!
  [test]
  (c/exec :mkdir :-p "/etc/rustfs")
  (cu/write-file! (:access-key test) "/etc/rustfs/access-key")
  (cu/write-file! (:secret-key test) "/etc/rustfs/secret-key")
  (c/exec :chmod "0600" "/etc/rustfs/access-key" "/etc/rustfs/secret-key"))

(defn start-process!
  [test]
  (let [volumes (topo/volume-arg test)]
    (.info log "Starting RustFS with {}" volumes)
    (write-credentials! test)
    (cu/start-daemon!
     {:env {:RUSTFS_ACCESS_KEY_FILE "/etc/rustfs/access-key"
            :RUSTFS_SECRET_KEY_FILE "/etc/rustfs/secret-key"
            :RUSTFS_ADDRESS (str "0.0.0.0:" (:port test))
            :RUSTFS_VOLUMES volumes
            :RUSTFS_CONSOLE_ENABLE "false"
            :RUSTFS_REGION "us-east-1"}
      :logfile logfile
      :pidfile pidfile
      :chdir "/var/lib/rustfs"
      :make-pidfile? true}
     binary
     "server")
    (cu/await-tcp-port (:port test) {:timeout 180000 :retry-interval 1000})))

(defn signal!
  [signal]
  (c/su
   (try+
    (c/exec :killall :-s (name signal) :rustfs)
    (catch [:type :jepsen.control/nonzero-exit :exit 1] _ :no-process))))

(defrecord RustFS []
  db/DB
  (setup! [_ test _node]
    (c/su
     (install-packages!)
     (install-binary! test)
     (stop-timesync!)
     (prepare-dirs! test)
     (start-process! test)))

  (teardown! [_ _test _node]
    (c/su
     (cu/stop-daemon! "rustfs" pidfile)
     (try+ (c/exec :find "/var/lib/rustfs" :-mindepth 2 :-delete) (catch Object _))
     (try+ (c/exec :rm :-f logfile) (catch Object _))))

  db/LogFiles
  (log-files [_ _test _node]
    {logfile "rustfs.log"})

  db/Kill
  (kill! [_ _test _node]
    (c/su (cu/stop-daemon! "rustfs" pidfile)))

  (start! [_ test _node]
    (c/su (start-process! test)))

  db/Pause
  (pause! [_ _test _node]
    (signal! :STOP))

  (resume! [_ _test _node]
    (signal! :CONT)))

(defn db [] (RustFS.))
