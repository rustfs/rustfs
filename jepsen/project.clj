(defproject rustfs-jepsen "0.1.0"
  :description "Jepsen tests for RustFS 1.0.7. Reuses jepsen-io/jepsen Nemesis, Elle, and Knossos."
  :url "https://github.com/rustfs/rustfs"
  :license {:name "Apache License 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  ;; The AWS SDK pulls SLF4J 1.7, which cannot load Jepsen's Logback 1.5
  ;; (org.slf4j.spi.LoggingEventAware). Keep the API version Jepsen uses.
  :dependencies [[org.clojure/clojure "1.12.7"]
                 [org.slf4j/slf4j-api "2.0.16"]
                 [jepsen "0.3.16"]
                 [software.amazon.awssdk/s3 "2.29.45" :exclusions [org.slf4j/slf4j-api]]
                 [software.amazon.awssdk/url-connection-client "2.29.45" :exclusions [org.slf4j/slf4j-api]]]
  :main rustfs.core
  :jvm-opts ["-Xmx3g"
             "-Djava.awt.headless=true"
             "-server"
             "-Djava.net.preferIPv4Stack=true"]
  :profiles {:dev {:jvm-opts ["-Xmx3g" "-Djava.awt.headless=true"]}})
