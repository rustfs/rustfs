(ns rustfs.nemesis
  "Faults from jepsen.nemesis.combined. Disk-full and slow-disk are not in
  Jepsen 0.3.16; packet delay is the latency fault the library does provide."
  (:require [jepsen.nemesis.combined :as combined]))

(def supported
  #{:partition :kill :pause :clock :packet :file-corruption})

(defn package
  [opts]
  (let [faults (set (:faults opts))
        unknown (remove supported faults)]
    (when (seq unknown)
      (throw (ex-info "Unknown nemesis fault."
                      {:unknown (vec unknown)
                       :supported (sort supported)})))
    (combined/nemesis-package
     (cond-> {:db (:db opts)
              :interval (:nemesis-interval opts 15)
              :faults faults
              :partition {:targets [:one :majority :majorities-ring]}
              :packet {:targets [:one]
                       :behaviors [{:delay {}}
                                   {:loss {:percent :20%}}]}
              :kill {:targets [:one :minority]}
              :pause {:targets [:one :minority]}
              :clock {:targets [:one]}}
       (contains? faults :file-corruption)
       (assoc :file-corruption
              {:targets [:one]
               :corruptions [{:type :bitflip
                              :file "/var/lib/rustfs/data1"
                              :probability 1.0e-4}]})))))
