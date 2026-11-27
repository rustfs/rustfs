(ns rustfs.topology-test
  (:require [clojure.test :refer [deftest is testing]]
            [rustfs.topology :as topo]))

(deftest official-quorums-match-rustfs
  (doseq [topology [:4x4 :3x2]]
    (is (topo/official-quorum-matches? topology) (name topology)))
  (is (= {:drives 16 :parity 4 :data 12 :read-quorum 12 :write-quorum 12}
         (topo/quorum 16)))
  (is (= {:drives 6 :parity 3 :data 3 :read-quorum 3 :write-quorum 4}
         (topo/quorum 6))))

(deftest default-five-nodes-become-the-topology
  (is (= ["n1.orb.local" "n2.orb.local" "n3.orb.local" "n4.orb.local"]
         (topo/resolve-nodes [:n1 :n2 :n3 :n4 :n5] :4x4 "n%d.orb.local")))
  (is (= ["n1.orb.local" "n2.orb.local" "n3.orb.local"]
         (topo/resolve-nodes ["n1" "n2" "n3" "n4" "n5"] :3x2 "n%d.orb.local"))))

(deftest explicit-nodes-must-match-the-count
  (is (= ["n1" "n2" "n3" "n4"]
         (topo/resolve-nodes ["n1" "n2" "n3" "n4"] :4x4 "n%d.orb.local")))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Node count"
                        (topo/resolve-nodes ["n1" "n2"] :4x4 "n%d.orb.local"))))

(deftest volume-argument-is-one-ellipsis-pool
  (testing "4x4 is the cartesian product of four hosts and four directories"
    (is (= "http://n{1...4}.orb.local:9000/var/lib/rustfs/data{1...4}"
           (topo/volume-arg {:topology :4x4
                             :port 9000
                             :nodes ["n1.orb.local" "n2.orb.local"
                                     "n3.orb.local" "n4.orb.local"]}))))
  (testing "3x2 is three hosts and two directories"
    (is (= "http://vm{1...3}:9000/var/lib/rustfs/data{1...2}"
           (topo/volume-arg {:topology :3x2
                             :port 9000
                             :nodes ["vm1" "vm2" "vm3"]})))))

(deftest nodes-must-be-numbered-without-gaps
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"no gaps"
                        (topo/volume-arg {:topology :4x4
                                          :port 9000
                                          :nodes ["n1" "n2" "n4" "n3"]}))))
