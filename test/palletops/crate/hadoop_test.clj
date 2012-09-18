(ns palletops.crate.hadoop-test
  (:use
   [pallet.actions :only [exec-checked-script]]
   [pallet.build-actions :only [build-actions]]
   palletops.crate.hadoop
   clojure.test))

(deftest hadoop-settings-test
  (is
   (build-actions {}
     (hadoop-settings {}))))

(deftest hadoop-exec-test
  (is
   (build-actions {}
     (hadoop-settings {})
     (hadoop-exec "ls"))))

(deftest hadoop-service-test
  (testing "default action"
    (is (= (first
            (build-actions {:phase-context "hadoop-service"}
              (exec-checked-script
               (str "start hadoop daemon: namenode")
               (if-not (pipe (jps) (grep "-i" namenode))
                 ((str "/usr/local/hadoop-0.20.2/bin/hadoop-daemon.sh")
                  start namenode)))))
           (first
            (build-actions {}
              (hadoop-settings {})
              (hadoop-service "namenode" {}))))))
  (testing "if-stopped"
    (is (= (first
            (build-actions {:phase-context "hadoop-service"}
              (exec-checked-script
               (str "start hadoop daemon: n")
               ((str "/usr/local/hadoop-0.20.2/bin/hadoop-daemon.sh")
                start namenode))))
           (first
            (build-actions {}
              (hadoop-settings {})
              (hadoop-service
               "namenode"
               {:description "n" :if-stopped false :action :start}))))))
  (testing ":stop"
    (is (= (first
            (build-actions {:phase-context "hadoop-service"}
              (exec-checked-script
               (str "stop hadoop daemon: namenode")
               ((str "/usr/local/hadoop-0.20.2/bin/hadoop-daemon.sh")
                stop namenode))))
           (first
            (build-actions {}
              (hadoop-settings {})
              (hadoop-service "namenode" {:action :stop})))))))
