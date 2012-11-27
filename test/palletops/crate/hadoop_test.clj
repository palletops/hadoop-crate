(ns palletops.crate.hadoop-test
  (:use
   [pallet.action :only [with-action-options]]
   [pallet.actions
    :only [directory exec-checked-script remote-file remote-file-content]]
   [pallet.algo.fsmop :only [complete?]]
   [pallet.api :only [lift plan-fn group-spec server-spec]]
   [pallet.build-actions :only [build-actions]]
   [pallet.crate :only [def-plan-fn get-settings]]
   [pallet.crate.automated-admin-user :only [automated-admin-user]]
   [pallet.crate.java :only [java-settings install-java]]
   [pallet.live-test :only [images test-nodes]]
   [pallet.test-utils :only [make-node]]
   palletops.crate.hadoop
   clojure.test))

(def nn {:roles #{:name-node :job-tracker} :node (make-node "n")})

(deftest hadoop-settings-test
  (is
   (build-actions {:service-state [nn]}
     (hadoop-settings {}))))

(deftest hadoop-exec-test
  (is
   (build-actions {:service-state [nn]}
     (hadoop-settings {})
     (hadoop-exec "ls"))))

(deftest hadoop-service-test
  (testing "default action"
    (is (= (first
            (build-actions {:phase-context "hadoop-service"}
              (exec-checked-script
               (str "start hadoop daemon: namenode")
               ("export" "PATH=${PATH}:/usr/local/hadoop-0.20.2")
               (if-not (pipe (jps) (grep "-i" namenode))
                 ((str "/usr/local/hadoop-0.20.2/bin/hadoop-daemon.sh")
                  start namenode)))))
           (first
            (build-actions {:service-state [nn]}
              (hadoop-settings {})
              (hadoop-service "namenode" {}))))))
  (testing "if-stopped"
    (is (= (first
            (build-actions {:phase-context "hadoop-service"}
              (exec-checked-script
               (str "start hadoop daemon: n")
               ("export" "PATH=${PATH}:/usr/local/hadoop-0.20.2")
               ((str "/usr/local/hadoop-0.20.2/bin/hadoop-daemon.sh")
                start namenode))))
           (first
            (build-actions {:service-state [nn]}
              (hadoop-settings {})
              (hadoop-service
               "namenode"
               {:description "n" :if-stopped false :action :start}))))))
  (testing ":stop"
    (is (= (first
            (build-actions {:phase-context "hadoop-service"}
              (exec-checked-script
               ("export" "PATH=${PATH}:/usr/local/hadoop-0.20.2")
               (str "stop hadoop daemon: namenode")
               ((str "/usr/local/hadoop-0.20.2/bin/hadoop-daemon.sh")
                stop namenode))))
           (first
            (build-actions {:service-state [nn]}
              (hadoop-settings {})
              (hadoop-service "namenode" {:action :stop})))))))

(deftest install-hadoop-test
  (testing "install"
    (is
     (first
      (build-actions {:service-state [nn]}
                     (hadoop-settings {})
                     (install-hadoop))))))

(def book-examples
  ["pg132.txt" "pg972.txt" "pg1661.txt" "pg4300.txt" "pg5000.txt"
   "pg19699.txt" "pg20417.txt"])

(def book-dir "/tmp/books")
(def book-output-dir "/tmp/book-output")

(def-plan-fn download-books
  []
  [{:keys [owner group] :as settings} (get-settings :hadoop {})]
  (directory book-dir :owner owner :group group :mode "0755")
  (map
   #(remote-file
    (str book-dir "/" %)
    :url (str "https://hadoopbooks.s3.amazonaws.com/" %)
    :owner owner :group group :mode "0755")
   book-examples))

(def-plan-fn import-books-to-hdfs
  []
  (hadoop-exec "dfs" "-rmr" "books/")
  (hadoop-exec "dfs" "-copyFromLocal" book-dir "books/"))

(def-plan-fn run-books
  []
  [{:keys [home] :as settings} (get-settings :hadoop {})]
  (hadoop-exec "dfs" "-rmr" "books-output/")
  (with-action-options {:script-dir home}
    (hadoop-exec "jar" "hadoop-examples-*.jar" "wordcount"
                 "books/" "books-output/")))

(def-plan-fn get-books-output
  []
  [{:keys [owner group] :as settings} (get-settings :hadoop {})]
  (directory book-output-dir :owner owner :group group :mode "0755")
  (hadoop-exec "dfs" "-getmerge" "books-output" book-output-dir)
  [result (remote-file-content (str book-output-dir "/books-output"))])

(def java
  (server-spec
   :phases {:settings (java-settings {:vendor :openjdk})
            :install (install-java)}))

(deftest ^:live-test live-test
  (let [settings {}]
    (doseq [image (images)]
      (test-nodes
       [compute node-map node-types [:install
                                     :configure
                                     :install-test
                                     :configure-test]]
       {:namenode
        (group-spec
         "namenode"
         :image image
         :count 1
         :extends [java
                   (hadoop-server-spec :name-node settings)
                   (hadoop-server-spec :job-tracker settings)]
         :phases {:bootstrap (plan-fn (automated-admin-user))
                  :install-test (plan-fn (download-books))
                  :configure-test (plan-fn (import-books-to-hdfs))
                  :run-test (plan-fn (run-books))
                  :post-run (plan-fn (get-books-output))})
         :datanode
         (group-spec
          "datanode"
          :image image
          :count 1
          :extends [java
                    (hadoop-server-spec :data-node settings)
                    (hadoop-server-spec :task-tracker settings)]
          :phases {:bootstrap (plan-fn (automated-admin-user))})}
       (let [op (lift (:namenode node-types)
                      :phase [:run-test :post-run]
                      :compute compute)]
         @op
         (is (complete? op)))))))
