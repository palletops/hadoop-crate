;;; Copyright 2012 Hugo Duncan.
;;; All rights reserved.

(ns palletops.crate.hadoop.config
  (:use
   [clojure.string :only [join]]
   [pallet.crate
    :only [defplan def-plan-fn assoc-settings get-settings defmethod-plan
           nodes-with-role]]
   [pallet.node :only [primary-ip]]
   [pallet.script.lib :only [user-home]]
   [pallet.stevedore :only [script]]
   [pallet.version-dispatch
    :only [defmulti-version-plan defmethod-version-plan]]))

;;; # Configuration properties
(defn user-file [user & components]
  (let [home (script (~user-home ~user))]
    (str home "/" (join "/" components))))

;;; # Settings
;;; These need to use the node cpu, disk and memory details

;;; ## Core settings
(defmulti-version-plan core-settings [version settings])

(defmethod-version-plan
    core-settings {:os :linux :version [[0 20 0] [0 20 99]]}
    [os os-version version {:keys [owner name-node-ip] :as settings}]
  (m-result
   {:fs.checkpoint.dir (user-file owner "/dfs/secondary")
    :fs.default.name (format "hdfs://%s:8020" name-node-ip)
    :fs.trash.interval 1440
    :io.file.buffer.size 65536
    :hadoop.tmp.dir "/tmp/hadoop"
    :hadoop.rpc.socket.factory.class.default
    "org.apache.hadoop.net.StandardSocketFactory"
    :hadoop.rpc.socket.factory.class.ClientProtocol ""
    :hadoop.rpc.socket.factory.class.JobSubmissionProtocol ""
    :io.compression.codecs (str
                            "org.apache.hadoop.io.compress.DefaultCodec,"
                            "org.apache.hadoop.io.compress.GzipCodec")}))

;;; ## HDFS settings
(defmulti-version-plan hdfs-settings [version settings])

(defmethod-version-plan
    hdfs-settings {:os :linux :version [[0 20 0] [0 20 99]]}
    [os os-version version {:keys [owner] :as settings}]
  (m-result
   {:dfs.data.dir (user-file owner "dfs/data")
    :dfs.name.dir (user-file owner "dfs/name")
    :dfs.datanode.du.reserved 1073741824
    :dfs.namenode.handler.count 10
    :dfs.permissions.enabled true
    :dfs.replication 3
    :dfs.datanode.max.xcievers 4096}))

;;; ## MapRed settings
(defmulti-version-plan mapred-settings [version settings])

(defmethod-version-plan
    mapred-settings {:os :linux :version [[0 20 0] [0 20 99]]}
  [os os-version version {:keys [owner job-tracker-ip] :as settings}]
  (m-result
   {:tasktracker.http.threads 46
    :mapred.local.dir (user-file owner "mapred/local")
    :mapred.system.dir "/hadoop/mapred/system"
    :mapred.child.java.opts "-Xmx550m"
    :mapred.job.tracker (format "%s:8021" job-tracker-ip)
    :mapred.job.tracker.handler.count 10
    :mapred.map.tasks.speculative.execution true
    :mapred.reduce.tasks.speculative.execution false
    :mapred.reduce.parallel.copies 10
    :mapred.reduce.tasks 5
    :mapred.submit.replication 10
    :mapred.tasktracker.map.tasks.maximum 2
    :mapred.tasktracker.reduce.tasks.maximum 1
    :mapred.compress.map.output true
    :mapred.output.compression.type "BLOCK"}))



;; ;; second pass
;; core-settings default-core-settings
;; hdfs-settings default-hdfs-settings
;; mapred-settings default-mapred-settings

;; (assoc-settings
;;  :hadoop
;;  (-> settings
;;      (update-in [:core-settings] #(merge core-settings %))
;;      (update-in [:hdfs-settings] #(merge hdfs-settings %))
;;      (update-in [:mapred-settings] #(merge mapred-settings %)))
;;  {:instance-id instance-id})

 ;; [pallet.script.lib :only [user-home]]
 ;; [pallet.stevedore :only [script]]
