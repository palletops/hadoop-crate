(ns palletops.crate.hadoop.ec2
  "EC2 defaults")

(def instance-types
  {:m1.small
   {:ram (* 1024 1.7)                   ; GiB memory
    :cpus 1        ; 1 EC2 Compute Unit (1 virtual core with 1 EC2 Compute Unit)
    :disks [{:size 160}]                ; GB instance storage
    :32-bit true
    :64-bit true
    :io :moderate
    :ebs-optimised false}

   ;; :m1.medium
   ;; {:ram (* 1024 3.75)                  ;  GiB memory
   ;;  :cpus 1          ; EC2 Compute Unit (1 virtual core with 2 EC2 Compute Unit)
   ;;  :disks [{:size 410}]                ; GB instance storage
   ;;  :32-bit true
   ;;  :64-bit true
   ;;  :io :moderate
   ;;  :ebs-optimised false}

   :m1.large
   {:ram (* 1024 7.5)                   ; GiB memory
    :cpus 2 ; 4 Compute Units (2 virtual cores with 2 EC2 Compute Units each)
    :disks [{:size 850}]                ; GB instance storage
    :64-bit true
    :io :high
    :ebs-optimised 500}                 ; Mbps

   :m1.xlarge
   {:ram (* 1024 15)                    ; GiB memory
    :cpus 4 ; 8 Compute Units (4 virtual cores with 2 EC2 Compute Units each)
    :disks [{:size 1690}]               ; GB instance storage
    :64-bit true
    :io :high
    :ebs-optimised 1000}                ; Mbps

   ;; :m3.xlarge
   ;; {:ram (* 1024 15)                    ;  GiB memory
   ;;  :cpus 4    ; 13 Compute Units (4 virtual cores with 3.25 Compute Units each)
   ;;  :disks []
   ;;  :64-bit true
   ;;  :io :moderate
   ;;  :ebs-optimised false}

   ;; :m3.2xlarge
   ;; {:ram (* 1024 30)                    ; GiB memory
   ;;  :cpus 8    ; 26 Compute Units (8 virtual cores with 3.25 Compute Units each)
   ;;  :disks []
   ;;  :64-bit true
   ;;  :io :high
   ;;  :ebs-optimised false}

   ;; :t1.micro
   ;; {:ram (/ 613 1024.0)  ; MiB memory
   ;;  :cpus 1              ; Up to 2 EC2 Compute Units (for short periodic bursts)
   ;;  :disks []
   ;;  :32-bit true
   ;;  :64-bit true
   ;;  :io :low
   ;;  :ebs-optimised false}

   :m2.xlarge
   {:ram (* 1024 17.1)                  ; GiB of memory
    :cpus 2   ; 6.5 Compute Units (2 virtual cores with 3.25 Compute Units each)
    :disks [{:size 420}]                ; GB of instance storage
    :64-bit true
    :io :moderate
    :ebs-optimised false}

   :m2.2xlarge
   {:ram (* 1024 34.2)                  ; GiB of memory
    :cpus 4    ; 13 Compute Units (4 virtual cores with 3.25 Compute Units each)
    :disks [{:size 850}]                ; GB of instance storage
    :64-bit true
    :io :high
    :ebs-optimised false}

   ;; :m2.4xlarge
   ;; {:ram (* 1024 68.4)                  ; GiB of memory
   ;;  :cpus 8    ; 26 Compute Units (8 virtual cores with 3.25 Compute Units each)
   ;;  :disks [{:size 1690}]               ; GB of instance storage
   ;;  :64-bit true
   ;;  :io :High
   ;;  :ebs-optimised 1000} ; Mbps

   :c1.medium
   {:ram (* 1024 1.7)                   ; GiB of memory
    :cpus 2      ; 5 Compute Units (2 virtual cores with 2.5 Compute Units each)
    :disks [{:size 350}]                ; GB of instance storage
    :32-bit true
    :64-bit true
    :io :moderate
    :ebs-optimised false}

   :c1.xlarge
   {:ram (* 1024 7)                     ; GiB of memory
    :cpus 8 ; 20 EC2 Compute Units (8 virtual cores with 2.5 Compute Units each)
    :disks [{:size 1690}]               ; GB of instance storage
    :64-bit true
    :io :high
    :ebs-optimised false}

   :cc1.4xlarge
   {:ram (* 1024 23)             ; GiB of memory
    :cpus [{:cores 4}{:cores 4}] ; 33.5 EC2 Compute Units (2 x Intel Xeon X5570,
                                        ; quad-core “Nehalem” architecture)
    :disks [{:size 1690}]               ; GB of instance storage
    :64-bit true
    :io :very-high                      ; (10 Gigabit Ethernet)
    :ebs-optimised false}

   :cc2.8xlarge
   {:ram (* 1024 60.5)           ; GiB of memory
    :cpus [{:cores 8}{:cores 8}] ; 88 EC2 Compute Units (2 x Intel Xeon E5-2670,
                                        ;eight-core "Sandy Bridge" architecture)
    :disks [{:size 3370}]               ; GB of instance storage
    :64-bit true
    :io :very-high                      ; (10 Gigabit Ethernet)
    :ebs-optimised false}

   :cg1.4xlarge
   {:ram (* 1024 22)             ; GiB of memory
    :cpus [{:cores 4}{:cores 4}] ; 33.5 EC2 Compute Units (2 x Intel Xeon X5570,
                                        ; quad-core “Nehalem” architecture)
                                        ; 2 x NVIDIA Tesla “Fermi” M2050 GPUs
    :disks [{:size 1690}]               ; GB of instance storage
    :64-bit true
    :io :very-high                      ; (10 Gigabit Ethernet)
    :ebs-optimised false}

   ;; :hi1.4xlarge
   ;; {:ram (* 1024 60.5)                ; GiB of memory
   ;;  :cpus [{:cores 16}]               ; 35 EC2 Compute Units (16 virtual cores*)
   ;;  :disks [{:size 1024}{:size 1024}] ; 2 SSD-based volumes each with 1024 GB
   ;;                                      ; of instance storage
   ;;  :64-bit true
   ;;  :io :very-high                      ; (10 Gigabit Ethernet)
   ;;  :ebs-optimised false}
   })

(def hadoop-defaults
  {:m1.small
   {:HADOOP_JOBTRACKER_HEAPSIZE 576
    :HADOOP_NAMENODE_HEAPSIZE 192
    :HADOOP_TASKTRACKER_HEAPSIZE 192
    :HADOOP_DATANODE_HEAPSIZE 96
    :mapred.child.java.opts "-Xmx288m"
    :mapred.tasktracker.map.tasks.maximum 2
    :mapred.tasktracker.reduce.tasks.maximum 1
    }
   :m1.large
   {:HADOOP_JOBTRACKER_HEAPSIZE  2304
    :HADOOP_NAMENODE_HEAPSIZE 768
    :HADOOP_TASKTRACKER_HEAPSIZE 384
    :HADOOP_DATANODE_HEAPSIZE 384
    :mapred.child.java.opts "-Xmx864m"
    :mapred.tasktracker.map.tasks.maximum 3
    :mapred.tasktracker.reduce.tasks.maximum 1}
   :m1.xlarge
   {:HADOOP_JOBTRACKER_HEAPSIZE 6912
    :HADOOP_NAMENODE_HEAPSIZE 2304
    :HADOOP_TASKTRACKER_HEAPSIZE 384
    :HADOOP_DATANODE_HEAPSIZE 384
    :mapred.child.java.opts "-Xmx768m"
    :mapred.tasktracker.map.tasks.maximum 8
    :mapred.tasktracker.reduce.tasks.maximum 3}
   :c1.medium
   {:HADOOP_JOBTRACKER_HEAPSIZE 576
    :HADOOP_NAMENODE_HEAPSIZE 192
    :HADOOP_TASKTRACKER_HEAPSIZE 192
    :HADOOP_DATANODE_HEAPSIZE 96
    :mapred.child.java.opts "-Xmx288m"
    :mapred.tasktracker.map.tasks.maximum 2
    :mapred.tasktracker.reduce.tasks.maximum 1}
   :c1.xlarge
   {:HADOOP_JOBTRACKER_HEAPSIZE 2304
    :HADOOP_NAMENODE_HEAPSIZE 768
    :HADOOP_TASKTRACKER_HEAPSIZE 384
    :HADOOP_DATANODE_HEAPSIZE 384
    :mapred.child.java.opts "-Xmx384m"
    :mapred.tasktracker.map.tasks.maximum 7
    :mapred.tasktracker.reduce.tasks.maximum 2}
   :m2.xlarge
   {:HADOOP_JOBTRACKER_HEAPSIZE 9216
    :HADOOP_NAMENODE_HEAPSIZE 3072
    :HADOOP_TASKTRACKER_HEAPSIZE 384
    :HADOOP_DATANODE_HEAPSIZE 384
    :mapred.child.java.opts "-Xmx2304m"
    :mapred.tasktracker.map.tasks.maximum 3
    :mapred.tasktracker.reduce.tasks.maximum 1}
   :m2.2xlarge
   {:HADOOP_JOBTRACKER_HEAPSIZE 18432
    :HADOOP_NAMENODE_HEAPSIZE 6144
    :HADOOP_TASKTRACKER_HEAPSIZE 384
    :HADOOP_DATANODE_HEAPSIZE 384
    :mapred.child.java.opts "-Xmx2688m"
    :mapred.tasktracker.map.tasks.maximum 6
    :mapred.tasktracker.reduce.tasks.maximum 2}
   :m2.4xlarge
   {:HADOOP_JOBTRACKER_HEAPSIZE 36864
    :HADOOP_NAMENODE_HEAPSIZE 12288
    :HADOOP_TASKTRACKER_HEAPSIZE 384
    :HADOOP_DATANODE_HEAPSIZE 384
    :mapred.child.java.opts "-Xmx2304m"
    :mapred.tasktracker.map.tasks.maximum 14
    :mapred.tasktracker.reduce.tasks.maximum 4}
   :cc1.4xlarge
   {:HADOOP_JOBTRACKER_HEAPSIZE 7680
    :HADOOP_NAMENODE_HEAPSIZE 3840
    :HADOOP_TASKTRACKER_HEAPSIZE 384
    :HADOOP_DATANODE_HEAPSIZE 384
    :mapred.child.java.opts "-Xmx912m"
    :mapred.tasktracker.map.tasks.maximum 12
    :mapred.tasktracker.reduce.tasks.maximum 3}
   :cc2.8xlarge
   {:HADOOP_JOBTRACKER_HEAPSIZE 30114
    :HADOOP_NAMENODE_HEAPSIZE 12288
    :HADOOP_TASKTRACKER_HEAPSIZE 384
    :HADOOP_DATANODE_HEAPSIZE 384
    :mapred.child.java.opts "-Xmx1536m"
    :mapred.tasktracker.map.tasks.maximum 24
    :mapred.tasktracker.reduce.tasks.maximum 6}

   :cg1.4xlarge
   {:HADOOP_JOBTRACKER_HEAPSIZE 7680
    :HADOOP_NAMENODE_HEAPSIZE 3840
    :HADOOP_TASKTRACKER_HEAPSIZE 384
    :HADOOP_DATANODE_HEAPSIZE 384
    :mapred.child.java.opts "-Xmx864m"
    :mapred.tasktracker.map.tasks.maximum 12
    :mapred.tasktracker.reduce.tasks.maximum 3}})

;;; HDFS Configuration (AMI 2.2)

{:dfs.block.size 134217728              ; (128 MB)
 :dfs.replication (fn [node-count]
                    (cond
                     (< node-count 4) 1 ; for clusters < four nodes
                     (< node-count 10) 2 ;2 for clusters < ten nodes
                     :else 3))}          ; for all other clusters


;;; MapRed configuration
;;; :mapred.tasktracker.map.tasks.maximum
;;; :mapred.tasktracker.reduce.tasks.maximum
;;; [mappers reducers]
(def mapred-config
  {:m1.small [2 1]
   :m1.large [3 1]
   :m1.xlarge [8 3]
   :c1.medium [2 1]
   :c1.xlarge [7 2]
   :m2.xlarge [3 1]
   :m2.2xlarge [6 2]
   :m2.4xlarge [14 4]
   :cc1.4xlarge [12 3]
   :cc2.8xlarge [24 6]
   :cg1.4xlarge [12 3]
   })

(let [types (keys instance-types)]
  (for [t types]
    {:t t
     :cpus (:cpus (t instance-types))
     :map (first (t mapred-config))
     :red (second (t mapred-config))
     :total (apply + (t mapred-config))}))


;; :mapred.map.tasks
;; :mapred.map.tasksperslot
;; :mapred.reduce.tasks
;; :mapred.reduce.tasksperslot

;; 1. mapred.map.tasks set by the Hadoop job
;; 2. mapred.map.tasks set in mapred-conf.xml on the master node
;; 3. mapred.map.tasksperslot if neither of those are defined

;; You can configure the amount of heap space for tasks as well as other JVM
;; options with the mapred.child.java.opts setting. Amazon EMR provides a
;; default -Xmx value in this spot, with the defaults per instance type shown in
;; the following table.
(def mapred.child.java.opts
 {:m1.small "-Xmx384m"
  :m1.large "-Xmx1152m"
  :m1.xlarge "-Xmx1024m"
  :c1.medium "-Xmx384m"
  :c1.xlarge "-Xmx512m"
  :m2.xlarge "-Xmx3072m"
  :m2.2xlarge "-Xmx3584m"
  :m2.4xlarge "-Xmx3072m"
  :cc1.4xlarge "-Xmx1216m"
  :cc2.8xlarge "-Xmx2048m"
  :cg1.4xlarge "-Xmx1152m"}
 )

:mapred.job.reuse.jvm.num.tasks 20

;; In a distributed environment, you are going to experience random delays, slow
;; hardware, failing hardware, and other problems that collectively slow down
;; your job flow. This is known as the stragglers problem. Hadoop has a feature
;; called speculative execution that can help mitigate this issue. As the job
;; flow progresses, some machines complete their tasks. Hadoop schedules tasks
;; on nodes that are free. Whichever task finishes first is the successful one
;; and the other tasks are killed. This feature can substantially cut down on
;; the run time of jobs. The general design of a mapreduce alogorithm is such
;; that the processing of map tasks is meant to be idempotent. If, however, you
;; are running a job where the task execution has side effects (for example, a
;; zero reducer job that calls an external resource) is it important to disable
;; speculative execution.

;; You can enable speculative execution for mappers and reducers
;; independently. By default, Amazon EMR enables it for mappers and reducers in
;; AMI 2.2. You can override these settings with a bootstrap action. For more
;; information on using bootstrap actions, refer to Bootstrap Actions (p. 85).

:mapred.reduce.tasks.speculative.execution false
