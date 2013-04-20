(ns palletops.crate.hadoop.common
  "Common configuration across distributions"
  (:require
   [clojure.string :refer [join]]
   [pallet.script.lib :refer [user-home]]
   [pallet.stevedore :refer [script with-source-line-comments]]
   [palletops.crate.hadoop.base :refer [dist-rules]]
   [palletops.locos :refer [!_ defrules]]))

(defn user-file [user & components]
  (let [home (with-source-line-comments false
               (script (~user-home ~user)))]
    (str home "/" (join "/" components))))

(def default-metrics
  {:dfs.class "org.apache.hadoop.metrics.spi.NoEmitMetricsContext"
    :dfs.period 10
    :mapred.class "org.apache.hadoop.metrics.spi.NoEmitMetricsContext"
    :mapred.period 10
    :jvm.class "org.apache.hadoop.metrics.spi.NoEmitMetricsContext"
    :jvm.period 10
    :ugi.class "org.apache.hadoop.metrics.spi.NoEmitMetricsContext"
    :ugi.period 10
    :fairscheduler.class "org.apache.hadoop.metrics.spi.NoEmitMetricsContext"
    :fairscheduler.period 10})

(defrules common-rules
  ^{:name :fs-checkoint-dir}
  [{:fs.checkpoint.dir !_
    :owner ?o}
   {:fs.checkpoint.dir (user-file ?o "/dfs/secondary")}]

  ^{:name :fs-default-name}
  [{:fs.default.name !_
    :ips {:namenode ?n}}
   {:fs.default.name (format "hdfs://%s:8020" (first ?n))}]

  ^{:name :mapred-job-tracker}
  [{:fs.default.name !_
    :ips {:jobtracker ?j}}
   {:mapred.job.tracker (format "%s:8021" (first ?j))}]

  ^{:name :hadoop-tmp}
  [{:hadoop.tmp.dir !_}
   {:hadoop.tmp.dir "/tmp/hadoop"}]

  ^{:name :dfs-data-dir}
  [{:dfs.data.dir !_
    :owner ?o}
   {:dfs.data.dir (user-file ?o "dfs/data")}]

  ^{:name :dfs-name-dir}
  [{:dfs.name.dir !_
    :owner ?o}
   {:dfs.name.dir (user-file ?o "dfs/name")}]

  ^{:name :mapred-local-dir}
  [{:mapred.local.dir !_
    :owner ?o}
   {:mapred.local.dir (user-file ?o "mapred/local")}]

  ^{:name :mapred-system-dir}
  [{:mapred.system.dir !_
    :owner ?o}
   {:mapred.system.dir (user-file ?o "mapred/system")}]

  ^{:name :metrics}
  [{:metrics !_}
   default-metrics]

  ^{:name :merge-metrics}
  [{:metrics ?m}
   (merge default-metrics ?m)])

(swap! dist-rules concat common-rules)
