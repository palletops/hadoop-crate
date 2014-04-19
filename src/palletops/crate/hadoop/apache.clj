(ns palletops.crate.hadoop.apache
  "Apache specific support for the hadoop crate."
  (:use
   [palletops.crate.hadoop.base
    :only [dist-rules hadoop-role-ports install-dist url]]
   [palletops.locos :only [defrules apply-productions !_]]
   [pathetic.core :only [render-path]]))

(defrules apache-rules
  ^{:name :apache-version}
  [{:version  !_}
   {:version "0.20.2"}]

  ^{:name :apache-home}
  [{:dist :apache
    :version ?v
    :home !_}
   {:home (render-path [:root "usr" "local" (str "hadoop-" ?v)])}]

  ^{:name :apache-config-dir}
  [{:config-dir !_
    :dist :apache
    :home ?h}
   {:config-dir (render-path [?h "conf"])}])

(swap! dist-rules concat apache-rules)

(defmethod install-dist :apache
  [_ target settings]
  (let [[url md5-url] (url settings)]
    (assoc settings
      :install-strategy :palletops.crate.hadoop.base/remote-directory
      :remote-directory {:url url
                         ;; pallet doesn't like :md5-url to be nil
                         :md5-url (or md5-url "")})))

(defmethod hadoop-role-ports :namenode
  [role]
  {:external [50070]
   :internal [8020]})

(defmethod hadoop-role-ports :datanode
  [role]
  {:external [50075]
   :internal [50010 50020]})

(defmethod hadoop-role-ports :secondary-namenode
  [role]
  {:external [50090]
   :internal []})

(defmethod hadoop-role-ports :jobtracker
  [role]
  {:external [50030]
   :internal [8021]})

(defmethod hadoop-role-ports :tasktracker
  [role]
  {:external [50060]
   :internal [8021]})

(defmethod hadoop-role-ports :backup
  [role]
  {:external [50105]
   :internal [50100]})
