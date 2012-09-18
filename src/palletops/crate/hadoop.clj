;;; Copyright 2012 Hugo Duncan.
;;; All rights reserved.

(ns palletops.crate.hadoop
  "A pallet crate for installing hadoop"
  (:use
   [clojure.string :only [join]]
   [clojure.data.xml :only [element indent-str]]
   [pallet.action :only [with-action-options]]
   [pallet.actions :only [exec-checked-script remote-directory]]
   [pallet.crate
    :only [defplan def-plan-fn assoc-settings get-settings defmethod-plan
           nodes-with-role]]
   [pallet.crate-install :only [install]]
   [palletops.crate.hadoop.config
    :only [core-settings hdfs-settings mapred-settings]]
   [pallet.node :only [primary-ip]]
   [pallet.version-dispatch
    :only [defmulti-version-plan defmethod-version-plan]]
   [pallet.versions :only [as-version-vector version-string]]))


;;; # Cluster Support
(def-plan-fn name-node
  "Return the IP of the namenode."
  [{:keys [instance-id]}]
  [{:keys [name-node-role] :as settings}
   (get-settings :hadoop {:instance-id instance-id})
   [namenode] (nodes-with-role name-node-role)]
  namenode)

(def-plan-fn job-tracker
  "Return the IP of the jobtracker."
  [{:keys [instance-id]}]
  [{:keys [job-tracker-role] :as settings}
   (get-settings :hadoop {:instance-id instance-id})
   [jobtracker] (nodes-with-role job-tracker-role)]
  jobtracker)

;;; # Settings
(def default-settings
  {:version "0.20.2"
   :cloudera-version "3.0"
   :user "hadoop"
   :owner "hadoop"
   :group "hadoop"
   :flavour :cloudera
   :dist-urls {:apache "http://www.apache.org/dist/"
              :cloudera "http://archive.cloudera.com/"}
   :name-node-role :namenode})

;;; # Settings

;;; We wish to allow multiple versions of hadoop to be installed,
;;; so we version the default install root.
(defn default-home
  "Default install root for hadoop."
  [{:keys [version]}]
  (format "/usr/local/hadoop-%s" version))

;;; The download url is implemented as a multimethod so it is open-coded.
(defmulti url
  "URL for downloading Hadoop."
  (fn [{:keys [flavour]}] flavour))

(defmethod url :apache
  [{:keys [version dist-urls]}]
  (let [url (format
             "%$1s/hadoop/core/hadoop-%$2s/hadoop-%$2s.tar.gz"
             (:apache dist-urls) version)]
    [url (str url ".md5")]))

(defmethod url :cloudera
  [{:keys [cloudera-version version dist-urls]}]
  (let [cdh-version (as-version-vector version)
        major-version (first cdh-version)
        url (format
             "%s/cdh/%s/hadoop-%s-%s.tar.gz"
             (:cloudera dist-urls)
             major-version
             version
             (join "u" cdh-version))]
    [url nil]))                         ; cloudera don't provide md5's :(

;;; At the moment we just have a single implementation of settings,
;;; but again, this is open-coded.
(defmulti-version-plan settings-map [version settings])

(defmethod-version-plan
    settings-map {:os :linux}
    [os os-version version settings]
  (m-result
   (cond
     (:strategy settings) settings
     (:remote-directory settings) (assoc settings :strategy ::remote-directory)
     :else (let [[url md5-url] (url settings)]
             (assoc settings
               :strategy ::remote-directory
               :remote-directory {:url url :md5-url md5-url})))))

(def-plan-fn hadoop-settings
  "Settings for hadoop"
  [{:keys [user owner group flavour dist-urls cloudera-version version
           instance-id]
    :as settings}]
  [settings (m-result (merge default-settings settings))
   settings (m-result (merge {:home (default-home settings)} settings))
   settings (settings-map (:version settings) settings)
   namenode (name-node settings)
   jobtracker (job-tracker settings)
   settings (m-result (assoc settings
                        :name-node-ip (primary-ip namenode)
                        :job-tracker-ip (primary-ip jobtracker)))
   ;; second pass
   core-settings core-settings
   hdfs-settings hdfs-settings
   mapred-settings mapred-settings
   settings (m-result
             (-> settings
                 (update-in [:core-settings] #(merge core-settings %))
                 (update-in [:hdfs-settings] #(merge hdfs-settings %))
                 (update-in [:mapred-settings] #(merge mapred-settings %))))]
  (assoc-settings :hadoop settings {:instance-id instance-id}))

;;; # Install
(defmethod-plan install ::remote-directory
  [facility instance-id]
  [{:keys [owner group url md5-url] :as settings}
   (get-settings facility {:instance-id instance-id})]
  (apply remote-directory
         (apply concat (merge {:owner owner :group group}
                              (:remote-directory settings)))))

(def-plan-fn install-hadoop
  "Install hadoop."
  [& {:keys [instance-id]}]
  [settings (get-settings
             :hadoop {:instance-id instance-id :default ::no-settings})]
  (install :hadoop instance-id))

;;; # Property files
(defn property-xml
  "Return a representation of a hadoop property as XMl. The property shoud be a
map entry."
  [property final?]
  (element :property {}
           (element :name {} (-> property key name))
           (element :value {} (str (val property)))
           (when final? (element :final {} "true"))))

(defn properties-xml
  "Represent a map of property values as xml. final is a map that should map a
  property to true to have it marked final."
  [properties final]
  (element
   :configuration {} (map #(property-xml % (get final (key %))) properties)))

(defn configuration-xml
  "Returns a string with configuration XML for the given properties and final
  map."
  [properties final]
  (indent-str (properties-xml properties final)))

;;; # Run hadoop
(def-plan-fn hadoop-exec
  "Calls the hadoop script with the specified arguments."
  {:arglists '[[options? args]]}
  [& args]
  [[args {:keys [instance-id]}] (m-result
                                 (if (map? (first args))
                                   [(rest args) (first args)]
                                   [args]))
   {:keys [home user] :as settings}
   (get-settings :hadoop {:instance-id instance-id :default ::no-settings})]
  (with-action-options {:sudo-user user}
    (exec-checked-script
     (str "hadoop " (join " " args))
     ((str ~home "/bin/hadoop") ~@args))))

;;; # Run hadoop daemons
(def-plan-fn hadoop-service
  "Calls the hadoop-daemon script for the specified daemon, if it isn't
already running."
  [daemon {:keys [action if-stopped description instance-id]
           :or {action :start}
           :as options}]
  [{:keys [home user] :as settings}
   (get-settings :hadoop {:instance-id instance-id :default ::no-settings})
   if-stopped (m-result
               (if (contains? options :if-stopped)
                 if-stopped
                 (= :start action)))]
  (if if-stopped
    (with-action-options {:sudo-user user}
      (exec-checked-script
       (str (name action) " hadoop daemon: "
            (if description description daemon))
       (if-not (pipe (jps) (grep "-i" ~daemon))
         ((str ~home "/bin/hadoop-daemon.sh") ~(name action) ~daemon))))
    (with-action-options {:sudo-user user}
      (exec-checked-script
       (str (name action) " hadoop daemon: "
            (if description description daemon))
       ((str ~home "/bin/hadoop-daemon.sh") ~(name action) ~daemon)))))
