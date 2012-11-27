;;; Copyright 2012 Hugo Duncan.
;;; All rights reserved.

;;; TODO kernel and os limits

(ns palletops.crate.hadoop
  "A pallet crate for installing hadoop"
  (:use
   [clojure.algo.monads :only [m-map]]
   [clojure.data.xml :only [element indent-str]]
   [clojure.string :only [join upper-case] :as string]
   [clojure.tools.logging :only [debugf]]
   [pallet.action :only [with-action-options]]
   [pallet.actions
    :only [directory exec-checked-script exec-script remote-directory
           remote-file symbolic-link user group assoc-settings update-settings]
    :rename {user user-action group group-action
             assoc-settings assoc-settings-action
             update-settings update-settings-action}]
   [pallet.api :only [plan-fn server-spec]]
   [pallet.config-file.format :only [name-values]]
   [pallet.crate
    :only [def-plan-fn assoc-settings update-settings
           defmethod-plan get-settings
           get-node-settings group-name nodes-with-role target-id
           target-name]]
   [pallet.crate.etc-default :only [write] :rename {write write-etc}]
   [pallet.crate.etc-hosts :only [hosts hosts-for-role] :as etc-hosts]
   [pallet.crate.java :only [java-home]]
   [pallet.crate.ssh-key :only [authorize-key generate-key public-key]]
   [pallet.crate-install :only [install]]
   [palletops.crate.hadoop.config
    :only [install-config metrics-config config-for final?]]
   [pallet.map-merge :only [merge-key merge-keys]]
   [pallet.node :only [primary-ip private-ip hostname]]
   [pallet.script.lib :only [pid-root log-root config-root user-home]]
   [pallet.stevedore :only [script]]
   [pallet.utils :only [apply-map]]
   [pallet.version-dispatch
    :only [defmulti-version-plan defmethod-version-plan]]
   [pallet.versions :only [as-version-vector version-string]]))


;;; # Cluster Support
(def-plan-fn namenode-node
  "Return the IP of the namenode."
  [{:keys [instance-id]}]
  [[namenode] (nodes-with-role :namenode)]
  (m-result (:node namenode)))

(def-plan-fn job-tracker-node
  "Return the IP of the jobtracker."
  [{:keys [instance-id]}]
  [[jobtracker] (nodes-with-role :jobtracker)]
  (m-result (:node jobtracker)))

;;; # Settings
(defn default-settings []
  {:version "0.20.2"
   :cloudera-version "3.0"
   :user "hadoop"
   :owner "hadoop"
   :group "hadoop"
   :dist :cloudera
   :dist-urls {:apache "http://www.apache.org/dist/"
              :cloudera "http://archive.cloudera.com/"}
   :namenode-role :namenode
   :data-dir "/tmp/namenode/data"
   :pid-dir (script (str (~pid-root) "/hadoop"))
   :log-dir (script (str (~log-root) "/hadoop"))
   :etc-config-dir (script (str (~config-root) "/hadoop"))})

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
  (fn [{:keys [dist]}] dist))

(defmethod url :apache
  [{:keys [version dist-urls]}]
  (let [url (format
             "%$1s/hadoop/core/hadoop-%$2s/hadoop-%$2s.tar.gz"
             (:apache dist-urls) version)]
    [url (str url ".md5")]))

(defmethod url :cloudera
  [{:keys [cloudera-version version dist-urls]}]
  (let [cdh-version (as-version-vector cloudera-version)
        major-version (first cdh-version)
        url (format
             "%s/cdh/%s/hadoop-%s-cdh%s.tar.gz"
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
     (:install-strategy settings) settings
     (:remote-directory settings) (assoc settings
                                    :install-strategy ::remote-directory)
     :else (let [[url md5-url] (url settings)]
             (assoc settings
               :install-strategy ::remote-directory
               :remote-directory {:url url :md5-url md5-url})))))

(defn env-var-merge
  [a b]
  (str a " " b))

(defmethod merge-key ::string-join
  [_ _ val-in-result val-in-latter]
  (merge-with
   #(if (.contains %1 %2)
      %1
      (str %1 " " %2))
   val-in-result val-in-latter))

(def
  ^{:doc "Map from key to merge algorithm. Specifies how specs are merged."}
  merge-settings-algorithm
  {:config :deep-merge
   :env-vars ::string-join})

(def ^{:doc "A sequence of all hadoop roles"}
  hadoop-roles
  [:namenode :jobtracker :datanode :tasktracker :secondary-namenode
   :balancer])

(def ^{:doc "A sequence of all hadoop roles"}
  hadoop-role-env-vars
  (->
   (into {}
         (map
          #(vector
            %
            (format
             "HADOOP_%s_OPTS"
             (upper-case (string/replace (name %) "-" ""))))
          hadoop-roles))))

(def java-system-properties
  {:jmx-authenticate "com.sun.management.jmxremote.authenticate"
   :jmx-password-file "com.sun.management.jmxremote.password.file"
   :jmx-port "com.sun.management.jmxremote.port"
   :jmx "com.sun.management.jmxremote"
   :jmx-ssl "com.sun.management.jmxremote.ssl"
   :jmx-ssl-registry "com.sun.management.jmxremote.registry.ssl"
   :jmx-ssl-client-auth "com.sun.management.jmxremote.ssl.need.client.auth"
   :key-store "javax.net.ssl.keyStore"
   :key-store-type "javax.net.ssl.keyStoreType"
   :key-store-password "javax.net.ssl.keyStorePassword"
   :trust-store "javax.net.ssl.trustStore"
   :trust-store-type "javax.net.ssl.trustStoreType"
   :trust-store-password "javax.net.ssl.trustStorePassword"})

(defn java-system-property
  "Return a java argument string to set the system properties specified in
   `options`."
  [property value]
  (format "-D%s%s" property (if (nil? value) "" (str "=" value))))

(def property-env-vars
  {:mx "-Xmx%sm"})

(defn env-var-update
  [v w]
  (str (if v (str v " ") "") w))

(defn role-properties->option-env-vars
  "Convert pallet properties to hadoop environment variables."
  [env-vars role config]
  (reduce
   (fn [env-vars [kw value-fmt]]
     (let [value (get-in config [:pallet role kw] ::not-found)]
       (if (= ::not-found value)
         env-vars
         (let [var (hadoop-role-env-vars role)
               val (format value-fmt value)]
           (update-in env-vars [var] env-var-update val)))))
   env-vars
   property-env-vars))

(defn role-properties->property-env-vars
  "Convert pallet properties to hadoop environment variables."
  [env-vars role config]
  (reduce
   (fn [env-vars [kw property]]
     (let [value (get-in config [:pallet role kw] ::not-found)]
       (if (= ::not-found value)
         env-vars
         (let [var (hadoop-role-env-vars role)]
           (update-in env-vars [var]
                      env-var-update (java-system-property property value))))))
   env-vars
   java-system-properties))

(defn properties->env-vars
 "Convert pallet properties to hadoop environment variables."
 [config]
 (reduce
  (fn [env-vars role]
    (->
     env-vars
     (role-properties->option-env-vars role config)
     (role-properties->property-env-vars role config)))
  {}
  hadoop-roles))

(def-plan-fn hadoop-settings
  "Settings for hadoop.

  `:config` takes a map of hadoop configuration properties to values. It can
            also contain pallet.* and kernel.* properties that are used as
            described below.

  pallet.* Properties:

    pallet.ROLE.mx sets the maximum java memory size for that role.
    pallet.ROLE.mx sets the maximum java memory size for that role.

  kernel.* Properties

    kernel.fs.file-max   the maximum number of file descriptors.
    kernel.vm.swapiness  the propensity for the os to swap application memory
    kernel.vm.overcommit        controls overcommit in the os
    kernel.vm.overcommit_ratio  controls the amount of memory overcommit
"
  [{:keys [user owner group dist dist-urls cloudera-version version
           config metrics instance-id]
    :as settings}]
  [settings (m-result (merge (default-settings) settings))
   home (m-result (or (:home settings) (default-home settings)))
   settings (m-result (merge {:home home :config-dir (str home "/conf")}
                             settings))
   settings (settings-map (:version settings) settings)
   namenode (namenode-node settings)
   jobtracker (job-tracker-node settings)
   settings (m-result
             (assoc settings
               :namenode-ip (or (private-ip namenode)
                                (primary-ip namenode))
               :jobtracker-ip (or (private-ip jobtracker)
                                  (primary-ip jobtracker))
               :namenode-hostname (hostname namenode)
               :jobtracker-hostname (hostname jobtracker)))
   install-config (install-config (:version settings) settings)
   metrics-config (metrics-config (:version settings) settings)
   env-vars (m-result (properties->env-vars config))
   settings (m-result
             (-> settings
                 (update-in [:config] #(merge install-config %))
                 (update-in [:metrics] #(merge metrics-config %))
                 (update-in
                  [:env-vars]
                  #(merge-with
                    env-var-merge
                    {:HADOOP_PID_DIR (:pid-dir settings)
                     :HADOOP_LOG_DIR (:log-dir settings)
                     :HADOOP_SSH_OPTS "-o StrictHostKeyChecking=no"
                     :HADOOP_OPTS "-Djava.net.preferIPv4Stack=true"}
                    %
                    env-vars))))]
  (update-settings
   :hadoop {:instance-id instance-id}
   (partial merge-keys merge-settings-algorithm) settings))

(def-plan-fn hadoop-env
  "Add into the hadoop env shell settings"
  [kv-pairs & {:keys [instance-id] :as options}]
  (update-settings :hadoop options update-in [:env-vars] merge kv-pairs))

;;; # Install
(defmethod-plan install ::remote-directory
  [facility instance-id]
  [{:keys [owner group home url] :as settings}
   (get-settings facility {:instance-id instance-id})]
  (apply pallet.actions/remote-directory home
         (apply concat (merge {:owner owner :group group}
                              (:remote-directory settings)))))

(def-plan-fn install-hadoop
  "Install hadoop."
  [& {:keys [instance-id]}]
  [settings (get-settings :hadoop {:instance-id instance-id})]
  (install :hadoop instance-id))

;;; ## User
(def-plan-fn hadoop-user
  "Create the hadoop user, setting java home and path"
  [{:keys [instance-id] :as options}]
  [{:keys [user group home]}
   (get-settings :hadoop {:instance-id instance-id})]
  (group-action group :system true)
  (user-action user :group group :system true :create-home true :shell :bash)
  (remote-file (script (str (~user-home user) "/.bash_profile"))
               :owner user
               :group group
               :literal true
               :content (script
                         (set! JAVA_HOME (~java-home))
                         (set! PATH (str @PATH ":" ~home "/bin")))))

;;; ## SSH Access
(def-plan-fn install-ssh-key
  "Ensure a public key for accessing the node is available in the node's
  settings."
  [{:keys [instance-id] :as options}]
  [id target-id
   group group-name
   key-name (m-result (format "ph_%s_%s_key" group id))
   {:keys [user] :as settings} (get-settings :hadoop options)]
  (generate-key user :comment key-name))

(def-plan-fn ssh-key
  "Make a public key for accessing the node is available in the node's
  settings."
  [{:keys [instance-id] :as options}]
  [{:keys [user] :as settings} (get-settings :hadoop options)
   key (public-key user)]
  (assoc-settings-action :hadoop {:public-key @key} :instance-id instance-id))

(def-plan-fn authorize-node
  "Authorises a specific node for ssh access to the specified `user`."
  [node {:keys [instance-id] :as options}]
  [{:keys [user public-key] :as s} (get-node-settings node :hadoop options)]
  (authorize-key user public-key))

(def-plan-fn authorize-role
  "Authorises all nodes with the specified role to access the current node."
  [role {:keys [instance-id] :as options}]
  [{:keys [user] :as settings} (get-settings :hadoop options)
   nodes (nodes-with-role role)]
  (m-map #(authorize-node % options) (map :node nodes)))


;;; # Property files

;;; ## Content generation
(defn property-xml
  "Return a representation of a hadoop property as XMl. The property shoud be a
map entry."
  [property]
  (element :property {}
           (element :name {} (-> property key name))
           (element :value {} (str (val property)))
           (when (final? property) (element :final {} "true"))))

(defn properties-xml
  "Represent a map of property values as xml. final is a map that should map a
  property to true to have it marked final."
  [properties]
  (element
   :configuration {} (map property-xml properties)))

(defn configuration-xml
  "Returns a string with configuration XML for the given properties and final
  map."
  [properties]
  (indent-str (properties-xml properties)))

;;; ## Write Files
(def-plan-fn create-directories
  "Creates the appropriate directories based on settings"
  [{:keys [instance-id] :as options}]
  [{:keys [owner group] :as settings} (get-settings :hadoop options)]
  (m-map
   #(directory (get settings %) :owner owner :group group :mode "0755")
   [:pid-dir :log-dir :config-dir])
  (symbolic-link (:config-dir settings) (:etc-config-dir settings)))

(def-plan-fn hadoop-config-file
  "Helper to write config files"
  [{:keys [owner group config-dir] :as settings} filename file-source]
  (apply
   remote-file (str config-dir "/" filename)
   :owner owner :group group
   (apply concat file-source)))

(def-plan-fn settings-config-file
  "Write an XML config file based on settings."
  [config-key {:keys [instance-id]}]
  [{:keys [home user config] :as settings}
   (get-settings :hadoop {:instance-id instance-id})
   config (m-result (config-for config config-key))]
  (hadoop-config-file
   settings (str (name config-key) ".xml")
   {:content (configuration-xml config)
    :literal false}))

(def properties-file
  {:metrics "hadoop-metrics.properties"})

(def-plan-fn properties-config-file
  "Write an properties config file based on settings."
  [config-key {:keys [instance-id]}]
  [{:keys [home user] :as settings}
   (get-settings :hadoop {:instance-id instance-id})]
  (hadoop-config-file
   settings (properties-file config-key)
   {:content (name-values (get settings config-key) :separator "=")
    :literal false}))

(defn default-hadoop-env
  [{:keys [home log-dir pid-dir] :as settings}]
  {:HADOOP_PID_DIR pid-dir
   :HADOOP_LOG_DIR log-dir
   :HADOOP_SSH_OPTS "-o StrictHostKeyChecking=no"
   :HADOOP_OPTS  "-Djava.net.preferIPv4Stack=true"
   :JAVA_HOME (script (~java-home))})

(def-plan-fn env-file
  "Environment settings for the hadoop services"
  [{:keys [instance-id]}]
  [{:keys [config-dir env-vars] :as settings}
   (get-settings :hadoop {:instance-id instance-id})]
  (apply-map
   write-etc (str config-dir "/hadoop-env.sh")
   (merge (default-hadoop-env settings) env-vars)))

;;; # Hostnames
(def-plan-fn set-hostname
  "Set the hostname on a node"
  [node-name target-name]
  (etc-hosts/set-hostname node-name))

(def-plan-fn setup-etc-hosts
  "Adds the ip addresses and host names of all nodes in all the groups in
  `groups` to the `/etc/hosts` file in this node.
   :private-ip will use the private ip address of the nodes instead of the
       public one "
  [roles & {:keys [private-ip] :as options}]
  (m-map #(hosts-for-role % :private-ip private-ip) roles)
  hosts)


;;; # Run hadoop
(defn hadoop-env-script
  [{:keys [home log-dir pid-dir] :as settings}]
  (script
   ("export" (set! PATH (str @PATH ":" ~home)))))

(defn hadoop-exec-script
  "Returns script for the specified hadoop command"
  [home args]
  (script ((str ~home "/bin/hadoop") ~@args)))

(def-plan-fn hadoop-exec
  "Calls the hadoop script with the specified arguments."
  {:arglists '[[options? & args]]}
  [& args]
  [[args {:keys [instance-id]}] (m-result
                                 (if (or (map? (first args))
                                         (nil? (first args)))
                                   [(rest args) (first args)]
                                   [args]))
   {:keys [home user] :as settings}
   (get-settings :hadoop {:instance-id instance-id})]
  (with-action-options {:sudo-user user}
    (exec-checked-script
     (str "hadoop " (join " " args))
     ~(hadoop-env-script settings)
     ~(hadoop-exec-script home args))))

(def-plan-fn hadoop-mkdir
  "Make the specifed path in the hadoop filesystem."
  {:arglists '[[options? path]]}
  [& args]
  [[[path] {:keys [instance-id]}] (m-result
                                 (if (or (map? (first args))
                                         (nil? (first args)))
                                   [(rest args) (first args)]
                                   [args]))
   {:keys [home user] :as settings}
   (get-settings :hadoop {:instance-id instance-id})]
  (with-action-options {:sudo-user user}
    (exec-checked-script
     (str "hadoop-mkdir " (join " " args))
     ~(hadoop-env-script settings)
     (when (not ~(hadoop-exec-script home ["fs" "-test" "-d" path]))
       ~(hadoop-exec-script home ["fs" "-mkdir" path])))))

(def-plan-fn hadoop-rmdir
  "Remove the specifed path in the hadoop filesystem, if it exitst."
  {:arglists '[[options? path]]}
  [& args]
  [[[path] {:keys [instance-id]}] (m-result
                                 (if (or (map? (first args))
                                         (nil? (first args)))
                                   [(rest args) (first args)]
                                   [args]))
   {:keys [home user] :as settings}
   (get-settings :hadoop {:instance-id instance-id})]
  (with-action-options {:sudo-user user}
    (exec-checked-script
     (str "hadoop-rmdir " (join " " args))
     ~(hadoop-env-script settings)
     (when ~(hadoop-exec-script home ["dfs" "-test" "-d" path])
       ~(hadoop-exec-script home ["dfs" "-rmr" path])))))


;;; # Run hadoop daemons
(def-plan-fn hadoop-service
  "Calls the hadoop-daemon script for the specified daemon, if it isn't
already running."
  [daemon {:keys [action if-stopped description instance-id]
           :or {action :start}
           :as options}]
  [{:keys [home user] :as settings}
   (get-settings :hadoop {:instance-id instance-id})
   if-stopped (m-result
               (if (contains? options :if-stopped)
                 if-stopped
                 (= :start action)))]
  (if if-stopped
    (with-action-options {:sudo-user user}
      (exec-checked-script
       (str (name action) " hadoop daemon: "
            (if description description daemon))
       ~(hadoop-env-script settings)
       (if-not (pipe (jps) (grep "-i" ~daemon))
         ((str ~home "/bin/hadoop-daemon.sh") ~(name action) ~daemon))))
    (with-action-options {:sudo-user user}
      (exec-checked-script
       (str (name action) " hadoop daemon: "
            (if description description daemon))
       ~(hadoop-env-script settings)
       ((str ~home "/bin/hadoop-daemon.sh") ~(name action) ~daemon)))))

;;; # HDFS
(def-plan-fn format-hdfs
  "Formats HDFS for the first time. If HDFS has already been formatted, does
  nothing. Unfortunately, hadoop returns non-zero if it has already been
  formatted, even if we tell it not to format, so it is difficult to check the
  exit code."
  [{:keys [force]} {:keys [instance-id]}]
  [{:keys [home user] :as settings}
   (get-settings :hadoop {:instance-id instance-id})
   name-dir (m-result (get-in settings [:hdfs-site :dfs.name.dir]))]
  (with-action-options {:sudo-user user}
    (exec-script
     ~(hadoop-env-script settings)
     (if (or (not (file-exists? ~(str name-dir "/current/VERSION"))) ~force)
       (pipe
        (echo "Y") ; confirmation
        ((str ~home "/bin/hadoop") namenode -format)))
     pwd))) ; gratuitous command to suppress errors

(def-plan-fn initialise-hdfs
  [{:keys [instance-id] :as opts}]
  [{:keys [data-dir] :as settings}
   (get-settings :hadoop {:instance-id instance-id})]
  (hadoop-exec opts "dfsadmin" "-safemode" "wait")
  (hadoop-mkdir opts data-dir)
  (hadoop-exec opts "fs" "-chmod" "+w" data-dir))

;;; # server-specs
(def-plan-fn local-dirs
  "Create the local directories referred to by the settings."
  [{:keys [instance-id] :as opts}]
  [{:keys [owner group] :as settings} (get-settings :hadoop opts)]
  (m-map
   #(directory (str %) :owner owner :group group :recursive false)
   (map
    #(get-in settings %)
    [[:config :dfs.data.dir]
     [:config :dfs.name.dir]
     [:config :fs.checkpoint.dir]
     [:config :mapred.local.dir]
     [:config :mapred.system.dir]])))

(defn hdfs-node
  "A hdfs server-spec. Settings as for hadoop-settings."
  [settings-fn {:keys [instance-id] :as opts}]
  (server-spec
   :roles #{:datanode}
   :phases
   {:settings (plan-fn
               [settings settings-fn]
               (hadoop-settings settings))
    :install (plan-fn
              (local-dirs opts))
    :configure (plan-fn
                "hdfs-node-configure"
                (setup-etc-hosts
                 [:hdfs-node :namenode :secondary-namenode :jobtracker
                  :datanode :tasktracker])
                (settings-config-file :hdfs-site opts)
                (env-file opts))}))

(defn enable-jobtracker-ssh-spec
  "Returns a server-spec that authorises the jobtracker for ssh."
  [{:keys [instance-id] :as opts}]
  (server-spec
   :phases {:configure (plan-fn (authorize-role :jobtracker opts))}))

(def config-file-for-role
  {:hdfs-node :hdfs-site
   :namenode :core-site
   :secondary-namenode :core-site
   :jobtracker :mapred-site
   :datanode :core-site
   :tasktracker :mapred-site})

(defn hadoop-role-spec
  "Returns a server-spec implementing the specified Hadoop server."
  [settings-fn {:keys [instance-id] :as opts} role service-name
   service-description & extends]
  (server-spec
   :roles #{role}
   :extends (vec (map #(% settings-fn opts) extends))
   :phases
   {:settings (plan-fn
               [settings settings-fn]
               (hadoop-settings settings))
    :install (plan-fn
              (hadoop-user opts)
              (create-directories opts)
              (install-hadoop :instance-id instance-id))
    :configure (plan-fn
                (settings-config-file (get config-file-for-role role) opts)
                (properties-config-file :metrics opts)
                (env-file opts))
    :run (plan-fn
          (hadoop-service
           service-name
           (merge {:description service-description} opts)))}))

(defmulti hadoop-server-spec
  "Return a server-spec for the given hadoop role and settings."
  (fn [role settings & {:keys [instance-id] :as opts}] role))

;; A namenode server-spec. Settings as for hadoop-settings.
(defmethod hadoop-server-spec :namenode
  [_ settings-fn & {:keys [instance-id] :as opts}]
  (server-spec
   :extends [(hadoop-role-spec
              settings-fn opts :namenode "namenode" "Name Node" hdfs-node)]
   :phases {:configure (plan-fn "format-hdfs" (format-hdfs {} opts))
            :init (plan-fn (initialise-hdfs opts))}))

;; A secondary namenode server-spec. Settings as for hadoop-settings.
(defmethod hadoop-server-spec :secondary-namenode
  [_ settings & {:keys [instance-id] :as opts}]
  (hadoop-role-spec
   settings opts
   :secondary-namenode "secondary-namenode" "Secondary Name Node"))

;; Returns a job tracker server-spec. Settings as for hadoop-settings.
(defmethod hadoop-server-spec :jobtracker
  [_ settings & {:keys [instance-id] :as opts}]
  (server-spec
   :extends [(hadoop-role-spec
              settings opts :jobtracker "jobtracker" "Job Tracker")]
   :phases {:install (install-ssh-key opts)
            :configure (ssh-key opts)
            :collect-ssh-keys (ssh-key opts)}))

;; A data node server-spec. Settings as for hadoop-settings.
(defmethod hadoop-server-spec :datanode
  [_ settings & {:keys [instance-id] :as opts}]
  (hadoop-role-spec
   settings opts :datanode "datanode" "Data Node" hdfs-node))

;; A task tracker server-spec. Settings as for hadoop-settings.
(defmethod hadoop-server-spec :tasktracker
  [_ settings & {:keys [instance-id] :as opts}]
  (server-spec
   :extends [(enable-jobtracker-ssh-spec opts)
             (hadoop-role-spec
              settings opts :tasktracker "tasktracker" "Task Tracker")]))
