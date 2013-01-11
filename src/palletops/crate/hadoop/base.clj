(ns palletops.crate.hadoop.base
  "Base abstraction for hadoop crates"
  (:use
   [clojure.data.xml :only [element indent-str]]
   [clojure.string :only [join upper-case] :as string]
   [clojure.tools.logging :only [debugf tracef]]
   [pallet.action :only [with-action-options]]
   [pallet.actions
    :only [directory exec-checked-script exec-script remote-directory
           remote-file symbolic-link user group assoc-settings update-settings
           plan-when]
    :rename {user user-action group group-action
             assoc-settings assoc-settings-action
             update-settings update-settings-action}]
   [pallet.api :only [plan-fn server-spec]]
   [pallet.compute :only [service-properties]]
   [pallet.config-file.format :only [name-values]]
   [pallet.crate
    :only [defplan assoc-settings update-settings
           defmethod-plan get-settings
           get-node-settings group-name nodes-with-role target-id
           role->nodes-map target target-name
           compute-service]]
   [pallet.crate-install :only [install]]
   [pallet.crate.etc-default :only [write] :rename {write write-etc}]
   [pallet.crate.etc-hosts :only [hosts hosts-for-role] :as etc-hosts]
   [pallet.crate.java :only [java-home]]
   [pallet.map-merge :only [merge-key merge-keys]]
   [pallet.node :only [primary-ip private-ip hostname]]
   [pallet.script.lib :only [pid-root log-root config-root user-home]]
   [pallet.stevedore :only [script]]
   [pallet.utils :only [apply-map]]
   [pallet.version-dispatch
    :only [defmulti-version-plan defmethod-version-plan]]
   [pallet.versions :only [as-version-vector version-string]]
   [palletops.crate.hadoop.config :only [config-for final?]]
   [palletops.locos :only [apply-productions deep-merge]]))

;;; # Settings
(defn default-settings []
  {:user "hadoop"
   :owner "hadoop"
   :group "hadoop"
   :dist :cloudera
   :version "0.20.2"
   :dist-urls {:apache "http://www.apache.org/dist/"
               :cloudera "http://archive.cloudera.com/"
               :mapr "http://package.mapr.com/releases/"}
   :namenode-role :namenode
   :data-dir "/tmp/namenode/data"
   :pid-dir (script (str (~pid-root) "/hadoop"))
   :log-dir (script (str (~log-root) "/hadoop"))
   :etc-config-dir (script (str (~config-root) "/hadoop"))})

(def ^{:doc "A sequence of all hadoop roles"}
  hadoop-roles
  [:namenode :jobtracker :datanode :tasktracker :secondary-namenode
   :balancer])

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

;;; At the moment we just have a single implementation of settings,
;;; but again, this is open-coded.
(defmulti-version-plan install-settings [version settings])

(defmulti install-dist
  "Install for a specific distribution"
  (fn [dist & _] dist))

(defmethod-version-plan
  install-settings {:os :linux}
  [os os-version version settings]
  (let [target (target)]
    (cond
     (:install-strategy settings) settings
     (:remote-directory settings) (assoc settings
                                    :install-strategy ::remote-directory)
     (:dist settings) (install-dist (:dist settings) target settings)

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
     (let [value (get-in config [:config role kw] ::not-found)]
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
     (let [value (get-in config [:config role kw] ::not-found)]
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

(def dist-rules (atom []))

(defn base-settings
  "Return the base settings given some specified initial settings, some
   defaults and a set of rules to apply."
  [initial defaults rules]
  (tracef "hadoop initial settings %s" initial)
  (tracef "hadoop defaults settings %s" defaults)
  (tracef "hadoop merged settings %s" (merge defaults initial))
  (tracef "hadoop rules %s" (vec rules))
  (apply-productions (merge defaults initial) rules))

(defplan role-maps
  "Provide maps from role to ips and to hostnames."
  []
  (let [role->nodes (role->nodes-map)]
    {:ips (into {}
                (map
                 (fn ips [[role nodes]]
                   [role
                    (vec (map #(or (private-ip (:node %))
                                   (primary-ip (:node %)))
                              nodes))])
                 role->nodes))
     :hostnames (into {}
                      (map
                       (fn hostnames [[role nodes]]
                         [role (vec (map #(hostname (:node %)) nodes))])
                       role->nodes))}))

;;; hadoop-settings will infer information based on the distribution being
;;; installed, and set paths, and urls, etc. It does not infer other hadoop
;;; configuration.
(defplan hadoop-settings
  "Settings for the hadoop crate.

`:dist`
: specifies the distribution (e.g. :apache, :cloudera, :mapr)

`:dist`
: specifies the distribution (e.g. :apache, :cloudera, :mapr)

You can also specify hadoop properties, and kernel.* properties.

kernel.* Properties
-------------------

`:kernel.fs.file-max`
: the maximum number of file descriptors.

`:kernel.vm.swapiness`
: the propensity for the os to swap application memory

`:kernel.vm.overcommit`
: controls overcommit in the os

`:kernel.vm.overcommit_ratio`
: controls the amount of memory overcommit "
  [{:keys [user owner group dist dist-urls metrics instance-id rules]
    :or {rules @dist-rules}
    :as settings}]
  (let [role-maps (role-maps)
        service (compute-service)
        service (when service (service-properties service))
        _ (tracef "service is %s" service)
        settings (base-settings
                  settings
                  (deep-merge
                   (when (= :aws-ec2 (:provider service))
                     {:fs.s3.awsAccessKeyId (:identity service)
                      :fs.s3.awsSecretAccessKey (:credential service)
                      :fs.s3n.awsAccessKeyId (:identity service)
                      :fs.s3n.awsSecretAccessKey (:credential service)})
                   (default-settings)
                   role-maps)
                  rules)
        _ (tracef "hadoop settings in %s" settings)
        _ (tracef "hadoop settings applied rules %s"
                  (-> settings meta :rules vec))
        settings (install-settings (:version settings) settings)

        env-vars (properties->env-vars settings)
        settings (-> settings
                     ;; (merge install-config settings)
                     ;; (update-in [:config] #(merge install-config %))
                     ;; (update-in [:metrics] #(merge metrics-config %))
                     (update-in
                      [:env-vars]
                      #(merge-with
                        env-var-merge
                        {:HADOOP_PID_DIR (:pid-dir settings)
                         :HADOOP_LOG_DIR (:log-dir settings)
                         :HADOOP_SSH_OPTS "-o StrictHostKeyChecking=no"
                         :HADOOP_OPTS "-Djava.net.preferIPv4Stack=true"}
                        %
                        env-vars)))]
    (tracef "hadoop settings out %s" settings)
    (update-settings
     :hadoop {:instance-id instance-id}
     (partial merge-keys merge-settings-algorithm)
     settings)))


   ;; install-config (install-config (:version settings) settings)
   ;; metrics-config (metrics-config (:version settings) settings)
   ;; home (m-result (or (:home settings) (default-home settings)))
   ;; settings (m-result (merge {:home home :config-dir (str home "/conf")}
   ;;                           settings))



(defplan hadoop-env
  "Add into the hadoop env shell settings"
  [kv-pairs & {:keys [instance-id] :as options}]
  (update-settings :hadoop options update-in [:env-vars] merge kv-pairs))

;;; # Install
(defmethod-plan install ::remote-directory
  [facility instance-id]
  (let [{:keys [owner group home url] :as settings}
        (get-settings facility {:instance-id instance-id})]
    (apply pallet.actions/remote-directory home
           (apply concat (merge {:owner owner :group group}
                                (:remote-directory settings))))))

(defplan install-hadoop
  "Install hadoop."
  [& {:keys [instance-id]}]
  (install :hadoop instance-id))

;;; ## User
(defplan hadoop-user
  "Create the hadoop user, setting java home and path"
  [{:keys [instance-id] :as options}]
  (let [{:keys [user group home]}
        (get-settings :hadoop {:instance-id instance-id})]
    (group-action group :system true)
    (user-action user :group group :system true :create-home true :shell :bash)
    (remote-file (script (str (~user-home user) "/.bash_profile"))
                 :owner user
                 :group group
                 :literal true
                 :content (script
                           (set! JAVA_HOME (~java-home))
                           (set! PATH (str @PATH ":" ~home "/bin"))))))

;;; # Property files

;;; ## Content generation
(defn property-xml
  "Return a representation of a hadoop property as XMl. The property shoud be a
map entry."
  [property]
  (element :property {}
           (element :name {} (-> property key name))
           (element :value {} (str (val property)))
           ;; (when (final? property) (element :final {} "true"))
           ))

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
(defplan create-directories
  "Creates the appropriate directories based on settings"
  [{:keys [instance-id] :as options}]
  (let [{:keys [owner group config-dir etc-config-dir] :as settings}
        (get-settings :hadoop options)]
    (map
     #(directory (get settings %) :owner owner :group group :mode "0755")
     [:pid-dir :log-dir :config-dir])
    (symbolic-link config-dir etc-config-dir)))

(defn hadoop-config-file
  "Helper to write config files"
  [{:keys [owner group config-dir] :as settings} filename file-source]
  (apply
   remote-file (str config-dir "/" filename)
   :owner owner :group group
   (apply concat file-source)))

(defplan settings-config-file
  "Write an XML config file based on settings."
  [config-key {:keys [instance-id]}]
  (let [{:keys [home user] :as settings}
        (get-settings :hadoop {:instance-id instance-id})
        config (config-for settings config-key)]
    (hadoop-config-file
     settings (str (name (or config-key "unspecified")) ".xml")
     {:content (configuration-xml config)
      :literal false})))

(def properties-file
  {:metrics "hadoop-metrics.properties"})

(defplan properties-config-file
  "Write an properties config file based on settings."
  [config-key {:keys [instance-id]}]
  (let [{:keys [home user] :as settings}
        (get-settings :hadoop {:instance-id instance-id})]
    (hadoop-config-file
     settings (properties-file config-key)
     {:content (name-values (get settings config-key) :separator "=")
      :literal false})))

(defn default-hadoop-env
  [{:keys [home log-dir pid-dir] :as settings}]
  {:HADOOP_PID_DIR pid-dir
   :HADOOP_LOG_DIR log-dir
   :HADOOP_SSH_OPTS "-o StrictHostKeyChecking=no"
   :HADOOP_OPTS  "-Djava.net.preferIPv4Stack=true"
   :JAVA_HOME (script (~java-home))})

(defplan env-file
  "Environment settings for the hadoop services"
  [{:keys [instance-id]}]
  (let [{:keys [config-dir env-vars] :as settings}
        (get-settings :hadoop {:instance-id instance-id})]
    (apply-map
     write-etc (str config-dir "/hadoop-env.sh")
     (merge (default-hadoop-env settings) env-vars))))

;;; # Hostnames
(defplan set-hostname
  "Set the hostname on a node"
  [node-name target-name]
  (etc-hosts/set-hostname node-name))

(defplan setup-etc-hosts
  "Adds the ip addresses and host names of all nodes in all the groups in
  `groups` to the `/etc/hosts` file in this node.
   :private-ip will use the private ip address of the nodes instead of the
       public one "
  [roles & {:keys [private-ip] :as options}]
  (doseq [role roles]
    (hosts-for-role role :private-ip private-ip))
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

(defplan hadoop-exec
  "Calls the hadoop script with the specified arguments."
  {:arglists '[[options? & args]]}
  [& args]
  (let [[args {:keys [instance-id]}] (if (or (map? (first args))
                                             (nil? (first args)))
                                       [(rest args) (first args)]
                                       [args])
        {:keys [home user] :as settings}
        (get-settings :hadoop {:instance-id instance-id})]
    (with-action-options {:sudo-user user}
      (exec-checked-script
       (str "hadoop " (join " " args))
       ~(hadoop-env-script settings)
       ~(hadoop-exec-script home args)))))

(defplan hadoop-mkdir
  "Make the specifed path in the hadoop filesystem."
  {:arglists '[[options? path]]}
  [& args]
  (let [[[path] {:keys [instance-id]}] (if (or (map? (first args))
                                               (nil? (first args)))
                                         [(rest args) (first args)]
                                         [args])
        {:keys [home user] :as settings}
        (get-settings :hadoop {:instance-id instance-id})]
    (with-action-options {:sudo-user user}
      (exec-checked-script
       (str "hadoop-mkdir " (join " " args))
       ~(hadoop-env-script settings)
       (when (not ~(hadoop-exec-script home ["fs" "-test" "-d" path]))
         ~(hadoop-exec-script home ["fs" "-mkdir" path]))))))

(defplan hadoop-rmdir
  "Remove the specifed path in the hadoop filesystem, if it exitst."
  {:arglists '[[options? path]]}
  [& args]
  (let [[[path] {:keys [instance-id]}] (if (or (map? (first args))
                                               (nil? (first args)))
                                         [(rest args) (first args)]
                                         [args])
        {:keys [home user] :as settings}
        (get-settings :hadoop {:instance-id instance-id})]
    (with-action-options {:sudo-user user}
      (exec-checked-script
       (str "hadoop-rmdir " (join " " args))
       ~(hadoop-env-script settings)
       (when ~(hadoop-exec-script home ["dfs" "-test" "-d" path])
         ~(hadoop-exec-script home ["dfs" "-rmr" path]))))))


;;; # Run hadoop daemons
(defplan hadoop-service
  "Calls the hadoop-daemon script for the specified daemon, if it isn't
already running."
  [daemon {:keys [action if-stopped description instance-id]
           :or {action :start}
           :as options}]
  (let [{:keys [home user] :as settings}
        (get-settings :hadoop {:instance-id instance-id})
        if-stopped (if (contains? options :if-stopped)
                     if-stopped
                     (= :start action))]
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
         ((str ~home "/bin/hadoop-daemon.sh") ~(name action) ~daemon))))))

(def config-file-for-role
  {:hdfs-node :hdfs-site
   :namenode :core-site
   :secondary-namenode :core-site
   :jobtracker :mapred-site
   :datanode :core-site
   :tasktracker :mapred-site})

(defmulti hadoop-server-spec
  "Return a server-spec for the given hadoop role and settings."
  (fn [role settings & {:keys [instance-id] :as opts}] role))

(defn hadoop-role-spec
  "Returns a server-spec implementing the specified Hadoop server."
  [settings-fn {:keys [instance-id] :as opts} role service-name
   service-description & extends]
  (server-spec
   :roles #{role}
   :extends (vec (map #(% settings-fn opts) extends))
   :phases
   {:settings (plan-fn
                (let [settings (settings-fn)]
                  (hadoop-settings settings)))
    :install (plan-fn
               (hadoop-user opts)
               (create-directories opts)
               (install-hadoop :instance-id instance-id))
    :configure (plan-fn
                 (let [config-kw (get config-file-for-role role)]
                   (plan-when config-kw
                     (settings-config-file config-kw opts))
                   (properties-config-file :metrics opts)
                   (env-file opts)))
    :run (plan-fn
           (hadoop-service
            service-name
            (merge {:description service-description} opts)))}))

(defmulti hadoop-role-ports
  "Returns ports used by the specified role"
  (fn [role] role))
