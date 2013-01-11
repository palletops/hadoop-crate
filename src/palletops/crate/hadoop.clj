;;; Copyright 2012, 2013 Hugo Duncan.
;;; All rights reserved.

;;; TODO kernel and os limits

(ns palletops.crate.hadoop
  "A pallet crate for installing hadoop"
  (:use
   [clojure.data.xml :only [element indent-str]]
   [clojure.string :only [join upper-case] :as string]
   [clojure.tools.logging :only [debugf]]
   [pallet.action :only [with-action-options]]
   [pallet.actions
    :only [directory exec-checked-script exec-script remote-directory
           remote-file symbolic-link user group assoc-settings update-settings
           on-one-node]
    :rename {user user-action group group-action
             assoc-settings assoc-settings-action
             update-settings update-settings-action}]
   [pallet.api :only [plan-fn server-spec]]
   [pallet.argument :only [delayed]]
   [pallet.config-file.format :only [name-values]]
   [pallet.crate
    :only [defplan assoc-settings update-settings
           defmethod-plan get-settings
           get-node-settings group-name nodes-with-role target-id
           target target-name]]
   [pallet.crate.etc-default :only [write] :rename {write write-etc}]
   [pallet.crate.etc-hosts :only [hosts hosts-for-role] :as etc-hosts]
   [pallet.crate.java :only [java-home]]
   [pallet.crate.ssh-key :only [authorize-key generate-key public-key]]
   [pallet.crate-install :only [install]]
   [palletops.crate.hadoop.base
    :only [env-file hadoop-env-script hadoop-role-spec
           setup-etc-hosts settings-config-file]]
   [palletops.crate.hadoop.config :only [config-for final?]]
   [palletops.locos :only [apply-productions]]
   [pallet.map-merge :only [merge-key merge-keys]]
   [pallet.node :only [primary-ip private-ip hostname]]
   [pallet.script.lib :only [pid-root log-root config-root user-home]]
   [pallet.stevedore :only [script]]
   [pallet.utils :only [apply-map]]
   [pallet.version-dispatch
    :only [defmulti-version-plan defmethod-version-plan]]
   [pallet.versions :only [as-version-vector version-string]])
  (:require
   palletops.crate.hadoop.common
   palletops.crate.hadoop.apache
   palletops.crate.hadoop.cloudera
   palletops.crate.hadoop.mapr))


;;; this should go elsewhere

(defn alias-var
  "Create a var in the current namespace, with the given name, and the same root
   binding as the give var, including metadata."
  [name ^clojure.lang.Var var]
  (let [m (merge (meta var) (meta name))]
    (if (.hasRoot var)
      (intern *ns* (with-meta name m) @var)
      (intern *ns* (with-meta name m)))))

(defmacro defalias
  "Defines a new var as an alias for the given var, with the given name. Any
   metadata on the name is merged onto the var's metadata. The root binding of
   the"
  [name var]
  `(alias-var '~name  #'~var))

(defn alias-ns
  "Defines new vars in the current namespace to alias each of the public vars in
  the supplied namespace."
  [ns-sym & {:keys [only] :as options}]
  (require ns-sym)
  (doseq [[name var] (if only
                       (select-keys (ns-publics (the-ns ns-sym)) only)
                       (ns-publics (the-ns ns-sym)))]
    (alias-var name var)))

(alias-ns 'palletops.crate.hadoop.base
          :only '[hadoop-server-spec hadoop-exec hadoop-rmdir hadoop-mkdir
                  hadoop-settings hadoop-service install-hadoop
                  hadoop-role-ports])

;;; # Cluster Support
(defplan namenode-node
  "Return the IP of the namenode."
  [{:keys [instance-id]}]
  (let [[namenode] (nodes-with-role :namenode)]
    (:node namenode)))

(defplan job-tracker-node
  "Return the IP of the jobtracker."
  [{:keys [instance-id]}]
  (let [[jobtracker] (nodes-with-role :jobtracker)]
    (:node jobtracker)))



;;; ## SSH Access
(defplan install-ssh-key
  "Ensure a public key for accessing the node is available in the node's
  settings."
  [{:keys [instance-id] :as options}]
  (let [id (target-id)
        group (group-name)
        key-name (format "ph_%s_%s_key" group id)
        {:keys [user] :as settings} (get-settings :hadoop options)]
    (generate-key user :comment key-name)))

(defplan ssh-key
  "Make a public key for accessing the node is available in the node's
  settings."
  [{:keys [instance-id] :as options}]
  (let [{:keys [user] :as settings} (get-settings :hadoop options)
        key (public-key user)]
    (assoc-settings-action
     :hadoop (delayed [_] {:public-key @key}) :instance-id instance-id)))

(defplan authorize-node
  "Authorises a specific node for ssh access to the specified `user`."
  [node {:keys [instance-id] :as options}]
  (let [{:keys [user public-key] :as s}
        (get-node-settings node :hadoop options)]
    (authorize-key user public-key)))

(defplan authorize-role
  "Authorises all nodes with the specified role to access the current node."
  [role {:keys [instance-id] :as options}]
  (let [{:keys [user] :as settings} (get-settings :hadoop options)
        nodes (nodes-with-role role)]
    (doseq [node (map :node nodes)] (authorize-node node options))))

;;; # Jobs
(defn single-quote [s] (str "'" s "'"))

(defplan hadoop-jar
  "Runs a hadoop jar.

`:jar`
: Specifies a map of remote-file options for the location of the jar to run.

`:main`
: Specifies the jar main class to run

`:input`
: Specifies the input data location.

`:output`
: Specifies the output data location.

`:args`
: Specifies additional arguments
"
  [{:keys [jar input output main args]}]
  (let [{:keys [home] :as settings} (get-settings :hadoop {})
        filename (str (gensym "job") ".jar")]
    (with-action-options {:script-dir home}
      (on-one-node
       [:jobtracker]
       (apply-map remote-file filename jar)
       (apply
        hadoop-exec
        "jar" filename main
        (if input (single-quote input) "")
        (if output (single-quote output) "")
        args)))))

(defplan s3distcp-url
  "The s3distcp url"
  [{:keys [region version] :or {version "1.latest"}}]
  (format
   "https://%selasticmapreduce.s3.amazonaws.com/libs/s3distcp/%s/s3distcp.jar"
   (if region (str region ".") "")
   version))


;;; # HDFS
(defplan format-hdfs
  "Formats HDFS for the first time. If HDFS has already been formatted, does
  nothing. Unfortunately, hadoop returns non-zero if it has already been
  formatted, even if we tell it not to format, so it is difficult to check the
  exit code."
  [{:keys [force]} {:keys [instance-id]}]
  (let [{:keys [home user] :as settings}
        (get-settings :hadoop {:instance-id instance-id})
        name-dir (get-in settings [:hdfs-site :dfs.name.dir])]
    (with-action-options {:sudo-user user}
      (exec-script
       ~(hadoop-env-script settings)
       (if (or (not (file-exists? ~(str name-dir "/current/VERSION"))) ~force)
         (pipe
          (echo "Y")                    ; confirmation
          ((str ~home "/bin/hadoop") namenode -format)))
       pwd))))                          ; gratuitous command to suppress errors

(defplan initialise-hdfs
  [{:keys [instance-id] :as opts}]
  (let [{:keys [data-dir] :as settings}
        (get-settings :hadoop {:instance-id instance-id})]
    (hadoop-exec opts "dfsadmin" "-safemode" "wait")
    (hadoop-mkdir opts data-dir)
    (hadoop-exec opts "fs" "-chmod" "+w" data-dir)))

;;; # server-specs
(defplan local-dirs
  "Create the local directories referred to by the settings."
  [{:keys [instance-id] :as opts}]
  (let [{:keys [owner group] :as settings} (get-settings :hadoop opts)]
    (doseq [dir (map
                 #(get-in settings %)
                 [[:dfs.data.dir]
                  [:dfs.name.dir]
                  [:fs.checkpoint.dir]
                  [:mapred.local.dir]
                  [:mapred.system.dir]
                  [:log-dir]
                  [:pid-dir]])]
      (directory (str dir) :owner owner :group group :recursive false))))

(defn hdfs-node
  "A hdfs server-spec. Settings as for hadoop-settings."
  [settings-fn {:keys [instance-id] :as opts}]
  (server-spec
   :roles #{:datanode}
   :phases
   {:settings (plan-fn
                (let [settings (settings-fn)]
                  (hadoop-settings settings)))
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
   :phases {:install (plan-fn (install-ssh-key opts))
            :configure (plan-fn (ssh-key opts))
            :collect-ssh-keys (plan-fn (ssh-key opts))}))

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
