(ns palletops.crate.hadoop.mapr
  "MapR specific support for the hadoop crate."
  (:use
   [clojure.string :only [join]]
   [pallet.actions
    :only [directory exec-checked-script exec-script package
           remote-directory
           remote-file symbolic-link user group assoc-settings update-settings
           service on-one-node]
    :rename {user user-action group group-action
             assoc-settings assoc-settings-action
             update-settings update-settings-action}]
   [pallet.api :only [plan-fn server-spec]]
   [pallet.crate
    :only [defplan assoc-settings update-settings
           defmethod-plan get-settings
           get-node-settings group-name nodes-with-role
           target target-id target target-name]]
   [pallet.crate.network-service :only [wait-for-port-listen]]
   [palletops.crate.hadoop.base
    :only [dist-rules hadoop-role-spec hadoop-server-spec install-dist url]]
   [palletops.locos :only [defrules apply-productions !_]]
   [pathetic.core :only [render-path]]))

;;; http://www.mapr.com/doc/display/MapR/Assigning+Services+to+Nodes+for+Best+Performance

(def mapr-hadoop-version
  "Map of the Hadoop version provided by each MapR version."
  {"2.1.0" "0.20.2"})

(defrules mapr-rules
  ^{:name :mapr-default-mapr-version}
  [{:dist :mapr
    :mapr-version !_}
   {:mapr-version "2.1.0"}]

  ^{:name :mapr-mapr-home}
  [{:dist :mapr
    :mapr-home !_}
   {:mapr-home (render-path [:root "opt" "mapr"])}]

  ^{:name :mapr-version}
  [{:version !_
    :mapr-version ?mv}
   {:version (mapr-hadoop-version ?mv)}]

  ^{:name :mapr-home}
  [{:home !_
    :mapr-home ?h
    :version ?v}
   {:home (render-path [?h "hadoop" (str "hadoop-" ?v)])}]

  ^{:name :mapr-config-dir}
  [{:config-dir !_
    :dist :mapr
    :home ?h}
   {:config-dir (render-path [?h "conf"])}]

  ^{:name :mapred-job-tracker}
  [{:dist :mapr}
   {:mapred.job.tracker "maprfs:///"}])

(swap! dist-rules concat mapr-rules)

;;; # MapR install functions

(defmethod url :mapr                    ; this is for the repo
  [{:keys [mapr-version version dist-urls]}]
  (let [url (format "%sv%s/" (:mapr dist-urls) mapr-version)]
    [url nil]))

(def mapr-role-packages
  {:tasktracker ["mapr-tasktracker"]
   :mapr/fileserver ["mapr-fileserver"]
   :jobtracker ["mapr-jobtracker"]
   :mapr/cldb ["mapr-cldb"]
   :mapr/webserver ["mapr-webserver"]
   :mapr/zookeeper ["mapr-zookeeper"]})

(defmethod install-dist :mapr
  [_ target settings]
  (let [[url md5-url] (url settings)]
    (assoc settings
      :install-strategy :package-source
      :package-source {:name "mapr"
                       :aptitude {:url (str url "ubuntu/")
                                  :release ""
                                  :scopes ["mapr" "optional"]}
                       :yum {:url (str url "redhat/")
                             :gpgcheck 0}}
      :package-options {:allow-unsigned true}
      :packages (reduce
                 (fn [packages role]
                   (concat packages (mapr-role-packages role)))
                 []
                 (:roles target)))))

;;; # MapR plan functions
(defplan configure-mapr-cluster
  [& {:keys [instance-id]}]
  (let [{:keys [mapr-home user ips]}
        (get-settings :hadoop {:instance-id instance-id})]
    (exec-checked-script
     "Configure the MapR cluster"
     ((str "MAPR_HOME=" ~mapr-home) (str "MAPR_USER=" ~user)
      (str ~mapr-home "/server/configure.sh")
      -C ~(join "," (ips :mapr/cldb))
      -Z ~(join "," (ips :mapr/zookeeper))
      "--isvm"))))

(defplan authorise-user
  [& {:keys [instance-id]}]
  (let [{:keys [mapr-home user]}
        (get-settings :hadoop {:instance-id instance-id})]
    (exec-checked-script
     "Authorise hadoop user on MapR cluster"
     ((str ~mapr-home "/bin/maprcli")
      acl edit -type cluster -user (str ~user ":fc")))))

;;; MapR roles
(defn mapr-role-spec []
  (server-spec
   :phases {:configure (plan-fn (configure-mapr-cluster))}))

(defmethod hadoop-server-spec :mapr/fileserver
  [_ settings-fn & {:keys [instance-id] :as opts}]
  (server-spec
   :extends [(hadoop-role-spec
              settings-fn opts
              :mapr/fileserver "mapr-fileserver" "File Server")
             (mapr-role-spec)]))

(defmethod hadoop-server-spec :mapr/cldb
  [_ settings-fn & {:keys [instance-id] :as opts}]
  (server-spec
   :extends [(hadoop-role-spec
              settings-fn opts :mapr/cldb "mapr-cldb" "CLDB")
             (mapr-role-spec)]
   :phases {:init (plan-fn
                   (on-one-node [:mapr/cldb :mapr/webserver]
                                (service "mapr-warden")
                                (authorise-user)))}))


(defmethod hadoop-server-spec :mapr/zookeeper
  [_ settings-fn & {:keys [instance-id] :as opts}]
  (server-spec
   :extends [(hadoop-role-spec
              settings-fn opts
              :mapr/zookeeper "mapr-zookeeper" "MapR Zookeeper")
             (mapr-role-spec)]
   :phases {:init (plan-fn
                   (service "mapr-zookeeper")
                   (wait-for-port-listen 5181))}))

(defmethod hadoop-server-spec :mapr/webserver
  [_ settings-fn & {:keys [instance-id] :as opts}]
  (server-spec
   :extends [(hadoop-role-spec
              settings-fn opts
              :mapr/webserver "mapr-webserver" "MapR WebServer")
             (mapr-role-spec)]
   :phases {:init (plan-fn
                   (on-one-node [:mapr/webserver :mapr/cldb]
                                (service "mapr-warden")))}))


;; Each node must be configured as follows:

;;     Each node have a unique hostname.
;;     SELinux must be disabled during the install procedure. If the MapR services run as a non-root user, SELinux can be enabled after installation and while the cluster is running.
;;     Each node must be able to perform forward and reverse hostname resolution with every other node in the cluster.
;;     MapR Administrative user - a Linux user chosen to have administrative privileges on the cluster.
;;         The MapR user must exist on each node, and the user name, user id (UID) and primary group id (GID) must match on all nodes.
;;         Make sure the user has a password (using sudo passwd <user> for example).
;;     Make sure the limit on the number of processes (NPROC_RLIMIT) is not set too low for the root user; the value should be at least 32786. In Red Hat or CentOS, the default may be very low (1024, for example). In Ubuntu, there may be no default; you should only set this value if you see errors related to inability to create new threads. Use the ulimit command to remove limits on file sizes or or other computing resources. Each node must have a number of available file descriptors greater than four times the number of nodes in the cluster. See ulimit for more detailed information.
;;     syslog must be enabled.

;; In VM environments like EC2, VMware, and Xen, when running Ubuntu 10.10, problems can occur due to an Ubuntu bug unless the IRQ balancer is turned off. On all nodes, edit the file /etc/default/irqbalance and set ENABLED=0 to turn off the IRQ balancer (requires reboot to take effect).
