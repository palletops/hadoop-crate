(ns palletops.crate.hadoop.cloudera
  "Cloudera specific support for the hadoop crate."
  (:use
   [clojure.string :only [join] :as string]
   [pallet.versions :only [as-version-vector version-string]]
   [palletops.crate.hadoop.base :only [dist-rules install-dist url]]
   [palletops.locos :only [defrules apply-productions !_]]
   [pathetic.core :only [render-path]]))

(def cloudera-hadoop-version
  {"3.0" "0.20.2"
   "3.5" "0.20.2"})

;;; multiple versions of cloudera may correspond to a single hadoop version, so
;;; this map is not just the inverse of cloudera-hadoop-version
(def hadoop-cloudera-version
  {"0.20.2" "3.5"})

(defrules cloudera-rules
  ^{:name :cloudera-home}
  [{:dist :cloudera
    :version ?v}
   {:home (render-path [:root "usr" "local" (str "hadoop-" ?v)])}]

  ^{:name :cloudera-default-cloudera-version}
  [{:version !_
    :cloudera-version !_}
   {:cloudera-version "3.5"}]

  ^{:name :cloudera-default-cloudera-version}
  [{:version ?v
    :cloudera-version !_}
   {:cloudera-version (hadoop-cloudera-version ?v)}]

  ^{:name :cloudera-version}
  [{:version  !_
    :cloudera-version ?mv}
   {:version (cloudera-hadoop-version ?mv)}]

  ^{:name :cloudera-config-dir}
  [{:config-dir !_
    :dist :cloudera
    :home ?h}
   {:config-dir (render-path [?h "conf"])}])

(swap! dist-rules concat cloudera-rules)

(defmethod url :cloudera
  [{:keys [cloudera-version version dist-urls]}]
  (let [cdh-version (as-version-vector cloudera-version)
        major-version (first cdh-version)
        url (format
             "%scdh/%s/hadoop-%s-cdh%s.tar.gz"
             (:cloudera dist-urls)
             major-version
             version
             (join "u" cdh-version))]
    [url nil]))                         ; cloudera don't provide md5's :(

(defmethod install-dist :cloudera
  [_ target settings]
  (let [[url md5-url] (url settings)]
    (assoc settings
      :install-strategy :palletops.crate.hadoop.base/remote-directory
      :remote-directory {:url url
                         ;; pallet doesn-t like :md5-url to be nil
                         :md5-url (or md5-url "")})))
