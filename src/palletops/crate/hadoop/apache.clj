(ns palletops.crate.hadoop.apache
  "Apache specific support for the hadoop crate."
  (:use
   [palletops.crate.hadoop.base :only [dist-rules install-dist url]]
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
      :remote-directory {:url url :md5-url md5-url})))
