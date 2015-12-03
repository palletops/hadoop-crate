(defproject com.palletops/hadoop-crate "0.1.9"
  :description "Crate for hadoop installation"
  :url "http://github.com/palletops/hadoop-crate"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/data.xml "0.0.6"]
                 [com.palletops/pallet "0.8.0"]
                 [com.palletops/net-rules-crate "0.8.0-alpha.9"
                  :exclusions [org.clojure/clojure]]
                 [com.palletops/java-crate "0.8.0-beta.6"]
                 [com.palletops/locos "0.1.3"]
                 [pathetic "0.5.1"]]
  :test-selectors {:default (complement :live-test)
                   :live-test :live-test
                   :all (constantly true)})
