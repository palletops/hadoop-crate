(defproject com.palletops/hadoop-crate "0.1.6"
  :description "Crate for hadoop installation"
  :url "http://github.com/palletops/hadoop-crate"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :scm {:url "git@github.com:palletops/hadoop-crate.git"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/data.xml "0.0.6"]
                 [com.palletops/pallet "0.8.0-RC.9"]
                 [com.palletops/java-crate "0.8.0-beta.6"]
                 [com.palletops/locos "0.1.1"]
                 [pathetic "0.4.0"]]
  :test-selectors {:default (complement :live-test)
                   :live-test :live-test
                   :all (constantly true)})
