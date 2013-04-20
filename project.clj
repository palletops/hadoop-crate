(defproject com.palletops/hadoop-crate "0.1.4-SNAPSHOT"
  :description "Crate for hadoop installation"
  :url "http://github.com/palletops/hadoop-crate"
  :license {:name "All rights reserved"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/data.xml "0.0.6"]
                 [com.palletops/pallet "0.8.0-beta.8"]
                 [com.palletops/java-crate "0.8.0-beta.4"]
                 [palletops/locos "0.1.0"]
                 [pathetic "0.4.0"]]
  :test-selectors {:default (complement :live-test)
                   :live-test :live-test
                   :all (constantly true)}
  :repositories
  {"sonatype-snapshots" "https://oss.sonatype.org/content/repositories/snapshots"
   "sonatype" "https://oss.sonatype.org/content/repositories/releases/"})
