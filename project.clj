(defproject hadoop-crate "0.1.0-SNAPSHOT"
  :description "Crate for hadoop installation"
  :url "http://github.com/palletops/hadoop-crate"
  :license {:name "All rights reserved"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/data.xml "0.0.6"]
                 [org.cloudhoist/pallet "0.8.0-alpha.3"]]
  :profiles {:dev {:dependencies [[org.cloudhoist/pallet "0.8.0-alpha.3"
                                   :classifier "tests"]]}}
  :repositories
  {"sonatype-snapshots" "https://oss.sonatype.org/content/repositories/snapshots"
   "sonatype" "https://oss.sonatype.org/content/repositories/releases/"})
