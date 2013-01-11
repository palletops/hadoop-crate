{:dev
 {:dependencies [[org.cloudhoist/pallet-vmfest "0.2.1"]
                 [org.cloudhoist/pallet "0.8.0-SNAPSHOT"
                  :classifier "tests"]
                 [ch.qos.logback/logback-classic "1.0.0"]]}
 :release
 {:plugins [[lein-set-version "0.2.1"]]
  :set-version
  {:updates [{:path "README.md"
              :no-snapshot true
              :search-regex
              #"com.palletops/hadoop-crate \"\d+\.\d+\.\d+\""}]}}}
