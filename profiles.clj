{:dev
 {:dependencies [[org.cloudhoist/pallet-vmfest "0.3.0-alpha.3"]
                 [com.palletops/pallet "0.8.0-beta.8"
                  :classifier "tests"]
                 [ch.qos.logback/logback-classic "1.0.0"]]}
 :no-checkouts {:checkout-deps-shares ^:replace []} ; disable checkouts
 :release
 {:plugins [[lein-set-version "0.2.1"]]
  :set-version
  {:updates [{:path "README.md"
              :no-snapshot true
              :search-regex
              #"com.palletops/hadoop-crate \"\d+\.\d+\.\d+\""}]}}}
