{:no-checkouts {:checkout-deps-shares ^{:replace true} []},
 :dev {:dependencies [[com.palletops/pallet-vmfest "0.4.0-alpha.1"]
                      [com.palletops/pallet "0.8.0-RC.9" :classifier "tests"]
                      [ch.qos.logback/logback-classic "1.0.0"]],
       :plugins [[lein-pallet-release "0.1.6"]],
       :pallet-release
       {:url "https://pbors:${GH_TOKEN}@github.com/palletops/hadoop-crate.git",
        :branch "master"}}}
