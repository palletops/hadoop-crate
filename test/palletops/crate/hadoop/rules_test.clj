(ns palletops.crate.hadoop.rules-test
  (:use
   palletops.crate.hadoop.rules
   clojure.test))

(defrules sizing
  [{:roles #{:name-node} :hardware {:ram ?r }}
   {:namenode-mx (* ?r 0.8)}]
  [{:roles #{:name-node :job-tracker} :hardware {:ram ?r }}
   {:namenode-mx (* ?r 0.4)
    :jobtracker-mx (* ?r 0.3)}]
  [{:roles #{:name-node :job-tracker} :hardware {:ram ?r }}
   {:namenode-mx (* ?r 0.5)}
   [> ?r 1024]]
  [{:roles #{:job-tracker} :hardware {:ram ?r }}
   {:jobtracker-mx (* ?r 0.8)}])

(deftest config-test
  (is (= {:namenode-mx (* 1024 0.8)}
         (config {:roles #{:name-node} :hardware {:ram 1024}} sizing)))
  (is (= {:jobtracker-mx (* 1024 0.3), :namenode-mx (* 1024 0.4)}
         (config
          {:roles #{:name-node :job-tracker} :hardware {:ram 1024}}
          sizing)))
  (is (= {:jobtracker-mx (* 1025 0.3), :namenode-mx (* 1025 0.5)}
         (config
          {:roles #{:name-node :job-tracker} :hardware {:ram 1025}}
          sizing)))
  (is (= {:jobtracker-mx (* 1024 0.8)}
         (config {:roles #{:job-tracker} :hardware {:ram 1024}} sizing))))
