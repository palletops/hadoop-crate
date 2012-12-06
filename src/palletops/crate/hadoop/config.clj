;;; Copyright 2012 Hugo Duncan.
;;; All rights reserved.

(ns palletops.crate.hadoop.config
  (:use
   [clojure.tools.logging :only [errorf]]
   [clojure.string :only [join]]
   [pallet.crate
    :only [defplan def-plan-fn assoc-settings get-settings defmethod-plan
           nodes-with-role target-node target]]
   [palletops.crate.hadoop.rules :only [defrules config]]
   [pallet.node :only [primary-ip hardware]]
   [pallet.script.lib :only [user-home]]
   [pallet.stevedore :only [script]]
   [pallet.version-dispatch
    :only [defmulti-version-plan defmethod-version-plan]])
  (:require
   [clojure.core.logic :as logic]))

;;; # Final Properties
(defprotocol FinalProperty
  "A protocol that provides a predicate for whether a property value is final"
  (final? [_] "Predicate for whether a property value is final"))

(extend-type Object FinalProperty (final? [_] false))

(deftype FinalPropertyValue [value]
  FinalProperty
  (final? [_] true)
  Object
  (toString [_] (str value)))

(defn final-value
  "Flag a property value as final"
  [x]
  (FinalPropertyValue. x))

;;; # Configuration properties
(defn user-file [user & components]
  (let [home (script (~user-home ~user))]
    (str home "/" (join "/" components))))

(defn config-set
  "Specify the config file for a property."
  [property]
  (let [n (name property)]
    (cond
     (.startsWith n "mapred.") :mapred-site
     (.startsWith n "tasktracker.") :mapred-site
     (.startsWith n "dfs.") :hdfs-site
     (.startsWith n "fs.") :core-site
     (.startsWith n "io.") :core-site
     (.startsWith n "hadoop.") :core-site
     (.startsWith n "pallet.") nil
     (.startsWith n "kernel.") nil
     :else (errorf "Failed to classify property %s" property))))

(defn config-for
  "Returns the settings for a specific hadoop component
   (:core-site, :mapred-site, hdfs-site)."
  [config component]
  (into {} (filter #(= component (config-set (key %))) config)))
