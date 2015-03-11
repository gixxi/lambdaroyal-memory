(defproject lambdaroyal-typed "0.1.0-SNAPSHOT"
  :description "STM-based in-memory database storing persistent data structures"
  :url "https://github.com/gixxi/lambdaroyal-memory"
  :license {:name "GPL v3"
            :url "http://www.gnu.org/copyleft/gpl.html"}
  :dependencies [[org.clojure/clojure "1.7.0-alpha5"]
                 [org.clojure/core.typed "0.2.77"]
                 [com.ashafa/clutch "0.4.0"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]]}}
  :aot [lambdaroyal.memory.core lambdaroyal.memory.eviction.core])
