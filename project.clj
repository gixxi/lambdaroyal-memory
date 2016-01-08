(defproject org.clojars.gixxi/lambdaroyal-memory "0.3-SNAPSHOT"
  :description "STM-based in-memory database storing persistent data structures"
  :url "https://github.com/gixxi/lambdaroyal-memory"
  :license {:name "GPL v3"
            :url "http://www.gnu.org/copyleft/gpl.html"}
  :dependencies [[org.clojure/clojure "1.8.0-RC4"]
                 [org.clojure/core.async "0.2.374"]
                 [clj-http "2.0.0"]
                 [com.ashafa/clutch "0.4.0"
                  :exclusions [clj-http]]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.1"]
                 [log4j "1.2.15" 
                  :exclusions [javax.mail/mail
                             javax.jms/jms
                             com.sun.jdmk/jmxtools
                             com.sun.jmx/jmxri]]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]]
                   :plugins [[lein-midje "3.1.3"]]}}
  :aot [lambdaroyal.memory.core lambdaroyal.memory.eviction.core lambdaroyal.memory.helper])
