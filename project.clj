(defproject org.clojars.gixxi/lambdaroyal-memory "0.2-SNAPSHOT"
  :description "STM-based in-memory database storing persistent data structures"
  :url "https://github.com/gixxi/lambdaroyal-memory"
  :license {:name "GPL v3"
            :url "http://www.gnu.org/copyleft/gpl.html"}
  :dependencies [[org.clojure/clojure "1.7.0-alpha5"]
                 [org.clojure/core.typed "0.2.77"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.ashafa/clutch "0.4.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.1"]
                 [log4j "1.2.15" 
                  :exclusions [javax.mail/mail
                             javax.jms/jms
                             com.sun.jdmk/jmxtools
                             com.sun.jmx/jmxri]]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]]}}
  :aot [lambdaroyal.memory.core lambdaroyal.memory.eviction.core lambdaroyal.memory.helper])
