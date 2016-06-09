(defproject org.clojars.gixxi/lambdaroyal-memory "0.5.1-SNAPSHOT"
  :description "STM-based in-memory database storing persistent data structures"
  :url "https://github.com/gixxi/lambdaroyal-memory"
  :license {:name "FreeBSD License"
            :url "http://www.freebsd.org/copyright/freebsd-license.html"}
  :dependencies [[org.clojure/clojure "1.7.0-alpha5"]
                 [org.clojure/core.typed "0.3.0-alpha2"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
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
