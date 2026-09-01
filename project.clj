(defproject de.dixieflatline/paperboy "0.1.0-SNAPSHOT"
  :description "Reliable message passing to arbitrary brokers"
  :url "http://github.com/20centaurixfux/paperboy"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [de.dixieflatline/supervise "0.1.0-SNAPSHOT"]
                 [instaparse "1.5.0"]
                 [clojurewerkz/machine_head "1.0.0"]
                 [metosin/reitit "0.10.1"]]
  :profiles {:test {:dependencies [[io.moquette/moquette-broker "0.17"
                                    :exclusions [com.bugsnag/bugsnag]]]}})