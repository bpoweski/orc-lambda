(defproject orc-lambda "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.fzakaria/slf4j-timbre "0.3.5"]
                 [org.apache.orc/orc-core "1.3.3" :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.apache.orc/orc-tools "1.3.3"]
                 [amazonica "0.3.94" :exclusions [com.google.protobuf/protobuf-java joda-time com.google.guava/guava
                                                  org.apache.httpcomponents/httpclient
                                                  org.apache.httpcomponents/httpcore commons-codec]]
                 [cheshire "5.7.0"]
                 [uswitch/lambada "0.1.2"]
                 [org.xerial.snappy/snappy-java "1.1.4-M3"]
                 [clj-time "0.13.0"]
                 [org.clojure/core.unify "0.5.7"]
                 [org.clojure/tools.trace "0.7.9"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [mvxcvi/puget "1.0.1"]
                 [com.taoensso/timbre "4.8.0"]]
  :main orc-lambda.core
  :profiles {:uberjar {:aot :all}
             :dev {:resource-paths ["test-resources"]}})
