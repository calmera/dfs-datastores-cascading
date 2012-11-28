(defproject backtype/dfs-datastores-cascading "1.2.2-SNAPSHOT"
  :description "Taps for using dfs-datastores with Cascading."
  :url "https://github.com/nathanmarz/dfs-datastores-cascading"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm" "test/jvm"]
  :hooks [leiningen.hooks.junit]
  :junit ["test/jvm"]
  :junit-options {:fork "off" :haltonfailure "on"}
  :repositories {"conjars" "http://conjars.org/repo"}
  :plugins [[lein-junit "1.0.3"]
            [lein-clojars "0.9.1"]]
  :dependencies [[backtype/dfs-datastores "1.2.0"]
                 [cascading/cascading-hadoop "2.0.2"
                  :exclusions [org.codehaus.janino/janino
                               org.apache.hadoop/hadoop-core]]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [junit "4.10"]]
  :profiles {:dev
             {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                             [junit "4.10"]]}})
