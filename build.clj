(ns build
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.build.api :as b]
            [clojure.java.io :as io]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'com.joshuadavey/ottla)
(def version "0.1.0")
(def class-dir "target/classes")

(defn test
  "Run all the tests."
  [opts]
  (let [basis    (b/create-basis {:aliases [:test :run-tests]})
        cmds     (b/java-command
                  {:basis      basis
                   :main      'clojure.main
                   :main-args ["-m" "kaocha.runner"]})
        {:keys [exit]} (b/process cmds)]
    (when-not (zero? exit) (throw (ex-info "Tests failed" {}))))
  opts)

(defn- jar-opts [opts]
  (assoc opts
          :lib lib :version version
          :jar-file (format "target/%s-%s.jar" lib version)
          :scm {:tag (str "v" version)}
          :basis (b/create-basis {})
          :class-dir class-dir
          :target "target"
          :src-dirs ["src"]))

(defn build-jar
  [opts]
  (let [opts (jar-opts opts)]
    (b/delete {:path (:target opts)})
    (println "* Writing pom.xml")
    (b/write-pom opts)
    (println "* Copying source")
    (b/copy-dir {:src-dirs ["resources" "src"] :target-dir class-dir})
    (println (str "* Building JAR at " (:jar-file opts)))
    (b/jar opts)))

(defn ci
  "Run the CI pipeline of tests (and build the JAR)."
  [opts]
  (test opts)
  (build-jar opts)
  opts)

(defn install
  "Install the JAR locally."
  [opts]
  (let [opts (jar-opts opts)]
    (b/install opts))
  opts)

(defn deploy
  "Deploy the JAR to Clojars."
  [opts]
  (let [{:keys [jar-file] :as opts} (jar-opts opts)]
    (when-not (.exists (io/file jar-file))
      (println "* Running build-jar")
      (build-jar opts))
    (dd/deploy {:installer :remote :artifact (b/resolve-path jar-file)
                :pom-file (b/pom-path (select-keys opts [:lib :class-dir]))}))
  opts)
