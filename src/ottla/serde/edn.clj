(ns ottla.serde.edn
  (:require [clojure.edn :as edn])
  (:import [java.nio.charset StandardCharsets]
           [java.io PushbackReader
                    InputStreamReader
                    ByteArrayInputStream]))

(defn serialize-edn-bytea [obj]
  (.getBytes (pr-str obj) StandardCharsets/UTF_8))

(defn deserialize-bytea-edn [ba]
  (with-open [rdr (PushbackReader.
                   (InputStreamReader.
                    (ByteArrayInputStream. ^bytes ba)
                    StandardCharsets/UTF_8))]
    (edn/read rdr)))

(defn serialize-edn-text [obj]
  (pr-str obj))

(defn deserialize-text-edn [^String value]
  (edn/read-string value))
