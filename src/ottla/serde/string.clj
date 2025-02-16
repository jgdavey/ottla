(ns ottla.serde.string
  (:import [java.nio.charset StandardCharsets]))

(defn serialize-string-text [obj]
  (when obj (str obj)))

(defn deserialize-text-string [^String value]
  value)

(defn serialize-string-bytea [obj]
  (when obj
    (.getBytes (str obj) StandardCharsets/UTF_8)))

(defn deserialize-bytea-string [^bytes value]
  (when value
    (String. value StandardCharsets/UTF_8)))
