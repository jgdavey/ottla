(ns ottla.serde.string
  (:import [java.nio.charset StandardCharsets]))

(defn serialize-string ^bytes [obj]
  (when obj
    (.getBytes (str obj) StandardCharsets/UTF_8)))

(defn deserialize-string [^bytes ba]
  (when ba
    (String. ba StandardCharsets/UTF_8)))
