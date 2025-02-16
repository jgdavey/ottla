(ns ottla.serde.edn
  (:require [clojure.edn :as edn]
            [ottla.serde.registry :refer [register-deserializer! register-serializer!]])
  (:import [java.nio.charset StandardCharsets]
           [java.io PushbackReader
                    InputStreamReader
            ByteArrayInputStream]))

(def data-type :edn)

(defn serialize-edn-bytea ^bytes [obj]
  (.getBytes ^String (pr-str obj) StandardCharsets/UTF_8))

(defn deserialize-bytea-edn [ba]
  (with-open [rdr (PushbackReader.
                   (InputStreamReader.
                    (ByteArrayInputStream. ^bytes ba)
                    StandardCharsets/UTF_8))]
    (edn/read rdr)))

(register-serializer! data-type :bytea serialize-edn-bytea)
(register-deserializer! data-type :bytea deserialize-bytea-edn)

(defn serialize-edn-text ^String [obj]
  (pr-str obj))

(defn deserialize-text-edn [^String value]
  (edn/read-string value))

(register-serializer! data-type :text serialize-edn-text)
(register-deserializer! data-type :text deserialize-text-edn)
