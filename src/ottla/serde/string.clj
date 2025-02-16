(ns ottla.serde.string
  (:require [pg.core :as pg]
            [ottla.serde.registry :refer [register-deserializer! register-serializer!]])
  (:import [java.nio.charset StandardCharsets]))

(def data-type :string)

(register-serializer! data-type :text identity)
(register-deserializer! data-type :text identity)

(defn serialize-string-jsonb [^String s]
  (pg/json-wrap s))

(register-serializer! data-type :jsonb serialize-string-jsonb)
(register-deserializer! data-type :jsonb str)

(defn serialize-string-bytea ^bytes [obj]
  (when obj
    (.getBytes (str obj) StandardCharsets/UTF_8)))

(defn deserialize-bytea-string ^String [^bytes value]
  (when value
    (String. value StandardCharsets/UTF_8)))

(register-serializer! data-type :bytea serialize-string-bytea)
(register-deserializer! data-type :bytea deserialize-bytea-string)
