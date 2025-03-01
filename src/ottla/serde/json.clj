(ns ottla.serde.json
  (:require [jsonista.core :as json]
            [ottla.serde.registry :refer [register-deserializer! register-serializer!]]))

(def data-type :json)

(defn serialize-json-text ^String [obj]
  (json/write-value-as-string obj json/keyword-keys-object-mapper))

(defn deserialize-text-json [^String value]
  (json/read-value value json/keyword-keys-object-mapper))

(register-serializer! data-type :text serialize-json-text)
(register-deserializer! data-type :text deserialize-text-json)

(def serialize-json-jsonb serialize-json-text)

;; these are handled by the PG driver
(defn deserialize-jsonb-json [value]
  value)

(register-serializer! data-type :jsonb serialize-json-jsonb)
(register-deserializer! data-type :jsonb deserialize-jsonb-json)

(defn serialize-json-bytea ^bytes [obj]
  (json/write-value-as-bytes obj json/keyword-keys-object-mapper))

(defn deserialize-bytea-json [^bytes value]
  (json/read-value value json/keyword-keys-object-mapper))

(register-serializer! data-type :bytea serialize-json-bytea)
(register-deserializer! data-type :bytea deserialize-bytea-json)
