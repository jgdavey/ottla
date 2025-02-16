(ns ottla.serde.json
  (:require [jsonista.core :as json]
            [pg.core :as pg]))

(defn serialize-object-jsonb [obj]
  obj)

(defn serialize-object-text ^String [obj]
  (json/write-value-as-string obj json/keyword-keys-object-mapper))

(defn serialize-object-bytea ^bytes [obj]
  (json/write-value-as-bytes obj json/keyword-keys-object-mapper))

(defn deserialize-jsonb-object [value]
  value)

(defn deserialize-text-object [^String value]
  (json/read-value value json/keyword-keys-object-mapper))

(defn deserialize-bytea-object [^bytes value]
  (json/read-value value json/keyword-keys-object-mapper))
