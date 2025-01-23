(ns ottla.serde.json
  (:require [jsonista.core :as json])
  (:import [com.fasterxml.jackson.databind ObjectMapper]))

(defn make-json-serde [^ObjectMapper mapper]
  {:serialize #(json/write-value-as-bytes % mapper)
   :deserialize (fn* [ba] (json/read-value ^bytes ba mapper))})

(let [{:keys [serialize deserialize]} (make-json-serde json/keyword-keys-object-mapper)]
  (def serialize-json serialize)
  (def deserialize-json deserialize))
