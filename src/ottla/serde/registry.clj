(ns ottla.serde.registry)

(defonce registry {})

(defn register-serializer! [data-type col-type f]
  (alter-var-root #'registry assoc-in [:serializer data-type col-type] f))

(defn register-deserializer! [data-type col-type f]
  (alter-var-root #'registry assoc-in [:deserializer data-type col-type] f))

(defn- get!
  [serde data-type col-type]
  (cond
    (keyword? data-type)
    (or
     (get-in registry [serde data-type col-type])
     (throw (UnsupportedOperationException.
             (format "No known %s for %s/%s" (name serde) data-type col-type))))

    (ifn? data-type)
    data-type

    :else
    (throw (IllegalArgumentException. "data-type must be keyword or function"))))

(defn get-serializer!
  [data-type col-type]
  (get! :serializer data-type col-type))

(defn get-deserializer!
  [data-type col-type]
  (get! :deserializer data-type col-type))
