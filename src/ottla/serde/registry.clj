(ns ottla.serde.registry)

(def registry {})

(defn register-serializer! [data-type col-type f]
  (assert (ifn? f) "f must be a function")
  (assert (keyword? data-type))
  (assert (keyword? col-type))
  (alter-var-root #'registry assoc-in [:serializer data-type col-type] f))

(defn register-deserializer! [data-type col-type f]
  (assert (ifn? f) "f must be a function")
  (assert (keyword? data-type))
  (assert (keyword? col-type))
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

(defn- ensure-bytea [obj]
  (cond
    (nil? obj) obj
    (bytes? obj) obj
    :else (throw (ex-info
                  "Object not a byte array. Convert or provide a serialize function"
                  {:object obj}))))

(defn get-serializer!
  [data-type col-type]
  (if data-type
    (get! :serializer data-type col-type)
    (case col-type
      :jsonb identity       ;; let pg2 do its thing
      :text #(some-> % str) ;; ensure string
      :bytea ensure-bytea)))

(defn get-deserializer!
  [data-type col-type]
  (if data-type
    (get! :deserializer data-type col-type)
    identity ;; Assume the user knows what to do
    ))
