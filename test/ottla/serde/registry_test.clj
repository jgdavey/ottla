(ns ottla.serde.registry-test
  (:require [clojure.test :refer [deftest is testing]]
            [ottla.serde.registry :as reg]
            [ottla.serde.string]))

(deftest get-serializer-test
  (testing "allows lookup by data type keyword"
    (is (fn? (reg/get-serializer! :string :text))))
  (testing "returns the function when passed a non-keyword function"
    (let [yikes #(str %)]
      (is (= yikes (reg/get-serializer! yikes :text)))))
  (testing "throws when getting an unknown data type"
    (is (thrown? Exception (reg/get-serializer! :foo :bytea))))
  (testing "throws when passed data type is neither a function nor a keyword"
    (is (thrown? Exception (reg/get-serializer! 123 :bytea)))))
