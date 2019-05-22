(ns performance-benchmark-framework.core-test
  (:require [clojure.test :refer :all]
            [performance-benchmark-framework.core :refer :all]))

(deftest generate-field-and-value-test
  (dotimes [size 10]
    (let [field (generate-field size)]
      (is (string? field))
      (is (= size (count field))))
    (let [value (generate-value size)]
      (is (string? value))
      (is (= size (count value))))))

(deftest rand-normal-test
  (let [min 0
        max 10]
    (dotimes [i 100]
      (let [n (rand-normal min max)]
        (is (float? n))
        (is (<= min n max))))))

(deftest rand-normal-int-test
  (let [min 0
        max 10]
    (dotimes [i 100]
      (let [n (rand-normal-int min max)]
        (is (int? n))
        (is (<= min n max))))))

(deftest generate-tokens-test
  (dorun
   (for [number-of-tokens (range 10)
         available-size (range 10)]
     (generate-tokens generate-field number-of-tokens available-size))))

(generate-tokens generate-value 40 40 {:unique? true})
