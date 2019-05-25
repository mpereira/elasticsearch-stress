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
  (let [iterations 40]
    (dorun
     (for [generator [generate-field generate-value]
           unique? [false true]
           number-of-tokens (range iterations)
           i (range iterations)]
       (let [available-size (* (+ i 1) number-of-tokens)
             {:keys [error] :as tokens} (generate-tokens generator
                                                         number-of-tokens
                                                         available-size
                                                         {:unique? unique?})]
         (is (= nil error)
             {:generator generator
              :unique? unique?
              :number-of-tokens number-of-tokens
              :available-size available-size}))))))

(generate-value 100)

(generate-foo 10)

(generate-tokens generate-foo 5 4 {:unique? true})

(clojure.pprint/pprint
 (generate-tokens generate-field 27 300 {:unique? true}))
