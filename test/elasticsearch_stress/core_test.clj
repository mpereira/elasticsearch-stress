(ns elasticsearch-stress.core-test
  (:require [clojure.test :refer :all]
            [elasticsearch-stress.generators :refer :all]
            [elasticsearch-stress.statistics :refer :all]))

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
   (for [[generator iterations] [[generate-field 27] [generate-value 41]]
         unique? [false true]
         number-of-tokens (range iterations)
         i (range iterations)]
     (let [available-size (* (+ i 1) number-of-tokens)
           {:keys [error] :as tokens} (generate-tokens generator
                                                       number-of-tokens
                                                       available-size
                                                       {:unique? unique?})
           message {:generator generator
                    :unique? unique?
                    :number-of-tokens number-of-tokens
                    :available-size available-size}]
       (is (= nil error)
           message)
       (is (= number-of-tokens (count tokens))
           message)
       (is (= available-size (count (apply str tokens)))
           message)))))

;; (deftest generate-fields-test
;;   (dorun
;;    (for [document-size (range 1000)]
;;      (let [available-size (* (+ i 1) number-of-tokens)
;;            {:keys [error] :as tokens} (generate-tokens generator
;;                                                        number-of-tokens
;;                                                        available-size
;;                                                        {:unique? unique?})
;;            message {:generator generator
;;                     :unique? unique?
;;                     :number-of-tokens number-of-tokens
;;                     :available-size available-size}]
;;        (is (= nil error)
;;            message)
;;        (is (= number-of-tokens (count tokens))
;;            message)
;;        (is (= available-size (count (apply str tokens)))
;;            message)))))
