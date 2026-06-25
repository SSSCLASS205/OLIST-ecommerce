-- Fails if a review was somehow "answered" before it was created.
SELECT
    review_id,
    review_creation_date,
    review_answer_timestamp
FROM {{ ref('order_reviews_silver') }}
WHERE review_answer_timestamp IS NOT NULL
  AND review_answer_timestamp < review_creation_date
