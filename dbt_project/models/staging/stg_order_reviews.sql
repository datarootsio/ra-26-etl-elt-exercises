select
    review_id,
    order_id,
    cast(review_score as int)               as review_score,
    cast(review_creation_date as timestamp)  as review_creation_ts,
    cast(review_answer_timestamp as timestamp) as review_answer_ts
from {{ source('bronze', 'order_reviews') }}
