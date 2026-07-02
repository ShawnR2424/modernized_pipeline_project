select
    name,
    price,
    rating,
    category,
    collection,
    product_id,
    availability
from {{ source("raw_data", "products") }}