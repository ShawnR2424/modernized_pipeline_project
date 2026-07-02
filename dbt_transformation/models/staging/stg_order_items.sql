select
    order_id,
    product_id,
    order_item_id,
    product_price
from {{ source("raw_data", "order_items") }}