SELECT *

FROM {{ source('coffee_shop', 'orders') }}