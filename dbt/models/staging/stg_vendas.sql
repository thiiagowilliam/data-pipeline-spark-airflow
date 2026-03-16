-- dbt/models/staging/stg_vendas.sql

with source as (

    select * from {{ source('public', 'vendas_staging') }}

)

select
    id,
    customer_id,
    product,
    qty,
    price,
    order_date
from source
