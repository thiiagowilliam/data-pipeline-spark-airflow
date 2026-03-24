{{
    config(
        materialized='incremental',
        unique_key='venda_id',
        partition_by={
            "field": "data_venda",
            "data_type": "date"
        }
    )
}}

with source as (
    select
        id as venda_id,
        cliente_id,
        produto_id,
        data_venda,
        valor_total,
        quantidade,
        metodo_pagto
    from {{ source('vendas_bronze', 'bronze') }}
    qualify row_number() over (partition by id order by data_venda desc) = 1
)

select * from source