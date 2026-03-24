{{
    config(
        materialized='incremental',
        unique_key='cliente_id',
        partition_by={
            "field": "data_cadastro",
            "data_type": "date"
        }
    )
}}

with source as (
    select
        id as cliente_id,
        nome,
        email,
        telefone,
        cidade,
        estado,
        status,
        data_cadastro
    from {{ source('clientes_bronze', 'bronze') }}
    qualify row_number() over (partition by id order by data_cadastro desc) = 1
)

select * from source