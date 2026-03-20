{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source AS (
    SELECT
        id AS id_venda,
        cliente_id AS id_cliente,
        data_venda,
        valor,
        metodo_pagamento,
        status_entrega,
        _airbyte_emitted_at as data_inclusao
    FROM {{ source('bronze', 'vendas') }}
)

SELECT
    id_venda,
    id_cliente,
    data_venda,
    valor,
    metodo_pagamento,
    status_entrega,
    data_inclusao
FROM source
