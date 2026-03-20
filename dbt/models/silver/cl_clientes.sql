{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source AS (
    SELECT
        id AS id_cliente,
        nome,
        email,
        telefone,
        estado,
        cidade,
        _airbyte_emitted_at as data_inclusao
    FROM {{ source('bronze', 'clientes') }}
)

SELECT
    id_cliente,
    SHA256(nome) as nome_hash,
    SHA256(email) as email_hash,
    {{ mask_partial('telefone', 2, 4) }} as telefone_mascarado,
    estado,
    cidade,
    data_inclusao
FROM source
