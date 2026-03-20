-- models/gold/datamart_resumo_vendas_cliente.sql

{{ config(
    materialized='table',
    schema='gold'
) }}

WITH vendas AS (
    SELECT * FROM {{ ref('ft_vendas') }}
),

clientes AS (
    SELECT * FROM {{ ref('cl_clientes') }}
),

vendas_por_cliente AS (
    SELECT
        id_cliente,
        COUNT(id_venda) AS quantidade_compras,
        SUM(valor) AS valor_total_comprado,
        MIN(data_venda) AS data_primeira_compra,
        MAX(data_venda) AS data_ultima_compra
    FROM vendas
    GROUP BY 1
),

final AS (
    SELECT
        c.id_cliente,
        c.nome_hash,
        c.email_hash,
        c.estado,
        c.cidade,
        COALESCE(vpc.quantidade_compras, 0) AS quantidade_compras,
        COALESCE(vpc.valor_total_comprado, 0) AS valor_total_comprado,
        vpc.data_primeira_compra,
        vpc.data_ultima_compra
    FROM clientes c
    LEFT JOIN vendas_por_cliente vpc ON c.id_cliente = vpc.id_cliente
)

SELECT * FROM final
