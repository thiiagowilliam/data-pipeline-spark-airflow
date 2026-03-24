{{
    config(
        materialized='table',
        cluster_by=['cliente_id', 'data_venda']
    )
}}

with vendas as (
    select * from {{ ref('ft_vendas') }}
),

clientes as (
    select * from {{ ref('cl_clientes') }}
)

select
    v.venda_id,
    c.cliente_id,
    c.nome as cliente_nome,
    c.cidade as cliente_cidade,
    c.estado as cliente_estado,
    v.produto_id,
    v.data_venda,
    v.valor_total,
    v.quantidade,
    v.metodo_pagto
from vendas v
left join clientes c on v.cliente_id = c.cliente_id