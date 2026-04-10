{{
    config(
        materialized='table',
        cluster_by=['mes_venda', 'estado']
    )
}}

with base_vendas as (
    select * from {{ ref('datamart_resumo_vendas_cliente') }}
)

select
    date_trunc(data_venda, month) as mes_venda,
    cliente_estado as estado,
    count(distinct venda_id) as total_vendas,
    sum(valor_total) as receita_total,
    sum(quantidade) as total_itens,
    round(sum(valor_total) / count(distinct venda_id), 2) as ticket_medio
from base_vendas
group by 1, 2