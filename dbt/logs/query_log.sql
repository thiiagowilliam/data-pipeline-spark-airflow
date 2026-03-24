-- created_at: 2026-03-21T01:03:05.350842261+00:00
-- finished_at: 2026-03-21T01:03:10.543459648+00:00
-- elapsed: 5.2s
-- outcome: success
-- dialect: bigquery
-- node_id: not available
-- query_id: TA6EyRN6zBNztPvbb4L5YJwiNdU
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "bigquery", "target_name": "dev"} */

    select distinct schema_name from `resolve-ai-407701`.INFORMATION_SCHEMA.SCHEMATA;
  ;
-- created_at: 2026-03-21T01:03:10.722041031+00:00
-- finished_at: 2026-03-21T01:03:14.547194165+00:00
-- elapsed: 3.8s
-- outcome: success
-- dialect: bigquery
-- node_id: model.data_pipeline.ft_vendas
-- query_id: 4pjxtxIE9qK2TnxJ7z4CfpaZf5i
-- desc: get_relation > list_relations call
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `resolve-ai-407701`.`silver_silver`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-03-21T01:03:10.717571902+00:00
-- finished_at: 2026-03-21T01:03:14.880241836+00:00
-- elapsed: 4.2s
-- outcome: success
-- dialect: bigquery
-- node_id: model.data_pipeline.cl_clientes
-- query_id: 4BePFgDuiu76qaclRPq0FSOr1Kh
-- desc: get_relation > list_relations call
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `resolve-ai-407701`.`silver_silver`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-03-21T01:03:14.557023670+00:00
-- finished_at: 2026-03-21T01:03:19.363750614+00:00
-- elapsed: 4.8s
-- outcome: success
-- dialect: bigquery
-- node_id: model.data_pipeline.ft_vendas
-- query_id: DS7u1eFHMEA9JjNG8Gxk6nl2QBa
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.data_pipeline.ft_vendas", "profile_name": "bigquery", "target_name": "dev"} */

  
    

    create or replace table `resolve-ai-407701`.`silver_silver`.`ft_vendas`
      
    partition by data_venda
    

    
    OPTIONS()
    as (
      

with source as (
    select
        id as venda_id,
        cliente_id,
        produto_id,
        data_venda,
        valor_total,
        quantidade,
        metodo_pagto
    from `resolve-ai-407701`.`vendas`.`bronze`
    qualify row_number() over (partition by id order by data_venda desc) = 1
)

select * from source
    );
  ;
-- created_at: 2026-03-21T01:03:14.894785441+00:00
-- finished_at: 2026-03-21T01:03:19.889355814+00:00
-- elapsed: 5.0s
-- outcome: success
-- dialect: bigquery
-- node_id: model.data_pipeline.cl_clientes
-- query_id: RvyMsbBsq1fRbOtH9ovntKRkp1v
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.data_pipeline.cl_clientes", "profile_name": "bigquery", "target_name": "dev"} */

  
    

    create or replace table `resolve-ai-407701`.`silver_silver`.`cl_clientes`
      
    partition by data_cadastro
    

    
    OPTIONS()
    as (
      

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
    from `resolve-ai-407701`.`clientes`.`bronze`
    qualify row_number() over (partition by id order by data_cadastro desc) = 1
)

select * from source
    );
  ;
-- created_at: 2026-03-21T01:03:19.371963204+00:00
-- finished_at: 2026-03-21T01:03:24.088344540+00:00
-- elapsed: 4.7s
-- outcome: success
-- dialect: bigquery
-- node_id: test.data_pipeline.not_null_ft_vendas_cliente_id.0907c4d601
-- query_id: GNsPiXw0pwYuuLOL0wSYjdh7eWA
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.data_pipeline.not_null_ft_vendas_cliente_id.0907c4d601", "profile_name": "bigquery", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cliente_id
from `resolve-ai-407701`.`silver_silver`.`ft_vendas`
where cliente_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-21T01:03:19.373985969+00:00
-- finished_at: 2026-03-21T01:03:24.103381048+00:00
-- elapsed: 4.7s
-- outcome: success
-- dialect: bigquery
-- node_id: test.data_pipeline.unique_ft_vendas_venda_id.c504146740
-- query_id: C1vyLJWvg31wAgIwaWzzelaLxyp
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.data_pipeline.unique_ft_vendas_venda_id.c504146740", "profile_name": "bigquery", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with dbt_test__target as (

  select venda_id as unique_field
  from `resolve-ai-407701`.`silver_silver`.`ft_vendas`
  where venda_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-21T01:03:19.901115436+00:00
-- finished_at: 2026-03-21T01:03:24.450769330+00:00
-- elapsed: 4.5s
-- outcome: success
-- dialect: bigquery
-- node_id: test.data_pipeline.unique_cl_clientes_email.c8b6abbf3c
-- query_id: vfM3B5kV4B8jRU2npFkSLKij5t9
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.data_pipeline.unique_cl_clientes_email.c8b6abbf3c", "profile_name": "bigquery", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with dbt_test__target as (

  select email as unique_field
  from `resolve-ai-407701`.`silver_silver`.`cl_clientes`
  where email is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-21T01:03:19.370232205+00:00
-- finished_at: 2026-03-21T01:03:27.151831652+00:00
-- elapsed: 7.8s
-- outcome: success
-- dialect: bigquery
-- node_id: test.data_pipeline.not_null_ft_vendas_venda_id.393add4310
-- query_id: zF8YLzS43fIExOemJ3QMdvNVKpt
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.data_pipeline.not_null_ft_vendas_venda_id.393add4310", "profile_name": "bigquery", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select venda_id
from `resolve-ai-407701`.`silver_silver`.`ft_vendas`
where venda_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-21T01:03:19.921630562+00:00
-- finished_at: 2026-03-21T01:03:28.473119991+00:00
-- elapsed: 8.6s
-- outcome: success
-- dialect: bigquery
-- node_id: test.data_pipeline.not_null_cl_clientes_email.7afecc4985
-- query_id: TNUge9vPo3MF14k242KQY3dlICV
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.data_pipeline.not_null_cl_clientes_email.7afecc4985", "profile_name": "bigquery", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select email
from `resolve-ai-407701`.`silver_silver`.`cl_clientes`
where email is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-21T01:03:19.926035234+00:00
-- finished_at: 2026-03-21T01:03:28.862664613+00:00
-- elapsed: 8.9s
-- outcome: success
-- dialect: bigquery
-- node_id: test.data_pipeline.accepted_values_cl_clientes_status__ativo__inativo__pendente.61419f1120
-- query_id: LFvWyuZBs3FBMhI8PksXKT7yN8d
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.data_pipeline.accepted_values_cl_clientes_status__ativo__inativo__pendente.61419f1120", "profile_name": "bigquery", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from `resolve-ai-407701`.`silver_silver`.`cl_clientes`
    group by status

)

select *
from all_values
where value_field not in (
    'ativo','inativo','pendente'
)



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-21T01:03:19.906467297+00:00
-- finished_at: 2026-03-21T01:03:28.889267940+00:00
-- elapsed: 9.0s
-- outcome: success
-- dialect: bigquery
-- node_id: test.data_pipeline.not_null_cl_clientes_cliente_id.cf6d505d57
-- query_id: k1rh5Iz0GamZQ0YmedBLp1cAJAL
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.data_pipeline.not_null_cl_clientes_cliente_id.cf6d505d57", "profile_name": "bigquery", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cliente_id
from `resolve-ai-407701`.`silver_silver`.`cl_clientes`
where cliente_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-21T01:03:19.930230453+00:00
-- finished_at: 2026-03-21T01:03:31.940829296+00:00
-- elapsed: 12.0s
-- outcome: success
-- dialect: bigquery
-- node_id: test.data_pipeline.relationships_ft_vendas_97381116c4a2895e4b9a65199cd8829b.1a725c439d
-- query_id: RP5123oJPbt9sl3Fb6amawYE3Gt
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.data_pipeline.relationships_ft_vendas_97381116c4a2895e4b9a65199cd8829b.1a725c439d", "profile_name": "bigquery", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select cliente_id as from_field
    from `resolve-ai-407701`.`silver_silver`.`ft_vendas`
    where cliente_id is not null
),

parent as (
    select cliente_id as to_field
    from `resolve-ai-407701`.`silver_silver`.`cl_clientes`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-21T01:03:19.933933587+00:00
-- finished_at: 2026-03-21T01:03:32.919598469+00:00
-- elapsed: 13.0s
-- outcome: success
-- dialect: bigquery
-- node_id: test.data_pipeline.unique_cl_clientes_cliente_id.ffe4e62c30
-- query_id: WpiuYw60CYa4JaieUjLQu7ITFC0
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.data_pipeline.unique_cl_clientes_cliente_id.ffe4e62c30", "profile_name": "bigquery", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with dbt_test__target as (

  select cliente_id as unique_field
  from `resolve-ai-407701`.`silver_silver`.`cl_clientes`
  where cliente_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-21T01:03:32.921836964+00:00
-- finished_at: 2026-03-21T01:03:35.757000414+00:00
-- elapsed: 2.8s
-- outcome: success
-- dialect: bigquery
-- node_id: model.data_pipeline.datamart_resumo_vendas_cliente
-- query_id: egMY3SOoQW2KPwx2ehvApIVSQfc
-- desc: get_relation > list_relations call
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `resolve-ai-407701`.`silver_gold`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-03-21T01:03:35.762004330+00:00
-- finished_at: 2026-03-21T01:03:40.845779747+00:00
-- elapsed: 5.1s
-- outcome: success
-- dialect: bigquery
-- node_id: model.data_pipeline.datamart_resumo_vendas_cliente
-- query_id: XusbqulSDMmjRwozx0SiIwtF4g3
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.data_pipeline.datamart_resumo_vendas_cliente", "profile_name": "bigquery", "target_name": "dev"} */

  
    

    create or replace table `resolve-ai-407701`.`silver_gold`.`datamart_resumo_vendas_cliente`
      
    
    cluster by cliente_id, data_venda

    
    OPTIONS()
    as (
      

with vendas as (
    select * from `resolve-ai-407701`.`silver_silver`.`ft_vendas`
),

clientes as (
    select * from `resolve-ai-407701`.`silver_silver`.`cl_clientes`
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
    );
  ;
-- created_at: 2026-03-21T01:03:40.850116942+00:00
-- finished_at: 2026-03-21T01:03:44.396976058+00:00
-- elapsed: 3.5s
-- outcome: success
-- dialect: bigquery
-- node_id: test.data_pipeline.unique_datamart_resumo_vendas_cliente_venda_id.9c9555bd1a
-- query_id: bSmJerLEeoDbTpKoor98ORnHQ0m
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.data_pipeline.unique_datamart_resumo_vendas_cliente_venda_id.9c9555bd1a", "profile_name": "bigquery", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with dbt_test__target as (

  select venda_id as unique_field
  from `resolve-ai-407701`.`silver_gold`.`datamart_resumo_vendas_cliente`
  where venda_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-21T01:03:40.850087787+00:00
-- finished_at: 2026-03-21T01:03:44.616496167+00:00
-- elapsed: 3.8s
-- outcome: success
-- dialect: bigquery
-- node_id: test.data_pipeline.not_null_datamart_resumo_vendas_cliente_venda_id.e28a034b0b
-- query_id: 3FhZmdlcjR2nLI0v6LgaVqW2Uhm
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.data_pipeline.not_null_datamart_resumo_vendas_cliente_venda_id.e28a034b0b", "profile_name": "bigquery", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select venda_id
from `resolve-ai-407701`.`silver_gold`.`datamart_resumo_vendas_cliente`
where venda_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-21T01:03:40.850122106+00:00
-- finished_at: 2026-03-21T01:03:44.663608487+00:00
-- elapsed: 3.8s
-- outcome: success
-- dialect: bigquery
-- node_id: test.data_pipeline.not_null_datamart_resumo_vendas_cliente_cliente_id.4f5958cae0
-- query_id: lnPdpythZWkV9ocGQzoDnaG9e9o
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.data_pipeline.not_null_datamart_resumo_vendas_cliente_cliente_id.4f5958cae0", "profile_name": "bigquery", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cliente_id
from `resolve-ai-407701`.`silver_gold`.`datamart_resumo_vendas_cliente`
where cliente_id is null



  
  
      
    ) dbt_internal_test;
