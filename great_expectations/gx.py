import great_expectations as gx
import pandas as pd

# Cria ou carrega o contexto (pasta ./gx/)
context = gx.get_context(mode="file")

print("Projeto GX criado/carregado! Pasta './gx/' está aí.")

# Dados de exemplo
dados = pd.DataFrame({
    "nome": ["Ana", "Bruno", "Carlos", "Ana", None],
    "idade": [25, 30, 22, 25, 28],
    "salario": [5000, 6000, 4500, 5500, 7000]
})

print("\nDados de exemplo:")
print(dados)

# Datasource: tenta usar existente ou cria novo
datasource_name = "meu_datasource_pandas"

if datasource_name in context.data_sources:
    datasource = context.data_sources[datasource_name]
    print(f"\nUsando datasource existente: {datasource_name}")
else:
    datasource = context.data_sources.add_pandas(datasource_name)
    print(f"\nCriado novo datasource: {datasource_name}")

# Asset: o mesmo, tenta usar ou cria
asset_name = "dados_exemplo"

if asset_name in datasource.assets:
    asset = datasource.assets[asset_name]
    print(f"Asset '{asset_name}' já existia, usando.")
else:
    asset = datasource.add_dataframe_asset(name=asset_name)
    print(f"Asset '{asset_name}' criado.")

# Expectation Suite (regras)
suite_name = "minhas_primeiras_regras"

try:
    suite = context.suites.get(suite_name)
    print(f"Suite '{suite_name}' já existia.")
except KeyError:
    suite = context.suites.add(
        gx.expectations.ExpectationSuite(
            name=suite_name,
            expectations=[
                gx.expectations.ExpectColumnValuesToNotBeNull("nome"),
                gx.expectations.ExpectColumnValuesToBeBetween("idade", min_value=18, max_value=65),
                gx.expectations.ExpectColumnValuesToBeGreaterThan("salario", value=0),
            ]
        )
    )
    print(f"Suite '{suite_name}' criada.")

# Validação
batch_request = asset.build_batch_request(dados)

checkpoint_name = "meu_primeiro_checkpoint"

try:
    checkpoint = context.checkpoints.get(checkpoint_name)
    print(f"Checkpoint '{checkpoint_name}' já existia.")
except KeyError:
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=checkpoint_name,
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": suite_name,
                }
            ]
        )
    )
    print(f"Checkpoint '{checkpoint_name}' criado.")

resultado = checkpoint.run()

print("\nResultado da validação:")
print(f"Sucesso total? {resultado['success']}")

for validation in resultado['results']:
    exp_type = validation['expectation_config']['expectation_type']
    coluna = validation['expectation_config']['kwargs'].get('column', 'N/A')
    passou = validation['success']
    print(f"- {exp_type} na coluna '{coluna}': {'PASSOU' if passou else 'FALHOU'}")

