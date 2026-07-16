# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze — Ingestão de Dados Brutos
# MAGIC
# MAGIC ## Objetivo
# MAGIC Esta camada é responsável por:
# MAGIC 1. Ler todos os arquivos CSV do Volume de origem
# MAGIC 2. Classificar cada arquivo como **válido** (segue o padrão esperado) ou
# MAGIC    **quarentena** (nome ou schema fora do padrão — ver análise em
# MAGIC    `01_analise_exploratoria_decisoes.md`)
# MAGIC 3. Para arquivos válidos de vendas, extrair `seller_id`, `year` e `month`
# MAGIC    a partir do **nome do arquivo** (o case garante que esses dados não
# MAGIC    estão dentro do CSV)
# MAGIC 4. Persistir os dados **exatamente como recebidos**, sem nenhuma
# MAGIC    transformação de negócio — a Bronze é a cópia fiel da origem, apenas
# MAGIC    acrescida de metadados técnicos de rastreabilidade
# MAGIC
# MAGIC ## Princípio da camada Bronze
# MAGIC Nenhuma decisão de qualidade de dado é tomada aqui (deduplicação, tratamento
# MAGIC de nulos, joins). Isso é responsabilidade da Silver. A Bronze só decide se um
# MAGIC arquivo **consegue ou não** ser interpretado como parte do domínio de vendas —
# MAGIC essa é a única "porta" que existe nesta camada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

import re
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Caminho de origem: Volume onde os CSVs brutos foram carregados manualmente
VOLUME_PATH = "/Volumes/workspace/default/raw_sales/"

# Catalog/schema onde as tabelas Bronze serão criadas
CATALOG = "workspace"
SCHEMA = "default"

TABLE_BRONZE_SALES = f"{CATALOG}.{SCHEMA}.bronze_sales"
TABLE_BRONZE_DIM_SELLER = f"{CATALOG}.{SCHEMA}.bronze_dim_seller"
TABLE_BRONZE_DIM_PRODUCT = f"{CATALOG}.{SCHEMA}.bronze_dim_product"
TABLE_BRONZE_QUARANTINE = f"{CATALOG}.{SCHEMA}.bronze_quarantine_log"

# Timestamp único desta execução do pipeline — usado para rastrear em qual
# rodada cada registro foi ingerido (útil para auditoria e para a estratégia
# de reprocessamento/retry do Job)
INGESTION_RUN_TS = datetime.now(timezone.utc)

print(f"Execução iniciada em: {INGESTION_RUN_TS.isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Classificação dos arquivos por nome
# MAGIC
# MAGIC Antes de tentar ler qualquer conteúdo, classificamos os arquivos pelo
# MAGIC **nome**, já que o padrão esperado (`SELLER_ID_YYYY_MM_sales.csv`) é a
# MAGIC própria fonte do metadado de vendedor e período.
# MAGIC
# MAGIC Arquivos de dimensão (`dim_product.csv`, `dim_seller.csv`) seguem um padrão
# MAGIC fixo próprio e são tratados à parte. Qualquer outro nome que não se encaixe
# MAGIC em nenhum dos dois padrões vai para quarentena.

# COMMAND ----------

# Regex que exige: um ou mais dígitos (seller_id) + underscore + 4 dígitos (ano)
# + underscore + 2 dígitos (mês, 01 a 12) + "_sales" + sufixo opcional de versão
# (ex.: "_v2") + ".csv"
# Isso é propositalmente restritivo: "abc_2025_99_sales.csv" NÃO deve casar,
# pois "abc" não é numérico e "99" não é um mês válido (01-12).
#
# O grupo de versão é OPCIONAL e captura sufixos como "_v2", "_v3" etc. Isso é
# necessário porque descobrimos, ao rodar esta célula pela primeira vez, que
# "1_2025_01_sales_v2.csv" estava caindo incorretamente em quarentena -- o
# regex anterior exigia que o nome terminasse exatamente em "_sales.csv", sem
# prever variações de versão. Isso contrariava a decisão já documentada de que
# arquivos "_v2" devem ser ingeridos normalmente na Bronze (não descartados por
# nome) e só reconciliados depois, na Silver.
PADRAO_ARQUIVO_VENDA = re.compile(r"^(\d+)_(\d{4})_(0[1-9]|1[0-2])_sales(?:_v(\d+))?\.csv$")

arquivos = [f.name for f in dbutils.fs.ls(VOLUME_PATH)]

arquivos_vendas_validos = []   # lista de dicts: {file_name, seller_id, year, month, version}
arquivos_dimensao = []         # dim_product.csv, dim_seller.csv
arquivos_quarentena = []       # tudo que não se encaixou em nenhum padrão

for nome in arquivos:
    match = PADRAO_ARQUIVO_VENDA.match(nome)
    if match:
        seller_id, year, month, versao = match.groups()
        arquivos_vendas_validos.append({
            "file_name": nome,
            "seller_id": int(seller_id),
            "year": int(year),
            "month": int(month),
            # versao=1 quando o arquivo não traz sufixo _vN (versão base/original);
            # caso contrário, usa o número capturado (_v2 -> 2, _v3 -> 3, etc.)
            "version": int(versao) if versao else 1,
        })
    elif nome in ("dim_product.csv", "dim_seller.csv"):
        arquivos_dimensao.append(nome)
    else:
        arquivos_quarentena.append(nome)

print(f"Arquivos de venda válidos:  {len(arquivos_vendas_validos)}")
print(f"Arquivos de dimensão:       {len(arquivos_dimensao)}")
print(f"Arquivos em quarentena:     {len(arquivos_quarentena)} -> {arquivos_quarentena}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Segunda validação: schema mínimo
# MAGIC
# MAGIC Um arquivo pode ter nome válido, mas conteúdo inesperado (proteção extra
# MAGIC contra arquivos corrompidos ou schema completamente incompatível). Aqui
# MAGIC validamos que o arquivo tem **pelo menos as colunas mínimas** para ser
# MAGIC considerado um dado de vendas: `order_id` e `product_id`.
# MAGIC
# MAGIC Não exigimos todas as 8 colunas aqui — isso permitiria, por exemplo, o
# MAGIC caso do `1_2025_01_sales_v2.csv` (schema incompleto, mas ainda reconhecível
# MAGIC como venda) passar pela Bronze. A reconciliação de schema incompleto é
# MAGIC responsabilidade da Silver, não da Bronze.

# COMMAND ----------

COLUNAS_MINIMAS_VENDA = {"order_id", "product_id"}

vendas_confirmadas = []

for item in arquivos_vendas_validos:
    caminho = VOLUME_PATH + item["file_name"]
    try:
        df_preview = spark.read.option("header", True).csv(caminho)
        colunas = set(df_preview.columns)
        if COLUNAS_MINIMAS_VENDA.issubset(colunas):
            vendas_confirmadas.append(item)
        else:
            item["motivo_quarentena"] = f"Schema sem colunas mínimas. Colunas encontradas: {sorted(colunas)}"
            arquivos_quarentena.append(item["file_name"])
    except Exception as e:
        item["motivo_quarentena"] = f"Erro ao ler arquivo: {e}"
        arquivos_quarentena.append(item["file_name"])

print(f"Arquivos de venda confirmados após checagem de schema: {len(vendas_confirmadas)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingestão dos arquivos de vendas válidos
# MAGIC
# MAGIC Cada arquivo é lido individualmente (não em lote via wildcard) porque
# MAGIC precisamos anexar o `seller_id`/`year`/`month` extraídos do **nome do
# MAGIC arquivo** a cada linha — essa informação não existe dentro do CSV.
# MAGIC
# MAGIC Colunas técnicas adicionadas (não fazem parte do dado de negócio):
# MAGIC - `_source_file`: nome do arquivo de origem (rastreabilidade)
# MAGIC - `_seller_id_from_filename`, `_year_from_filename`, `_month_from_filename`:
# MAGIC   metadados extraídos do nome
# MAGIC - `_ingested_at`: timestamp desta execução do pipeline

# COMMAND ----------

dfs_vendas = []

for item in vendas_confirmadas:
    caminho = VOLUME_PATH + item["file_name"]

    # infer_schema=False mantém tudo como string na Bronze -- a tipagem correta
    # (datas, decimais) é responsabilidade da Silver. Isso evita que o Spark
    # "adivinhe" um tipo errado silenciosamente e já corrompa o dado bruto.
    df = spark.read.option("header", True).option("inferSchema", False).csv(caminho)

    df = (
        df.withColumn("_source_file", F.lit(item["file_name"]))
          .withColumn("_seller_id_from_filename", F.lit(item["seller_id"]))
          .withColumn("_year_from_filename", F.lit(item["year"]))
          .withColumn("_month_from_filename", F.lit(item["month"]))
          .withColumn("_version_from_filename", F.lit(item["version"]))
          .withColumn("_ingested_at", F.lit(INGESTION_RUN_TS))
    )
    dfs_vendas.append(df)

# União de todos os arquivos em um único DataFrame.
# allowMissingColumns=True é essencial aqui: é o que permite que o
# "1_2025_01_sales_v2.csv" (sem as colunas discount/ingestion_timestamp)
# seja unido aos demais sem quebrar o pipeline -- as colunas ausentes
# simplesmente ficam como null para aquele arquivo.
df_bronze_sales = dfs_vendas[0]
for df in dfs_vendas[1:]:
    df_bronze_sales = df_bronze_sales.unionByName(df, allowMissingColumns=True)

print(f"Total de linhas ingeridas na Bronze (vendas): {df_bronze_sales.count()}")
display(df_bronze_sales.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingestão das dimensões
# MAGIC
# MAGIC Mesmo princípio: dados carregados como estão, sem tratamento, apenas com
# MAGIC metadados técnicos de rastreabilidade.

# COMMAND ----------

df_bronze_dim_seller = (
    spark.read.option("header", True).csv(VOLUME_PATH + "dim_seller.csv")
    .withColumn("_source_file", F.lit("dim_seller.csv"))
    .withColumn("_ingested_at", F.lit(INGESTION_RUN_TS))
)

df_bronze_dim_product = (
    spark.read.option("header", True).csv(VOLUME_PATH + "dim_product.csv")
    .withColumn("_source_file", F.lit("dim_product.csv"))
    .withColumn("_ingested_at", F.lit(INGESTION_RUN_TS))
)

print(f"Linhas dim_seller:  {df_bronze_dim_seller.count()}")
print(f"Linhas dim_product: {df_bronze_dim_product.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Log de quarentena
# MAGIC
# MAGIC Registra todo arquivo que não foi promovido para as tabelas Bronze de
# MAGIC negócio, junto do motivo. Esse log é o que permite auditoria posterior
# MAGIC (ex.: alguém perguntar "por que o arquivo X não apareceu no relatório?").

# COMMAND ----------

schema_quarentena = StructType([
    StructField("file_name", StringType(), True),
    StructField("motivo", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
])

registros_quarentena = []
for nome in set(arquivos_quarentena):
    # Recupera o motivo específico se já foi identificado na etapa de schema;
    # caso contrário, o motivo é a própria falha no reconhecimento do nome do arquivo.
    motivo_especifico = next(
        (i.get("motivo_quarentena") for i in arquivos_vendas_validos if i["file_name"] == nome and "motivo_quarentena" in i),
        "Nome de arquivo fora do padrão esperado (SELLER_ID_YYYY_MM_sales.csv)"
    )
    registros_quarentena.append((nome, motivo_especifico, INGESTION_RUN_TS))

df_bronze_quarantine = spark.createDataFrame(registros_quarentena, schema=schema_quarentena)
display(df_bronze_quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Persistência em Delta Lake
# MAGIC
# MAGIC Estratégia de escrita: `append`. A Bronze é um histórico cumulativo de
# MAGIC tudo que já foi recebido — cada execução do pipeline adiciona os dados
# MAGIC daquela rodada, identificados pela coluna `_ingested_at`. A deduplicação
# MAGIC entre execuções (idempotência) é tratada na Silver, não aqui.

# COMMAND ----------

(df_bronze_sales.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")  # permite evolução de schema entre execuções
    .saveAsTable(TABLE_BRONZE_SALES))

(df_bronze_dim_seller.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TABLE_BRONZE_DIM_SELLER))

(df_bronze_dim_product.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TABLE_BRONZE_DIM_PRODUCT))

(df_bronze_quarantine.write
    .format("delta")
    .mode("append")
    .saveAsTable(TABLE_BRONZE_QUARANTINE))

print("Tabelas Bronze gravadas com sucesso:")
print(f"  - {TABLE_BRONZE_SALES}")
print(f"  - {TABLE_BRONZE_DIM_SELLER}")
print(f"  - {TABLE_BRONZE_DIM_PRODUCT}")
print(f"  - {TABLE_BRONZE_QUARANTINE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Checagem rápida de qualidade (sanity check)
# MAGIC
# MAGIC Antes de considerar a camada Bronze concluída, validamos que:
# MAGIC - Nenhuma tabela ficou vazia inesperadamente
# MAGIC - A contagem de arquivos processados bate com o total de arquivos do Volume

# COMMAND ----------

total_arquivos_volume = len(arquivos)
total_processado = len(vendas_confirmadas) + len(arquivos_dimensao) + len(set(arquivos_quarentena))

print(f"Total de arquivos no Volume:        {total_arquivos_volume}")
print(f"Total contabilizado (venda+dim+quarentena): {total_processado}")

assert total_arquivos_volume == total_processado, (
    "ALERTA: a soma de arquivos processados não bate com o total do Volume. "
    "Verifique se algum arquivo foi contado em duplicidade ou ignorado."
)

assert df_bronze_sales.count() > 0, "ALERTA: tabela bronze_sales está vazia."
assert df_bronze_dim_seller.count() > 0, "ALERTA: tabela bronze_dim_seller está vazia."
assert df_bronze_dim_product.count() > 0, "ALERTA: tabela bronze_dim_product está vazia."

print("Checagens de sanidade da Bronze: OK")
