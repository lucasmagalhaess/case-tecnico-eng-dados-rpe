# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver — Dados Tratados (Estratégia Incremental)
# MAGIC
# MAGIC ## Objetivo
# MAGIC Partindo das tabelas Bronze (dado bruto, fiel à origem), esta camada aplica
# MAGIC todas as regras de qualidade e padronização necessárias para consumo
# MAGIC analítico na Gold:
# MAGIC
# MAGIC 1. Correção de tipos (`product_id` como texto -> inteiro, datas, decimais)
# MAGIC 2. Deduplicação de vendas por `seller_id + year + month + order_id`
# MAGIC 3. Padronização de texto (`product_name` com capitalização inconsistente)
# MAGIC 4. Tratamento de dimensões incompletas (vendedor/produto sem cadastro)
# MAGIC 5. Reconciliação do schema incompleto do arquivo `_v2`
# MAGIC 6. Checagens de qualidade (nulos, duplicidade, soma de valores)
# MAGIC
# MAGIC ## Estratégia incremental
# MAGIC Diferente de um `overwrite` completo (que reprocessaria toda a Bronze a
# MAGIC cada execução), esta versão:
# MAGIC
# MAGIC - Mantém uma tabela de controle (`silver_load_control`) com o **watermark**
# MAGIC   (timestamp da última linha processada com sucesso)
# MAGIC - A cada execução, lê da Bronze **apenas as linhas com `_ingested_at` maior
# MAGIC   que esse watermark** -- ou seja, só o que é novo desde a última rodada
# MAGIC - Usa **`MERGE INTO`** (upsert) para gravar na Silver: se o registro já
# MAGIC   existe (mesma chave `seller_id+year+month+order_id`), atualiza apenas se
# MAGIC   o novo dado for mais recente; se não existe, insere
# MAGIC - Ao final, avança o watermark para a próxima execução
# MAGIC
# MAGIC Isso também resolve **dados atrasados (late arriving data)**: se uma
# MAGIC correção de uma venda antiga chegar em uma nova execução da Bronze, o
# MAGIC MERGE identifica que já existe um registro com aquela chave e decide, pelo
# MAGIC `ingestion_timestamp`, se deve sobrescrever ou ignorar.
# MAGIC
# MAGIC Todas as decisões de tratamento foram documentadas previamente em
# MAGIC `01_analise_exploratoria_decisoes.md`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timezone

CATALOG = "workspace"
SCHEMA = "default"

TABLE_BRONZE_SALES = f"{CATALOG}.{SCHEMA}.bronze_sales"
TABLE_BRONZE_DIM_SELLER = f"{CATALOG}.{SCHEMA}.bronze_dim_seller"
TABLE_BRONZE_DIM_PRODUCT = f"{CATALOG}.{SCHEMA}.bronze_dim_product"

TABLE_SILVER_FACT_SALES = f"{CATALOG}.{SCHEMA}.silver_fact_sales"
TABLE_SILVER_DIM_SELLER = f"{CATALOG}.{SCHEMA}.silver_dim_seller"
TABLE_SILVER_DIM_PRODUCT = f"{CATALOG}.{SCHEMA}.silver_dim_product"
TABLE_CONTROL = f"{CATALOG}.{SCHEMA}.silver_load_control"

PIPELINE_NAME = "silver_fact_sales"

# Chave sentinela usada para representar "sem correspondência na dimensão"
# (padrão de modelagem dimensional "unknown member" -- ver notebook original
# para explicação completa).
CHAVE_DESCONHECIDA = -1

RUN_TS = datetime.now(timezone.utc)
print(f"Execução iniciada em: {RUN_TS.isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Controle de watermark
# MAGIC
# MAGIC Cria a tabela de controle na primeira execução (se não existir) e lê o
# MAGIC watermark da última execução bem-sucedida. Se for a primeira vez, o
# MAGIC watermark é uma data bem antiga, garantindo que tudo que já está na
# MAGIC Bronze seja processado nessa primeira carga.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_CONTROL} (
        pipeline_name STRING,
        last_watermark TIMESTAMP,
        last_run_at TIMESTAMP,
        rows_processed_last_run LONG
    ) USING DELTA
""")

watermark_row = (
    spark.table(TABLE_CONTROL)
    .filter(F.col("pipeline_name") == PIPELINE_NAME)
    .orderBy(F.col("last_run_at").desc())
    .limit(1)
    .collect()
)

if watermark_row:
    last_watermark = watermark_row[0]["last_watermark"]
    print(f"Watermark encontrado -- processando apenas dados ingeridos após: {last_watermark}")
else:
    last_watermark = datetime(1900, 1, 1, tzinfo=timezone.utc)
    print("Nenhum watermark anterior encontrado -- esta é a primeira carga (processará tudo da Bronze).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura incremental da Bronze
# MAGIC
# MAGIC Filtra apenas as linhas cujo `_ingested_at` (timestamp de quando a Bronze
# MAGIC processou aquele arquivo) é posterior ao watermark. Se não houver nada
# MAGIC novo, o notebook para aqui -- não há necessidade de reprocessar nada.

# COMMAND ----------

df_sales_raw = spark.table(TABLE_BRONZE_SALES).filter(F.col("_ingested_at") > last_watermark)

qtd_linhas_novas = df_sales_raw.count()
print(f"Linhas novas na Bronze desde o último watermark: {qtd_linhas_novas}")

if qtd_linhas_novas == 0:
    print("\nNada novo para processar. Encerrando execução sem alterar a Silver.")
    dbutils.notebook.exit("SEM_DADOS_NOVOS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tratamento das dimensões
# MAGIC
# MAGIC As dimensões (`dim_seller.csv`, `dim_product.csv`) não seguem o mesmo
# MAGIC padrão incremental dos arquivos de venda -- são recarregadas por inteiro a
# MAGIC cada execução da Bronze (arquivos pequenos, cadastro completo). Por isso,
# MAGIC aqui a Silver sempre lê a dimensão inteira da Bronze mais recente, mas usa
# MAGIC `MERGE` (não `overwrite`) para gravar: registros novos são inseridos,
# MAGIC registros existentes são atualizados apenas se algo mudou, e o registro
# MAGIC "desconhecido" (-1) é preservado entre execuções.

# COMMAND ----------

df_dim_seller_raw = spark.table(TABLE_BRONZE_DIM_SELLER)

df_silver_dim_seller_novo = (
    df_dim_seller_raw
    .select(
        F.expr("try_cast(seller_id as int)").alias("seller_id"),
        F.trim(F.col("seller_name")).alias("seller_name"),
        F.upper(F.trim(F.col("state"))).alias("state"),
    )
    .dropDuplicates(["seller_id"])
    .filter(F.col("seller_id").isNotNull())
)

df_dim_product_raw = spark.table(TABLE_BRONZE_DIM_PRODUCT)

df_silver_dim_product_novo = (
    df_dim_product_raw
    .select(
        F.expr("try_cast(product_id as int)").alias("product_id"),
        F.initcap(F.trim(F.col("product_name"))).alias("product_name"),
        F.initcap(F.trim(F.col("category"))).alias("category"),
    )
    .dropDuplicates(["product_id"])
    .filter(F.col("product_id").isNotNull())
)

print(f"Vendedores na Bronze (para merge): {df_silver_dim_seller_novo.count()}")
print(f"Produtos na Bronze (para merge):   {df_silver_dim_product_novo.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Bootstrap das tabelas Silver de dimensão (primeira execução)
# MAGIC
# MAGIC Se as tabelas Silver de dimensão ainda não existem, cria-as já incluindo
# MAGIC o registro "desconhecido" (-1). Nas execuções seguintes, esse bloco é
# MAGIC ignorado e o MERGE (próxima célula) cuida das atualizações.

# COMMAND ----------

def tabela_existe(nome_tabela):
    return spark.catalog.tableExists(nome_tabela)

if not tabela_existe(TABLE_SILVER_DIM_SELLER):
    df_seller_desconhecido = spark.createDataFrame(
        [(CHAVE_DESCONHECIDA, "Não cadastrado", "Desconhecido")],
        schema=["seller_id", "seller_name", "state"],
    )
    (df_silver_dim_seller_novo.unionByName(df_seller_desconhecido)
        .write.format("delta").mode("overwrite").saveAsTable(TABLE_SILVER_DIM_SELLER))
    print(f"Tabela {TABLE_SILVER_DIM_SELLER} criada (bootstrap).")

if not tabela_existe(TABLE_SILVER_DIM_PRODUCT):
    df_product_desconhecido = spark.createDataFrame(
        [(CHAVE_DESCONHECIDA, "Não Cadastrado", "Desconhecida")],
        schema=["product_id", "product_name", "category"],
    )
    (df_silver_dim_product_novo.unionByName(df_product_desconhecido)
        .write.format("delta").mode("overwrite").saveAsTable(TABLE_SILVER_DIM_PRODUCT))
    print(f"Tabela {TABLE_SILVER_DIM_PRODUCT} criada (bootstrap).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 MERGE das dimensões (execuções seguintes)
# MAGIC
# MAGIC Atualiza vendedores/produtos que já existem (caso o cadastro mude) e
# MAGIC insere os que forem novos. O registro `-1` nunca é tocado, pois nunca
# MAGIC existirá na origem (Bronze) para dar match.

# COMMAND ----------

DeltaTable.forName(spark, TABLE_SILVER_DIM_SELLER).alias("t").merge(
    df_silver_dim_seller_novo.alias("s"),
    "t.seller_id = s.seller_id"
).whenMatchedUpdate(set={
    "seller_name": "s.seller_name",
    "state": "s.state",
}).whenNotMatchedInsert(values={
    "seller_id": "s.seller_id",
    "seller_name": "s.seller_name",
    "state": "s.state",
}).execute()

DeltaTable.forName(spark, TABLE_SILVER_DIM_PRODUCT).alias("t").merge(
    df_silver_dim_product_novo.alias("s"),
    "t.product_id = s.product_id"
).whenMatchedUpdate(set={
    "product_name": "s.product_name",
    "category": "s.category",
}).whenNotMatchedInsert(values={
    "product_id": "s.product_id",
    "product_name": "s.product_name",
    "category": "s.category",
}).execute()

df_silver_dim_seller = spark.table(TABLE_SILVER_DIM_SELLER)
df_silver_dim_product = spark.table(TABLE_SILVER_DIM_PRODUCT)

print(f"Dimensão de vendedores após merge: {df_silver_dim_seller.count()} registros")
print(f"Dimensão de produtos após merge:   {df_silver_dim_product.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tratamento do lote incremental de vendas
# MAGIC
# MAGIC Mesma lógica de tipagem, tratamento de malformados e reconciliação de FK
# MAGIC já validada anteriormente -- agora aplicada apenas ao lote novo
# MAGIC (`df_sales_raw`, já filtrado pelo watermark), não à Bronze inteira.

# COMMAND ----------

df_sales_typed = (
    df_sales_raw
    .select(
        F.expr("try_cast(order_id as long)").alias("order_id"),
        F.expr("try_cast(product_id as int)").alias("product_id"),
        F.expr("try_cast(quantity as int)").alias("quantity"),
        F.expr("try_cast(amount as double)").alias("amount"),
        F.lower(F.trim(F.col("status"))).alias("status"),
        F.expr("try_cast(sale_date as date)").alias("sale_date"),
        F.coalesce(F.expr("try_cast(discount as double)"), F.lit(0.0)).alias("discount"),
        F.coalesce(
            F.expr("try_cast(ingestion_timestamp as timestamp)"),
            F.col("_ingested_at").cast("timestamp"),
        ).alias("ingestion_timestamp"),
        F.col("_seller_id_from_filename").cast("int").alias("seller_id"),
        F.col("_year_from_filename").cast("int").alias("year"),
        F.col("_month_from_filename").cast("int").alias("month"),
        F.when(
            F.regexp_extract(F.col("_source_file"), r"_v(\d+)\.csv$", 1) == "",
            F.lit(1),
        ).otherwise(
            F.regexp_extract(F.col("_source_file"), r"_v(\d+)\.csv$", 1).cast("int")
        ).alias("file_version"),
        F.col("_source_file").alias("source_file"),
    )
)

colunas_numericas_criticas = ["order_id", "product_id", "quantity", "amount", "sale_date"]
condicao_malformada = None
for coluna in colunas_numericas_criticas:
    cond = F.col(coluna).isNull()
    condicao_malformada = cond if condicao_malformada is None else (condicao_malformada | cond)

df_sales_rejeitadas = df_sales_typed.filter(condicao_malformada)
df_sales_typed = df_sales_typed.filter(~condicao_malformada)

print(f"Linhas do lote com dado malformado (rejeitadas): {df_sales_rejeitadas.count()}")
print(f"Linhas do lote seguindo no pipeline:              {df_sales_typed.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Deduplicação DENTRO do lote novo
# MAGIC
# MAGIC Mesma chave composta (`seller_id + year + month + order_id`) e mesmo
# MAGIC critério (mais recente por `ingestion_timestamp`) usados na validação
# MAGIC anterior -- necessário caso o lote novo em si contenha duplicatas
# MAGIC internas (ex.: o mesmo arquivo lido duas vezes na mesma execução da
# MAGIC Bronze).
# MAGIC
# MAGIC A deduplicação **contra o que já existe na Silver** (registros de
# MAGIC execuções anteriores) é resolvida depois, pelo próprio `MERGE`.

# COMMAND ----------

janela_dedup = Window.partitionBy(
    "seller_id", "year", "month", "order_id"
).orderBy(F.col("ingestion_timestamp").desc())

df_sales_dedup = (
    df_sales_typed
    .withColumn("_rn", F.row_number().over(janela_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

print(f"Linhas do lote após deduplicação interna: {df_sales_dedup.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Reconciliação de chaves estrangeiras

# COMMAND ----------

sellers_cadastrados = [row.seller_id for row in df_silver_dim_seller.select("seller_id").collect()]
produtos_cadastrados = [row.product_id for row in df_silver_dim_product.select("product_id").collect()]

df_sales_reconciliado = (
    df_sales_dedup
    .withColumn(
        "seller_id_reconciliado",
        F.when(F.col("seller_id").isin(sellers_cadastrados), F.col("seller_id")).otherwise(F.lit(CHAVE_DESCONHECIDA)),
    )
    .withColumn(
        "product_id_reconciliado",
        F.when(F.col("product_id").isin(produtos_cadastrados), F.col("product_id")).otherwise(F.lit(CHAVE_DESCONHECIDA)),
    )
)

df_silver_fact_batch = (
    df_sales_reconciliado
    .select(
        "order_id",
        F.col("seller_id_reconciliado").alias("seller_id"),
        F.col("product_id_reconciliado").alias("product_id"),
        "quantity", "amount", "discount", "status", "sale_date",
        "ingestion_timestamp", "year", "month", "file_version", "source_file",
    )
)

print(f"Linhas do lote prontas para MERGE: {df_silver_fact_batch.count()}")
display(df_silver_fact_batch.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Checagens de qualidade (escopo: lote incremental)

# COMMAND ----------

for coluna in ["order_id", "seller_id", "product_id", "amount", "sale_date"]:
    nulos = df_silver_fact_batch.filter(F.col(coluna).isNull()).count()
    assert nulos == 0, f"ALERTA: coluna '{coluna}' tem {nulos} valores nulos no lote."
    print(f"Nulos em '{coluna}' (lote): {nulos} (OK)")

duplicatas_no_lote = (
    df_silver_fact_batch.groupBy("seller_id", "year", "month", "order_id")
    .count().filter(F.col("count") > 1).count()
)
assert duplicatas_no_lote == 0, f"ALERTA: {duplicatas_no_lote} duplicatas remanescentes no lote."

print("\nChecagens de qualidade do lote: OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Bootstrap da tabela Silver de vendas (primeira execução)

# COMMAND ----------

if not tabela_existe(TABLE_SILVER_FACT_SALES):
    (df_silver_fact_batch.write.format("delta").mode("overwrite")
        .saveAsTable(TABLE_SILVER_FACT_SALES))
    print(f"Tabela {TABLE_SILVER_FACT_SALES} criada (bootstrap) com {df_silver_fact_batch.count()} linhas.")
else:
    print(f"Tabela {TABLE_SILVER_FACT_SALES} já existe -- seguindo para MERGE incremental.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. MERGE incremental na tabela de fatos
# MAGIC
# MAGIC Regra de upsert:
# MAGIC - Se a combinação `seller_id+year+month+order_id` já existe na Silver
# MAGIC   **e** o registro novo tem `ingestion_timestamp` mais recente -> atualiza
# MAGIC   (cobre correções / dados atrasados / late arriving data)
# MAGIC - Se a combinação já existe mas o registro novo é **mais antigo** -> não
# MAGIC   faz nada (evita regressão de dado)
# MAGIC - Se a combinação não existe -> insere como novo registro

# COMMAND ----------

(DeltaTable.forName(spark, TABLE_SILVER_FACT_SALES).alias("t")
    .merge(
        df_silver_fact_batch.alias("s"),
        "t.seller_id = s.seller_id AND t.year = s.year AND t.month = s.month AND t.order_id = s.order_id"
    )
    .whenMatchedUpdate(
        condition="s.ingestion_timestamp > t.ingestion_timestamp",
        set={
            "product_id": "s.product_id",
            "quantity": "s.quantity",
            "amount": "s.amount",
            "discount": "s.discount",
            "status": "s.status",
            "sale_date": "s.sale_date",
            "ingestion_timestamp": "s.ingestion_timestamp",
            "file_version": "s.file_version",
            "source_file": "s.source_file",
        },
    )
    .whenNotMatchedInsert(values={
        "order_id": "s.order_id",
        "seller_id": "s.seller_id",
        "product_id": "s.product_id",
        "quantity": "s.quantity",
        "amount": "s.amount",
        "discount": "s.discount",
        "status": "s.status",
        "sale_date": "s.sale_date",
        "ingestion_timestamp": "s.ingestion_timestamp",
        "year": "s.year",
        "month": "s.month",
        "file_version": "s.file_version",
        "source_file": "s.source_file",
    })
    .execute())

total_silver_atual = spark.table(TABLE_SILVER_FACT_SALES).count()
print(f"MERGE concluído. Total de linhas na Silver após esta execução: {total_silver_atual}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Avanço do watermark
# MAGIC
# MAGIC Registra o maior `_ingested_at` processado nesta execução como o novo
# MAGIC watermark, para que a próxima execução só pegue dados posteriores a este.

# COMMAND ----------

novo_watermark = df_sales_raw.agg(F.max("_ingested_at")).collect()[0][0]

spark.createDataFrame(
    [(PIPELINE_NAME, novo_watermark, RUN_TS, qtd_linhas_novas)],
    schema=["pipeline_name", "last_watermark", "last_run_at", "rows_processed_last_run"],
).write.format("delta").mode("append").saveAsTable(TABLE_CONTROL)

print(f"Watermark avançado para: {novo_watermark}")
print(f"Linhas processadas nesta execução: {qtd_linhas_novas}")
print("\nExecução da camada Silver (incremental) concluída com sucesso.")
