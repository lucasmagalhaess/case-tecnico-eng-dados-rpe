# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold — Modelagem Analítica e Métricas de Negócio
# MAGIC
# MAGIC ## Objetivo
# MAGIC Partindo da Silver (dados já tratados, deduplicados e reconciliados), esta
# MAGIC camada:
# MAGIC 1. Finaliza a modelagem em **Star Schema** (fato de vendas + dimensões de
# MAGIC    vendedor, produto e tempo)
# MAGIC 2. Responde às 11 perguntas analíticas do case, cada uma como uma tabela
# MAGIC    Delta pronta para ser consumida diretamente no Power BI/Excel, sem
# MAGIC    necessidade de cálculos adicionais na ferramenta de BI
# MAGIC
# MAGIC ## Por que Star Schema
# MAGIC - **Facilidade de entendimento:** qualquer pessoa de negócio reconhece o
# MAGIC   padrão "fato no centro, dimensões ao redor" -- é o modelo mais difundido
# MAGIC   em ferramentas de BI self-service como Power BI
# MAGIC - **Performance:** menos joins que um modelo normalizado (snowflake),
# MAGIC   já que as dimensões não se ramificam em sub-dimensões
# MAGIC - **Manutenção:** adicionar uma nova métrica ou dimensão não exige
# MAGIC   reestruturar o fato -- só um novo join

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "default"

TABLE_SILVER_FACT_SALES = f"{CATALOG}.{SCHEMA}.silver_fact_sales"
TABLE_SILVER_DIM_SELLER = f"{CATALOG}.{SCHEMA}.silver_dim_seller"
TABLE_SILVER_DIM_PRODUCT = f"{CATALOG}.{SCHEMA}.silver_dim_product"

TABLE_GOLD_FACT_SALES = f"{CATALOG}.{SCHEMA}.gold_fact_sales"
TABLE_GOLD_DIM_SELLER = f"{CATALOG}.{SCHEMA}.gold_dim_seller"
TABLE_GOLD_DIM_PRODUCT = f"{CATALOG}.{SCHEMA}.gold_dim_product"
TABLE_GOLD_DIM_TIME = f"{CATALOG}.{SCHEMA}.gold_dim_time"

df_fact = spark.table(TABLE_SILVER_FACT_SALES)
df_dim_seller = spark.table(TABLE_SILVER_DIM_SELLER)
df_dim_product = spark.table(TABLE_SILVER_DIM_PRODUCT)

print(f"Linhas no fato de vendas (Silver): {df_fact.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dimensão de tempo
# MAGIC
# MAGIC Construída a partir dos períodos (`year`, `month`) realmente presentes no
# MAGIC fato de vendas -- evita criar um calendário genérico com datas que nunca
# MAGIC ocorrem no dataset.

# COMMAND ----------

MESES_PT = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}

df_gold_dim_time = (
    df_fact.select("year", "month").distinct()
    .withColumn("quarter", F.ceil(F.col("month") / 3).cast("int"))
    .withColumn("year_month", F.format_string("%d-%02d", F.col("year"), F.col("month")))
)

meses_df = spark.createDataFrame(
    [(k, v) for k, v in MESES_PT.items()], schema=["month", "month_name"]
)
df_gold_dim_time = df_gold_dim_time.join(meses_df, on="month", how="left").select(
    "year", "month", "month_name", "quarter", "year_month"
).orderBy("year", "month")

(df_gold_dim_time.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(TABLE_GOLD_DIM_TIME))

print(f"Dimensão de tempo: {df_gold_dim_time.count()} períodos")
display(df_gold_dim_time)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dimensões de vendedor e produto (Gold)
# MAGIC
# MAGIC Já vêm tratadas da Silver -- aqui apenas materializamos como tabelas Gold,
# MAGIC mantendo a separação de camadas explícita mesmo sem transformação adicional.

# COMMAND ----------

(df_dim_seller.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(TABLE_GOLD_DIM_SELLER))
(df_dim_product.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(TABLE_GOLD_DIM_PRODUCT))

print("Dimensões Gold de vendedor e produto materializadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fato de vendas (Gold)
# MAGIC
# MAGIC Mesma granularidade da Silver (1 linha = 1 venda). Adicionamos apenas uma
# MAGIC coluna calculada (`net_amount`) para deixar explícito na modelagem que
# MAGIC `amount` já é o valor com desconto aplicado ou não -- documentado como
# MAGIC premissa abaixo.
# MAGIC
# MAGIC **Premissa assumida:** a coluna `amount`, conforme veio da origem, já
# MAGIC representa o valor final da venda (pós-desconto). `discount` é tratada
# MAGIC como informação complementar (quanto foi descontado), não como um valor a
# MAGIC subtrair de `amount` novamente. Essa premissa é necessária porque o
# MAGIC enunciado não especifica a relação entre as duas colunas, e não há como
# MAGIC confirmar contra uma fonte externa.

# COMMAND ----------

df_gold_fact_sales = df_fact.withColumn("net_amount", F.col("amount"))

(df_gold_fact_sales.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(TABLE_GOLD_FACT_SALES))

print(f"Fato de vendas Gold: {df_gold_fact_sales.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Perguntas Analíticas
# MAGIC
# MAGIC Cada pergunta é respondida como uma tabela Delta própria
# MAGIC (`gold_kpi_*`), já pronta para ser conectada diretamente a um visual no
# MAGIC Power BI/Excel, sem necessidade de escrever DAX/fórmulas complexas na
# MAGIC ferramenta de BI.
# MAGIC
# MAGIC **Premissa geral:** todas as métricas de receita consideram **todos os
# MAGIC status de pedido** (`completed` e `cancelled`), exceto quando a própria
# MAGIC pergunta trata do cancelamento em si. Isso é uma decisão de modelagem --
# MAGIC em um cenário real, valeria confirmar com o time de negócio se pedidos
# MAGIC cancelados devem ou não compor a receita.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Receita total por mês

# COMMAND ----------

gold_kpi_receita_mensal = (
    df_gold_fact_sales
    .groupBy("year", "month")
    .agg(F.round(F.sum("net_amount"), 2).alias("receita_total"))
    .join(df_gold_dim_time.select("year", "month", "month_name"), on=["year", "month"], how="left")
    .select("year", "month", "month_name", "receita_total")
    .orderBy("year", "month")
)
gold_kpi_receita_mensal.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_receita_mensal")
display(gold_kpi_receita_mensal)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Ticket médio por pedido

# COMMAND ----------

gold_kpi_ticket_medio = df_gold_fact_sales.agg(
    F.round(F.avg("net_amount"), 2).alias("ticket_medio"),
    F.count("order_id").alias("qtd_pedidos"),
)
gold_kpi_ticket_medio.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_ticket_medio")
display(gold_kpi_ticket_medio)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Top 5 produtos por receita
# MAGIC
# MAGIC **Decisão:** o registro sentinela (`product_id = -1`, "Não Cadastrado") é
# MAGIC excluído deste ranking. Embora tecnicamente correto incluí-lo (é a soma
# MAGIC real de vendas de produtos sem cadastro), ele não representa um produto
# MAGIC de negócio acionável -- apareceria distorcendo o Top 5 com um "produto"
# MAGIC que na verdade é um agregado de falhas de cadastro. Esse valor é
# MAGIC reportado separadamente na seção 4.3.1, como métrica de qualidade de dado.

# COMMAND ----------

gold_kpi_top5_produtos_receita = (
    df_gold_fact_sales.filter(F.col("product_id") != -1)
    .groupBy("product_id")
    .agg(F.round(F.sum("net_amount"), 2).alias("receita_total"))
    .join(df_dim_product, on="product_id", how="left")
    .select("product_id", "product_name", "category", "receita_total")
    .orderBy(F.col("receita_total").desc())
    .limit(5)
)
gold_kpi_top5_produtos_receita.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_top5_produtos_receita")
display(gold_kpi_top5_produtos_receita)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.3.1 Receita de produtos sem cadastro (métrica de qualidade, à parte)

# COMMAND ----------

receita_produto_nao_cadastrado = (
    df_gold_fact_sales.filter(F.col("product_id") == -1)
    .agg(F.round(F.sum("net_amount"), 2).alias("receita_produtos_nao_cadastrados"))
    .collect()[0][0]
)
print(f"Receita de vendas com produto não cadastrado: R$ {receita_produto_nao_cadastrado:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Top 5 produtos por quantidade vendida
# MAGIC
# MAGIC Mesma decisão do item anterior: `product_id = -1` excluído do ranking.

# COMMAND ----------

gold_kpi_top5_produtos_quantidade = (
    df_gold_fact_sales.filter(F.col("product_id") != -1)
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("quantidade_total"))
    .join(df_dim_product, on="product_id", how="left")
    .select("product_id", "product_name", "category", "quantidade_total")
    .orderBy(F.col("quantidade_total").desc())
    .limit(5)
)
gold_kpi_top5_produtos_quantidade.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_top5_produtos_quantidade")
display(gold_kpi_top5_produtos_quantidade)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Top 5 vendedores por receita gerada
# MAGIC
# MAGIC Mesma decisão: `seller_id = -1` (vendedor não cadastrado, caso do seller
# MAGIC `3`) excluído do ranking, reportado separadamente a seguir.

# COMMAND ----------

gold_kpi_top5_vendedores_receita = (
    df_gold_fact_sales.filter(F.col("seller_id") != -1)
    .groupBy("seller_id")
    .agg(F.round(F.sum("net_amount"), 2).alias("receita_total"))
    .join(df_dim_seller, on="seller_id", how="left")
    .select("seller_id", "seller_name", "state", "receita_total")
    .orderBy(F.col("receita_total").desc())
    .limit(5)
)
gold_kpi_top5_vendedores_receita.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_top5_vendedores_receita")
display(gold_kpi_top5_vendedores_receita)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.5.1 Receita de vendedores sem cadastro (métrica de qualidade, à parte)

# COMMAND ----------

receita_vendedor_nao_cadastrado = (
    df_gold_fact_sales.filter(F.col("seller_id") == -1)
    .agg(F.round(F.sum("net_amount"), 2).alias("receita_vendedores_nao_cadastrados"))
    .collect()[0][0]
)
print(f"Receita de vendas com vendedor não cadastrado: R$ {receita_vendedor_nao_cadastrado:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5.2 Consolidado de qualidade de dados (para o dashboard)
# MAGIC
# MAGIC Reúne as métricas de "vendas sem cadastro" numa única tabela pequena,
# MAGIC pensada para alimentar um card de "Qualidade dos Dados" no dashboard --
# MAGIC dá visibilidade contínua ao problema de cadastro incompleto, sem misturar
# MAGIC com os rankings de negócio.

# COMMAND ----------

qtd_vendas_produto_nao_cadastrado = df_gold_fact_sales.filter(F.col("product_id") == -1).count()
qtd_vendas_vendedor_nao_cadastrado = df_gold_fact_sales.filter(F.col("seller_id") == -1).count()
total_vendas_gold = df_gold_fact_sales.count()

# ATENÇÃO: somar receita_produto_nao_cadastrado + receita_vendedor_nao_cadastrado
# diretamente contaria em dobro qualquer venda que tenha AO MESMO TEMPO produto
# E vendedor não cadastrados. Por isso calculamos a receita/contagem "sem
# cadastro completo" com um filtro OR direto sobre o fato, garantindo que cada
# venda problemática seja contada uma única vez, mesmo que viole os dois
# cadastros simultaneamente.
df_sem_cadastro_completo = df_gold_fact_sales.filter((F.col("product_id") == -1) | (F.col("seller_id") == -1))
qtd_vendas_sem_cadastro_completo = df_sem_cadastro_completo.count()
receita_sem_cadastro_completo = (
    df_sem_cadastro_completo.agg(F.round(F.sum("net_amount"), 2)).collect()[0][0] or 0.0
)

gold_kpi_qualidade_dados = spark.createDataFrame(
    [(
        total_vendas_gold,
        qtd_vendas_produto_nao_cadastrado,
        qtd_vendas_vendedor_nao_cadastrado,
        receita_produto_nao_cadastrado,
        receita_vendedor_nao_cadastrado,
        round((qtd_vendas_produto_nao_cadastrado / total_vendas_gold) * 100, 2) if total_vendas_gold else 0,
        round((qtd_vendas_vendedor_nao_cadastrado / total_vendas_gold) * 100, 2) if total_vendas_gold else 0,
        qtd_vendas_sem_cadastro_completo,
        receita_sem_cadastro_completo,
        round((qtd_vendas_sem_cadastro_completo / total_vendas_gold) * 100, 2) if total_vendas_gold else 0,
    )],
    schema=[
        "total_vendas", "qtd_vendas_produto_nao_cadastrado", "qtd_vendas_vendedor_nao_cadastrado",
        "receita_produto_nao_cadastrado", "receita_vendedor_nao_cadastrado",
        "pct_vendas_produto_nao_cadastrado", "pct_vendas_vendedor_nao_cadastrado",
        "qtd_vendas_sem_cadastro_completo", "receita_sem_cadastro_completo",
        "pct_vendas_sem_cadastro_completo",
    ],
)
gold_kpi_qualidade_dados.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_qualidade_dados")
display(gold_kpi_qualidade_dados)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5.3 Identificação dos vendedores sem cadastro (ação para o time de cadastro)
# MAGIC
# MAGIC A métrica agregada acima (29,88% das vendas) não diz **qual** `seller_id`
# MAGIC está sem cadastro -- informação essencial para o time responsável agir na
# MAGIC causa raiz. Recuperamos o `seller_id` original a partir do nome do arquivo
# MAGIC (`source_file`), preservado mesmo após a reconciliação para -1.
# MAGIC
# MAGIC **Limitação identificada:** o mesmo não é possível para `product_id` sem
# MAGIC cadastro -- diferente do vendedor, o produto não é codificado no nome do
# MAGIC arquivo, e o `product_id` original é sobrescrito pela reconciliação sem
# MAGIC ser preservado em uma coluna à parte na Silver atual. Registrado como
# MAGIC melhoria futura: manter uma coluna `product_id_original` no fato,
# MAGIC paralela à `product_id` reconciliada.

# COMMAND ----------

gold_kpi_vendedores_sem_cadastro = (
    df_gold_fact_sales
    .filter(F.col("seller_id") == -1)
    .withColumn("seller_id_original", F.regexp_extract(F.col("source_file"), r"^(\d+)_", 1).cast("int"))
    .groupBy("seller_id_original")
    .agg(
        F.count("*").alias("qtd_vendas"),
        F.round(F.sum("net_amount"), 2).alias("receita_afetada"),
    )
    .orderBy(F.col("receita_afetada").desc())
)
gold_kpi_vendedores_sem_cadastro.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_vendedores_sem_cadastro")
display(gold_kpi_vendedores_sem_cadastro)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Vendedores recorrentes vs. novos
# MAGIC
# MAGIC **Premissa assumida:** como o dataset cobre um único ano (2025) sem
# MAGIC histórico anterior para comparação, "recorrente" é definido como um
# MAGIC vendedor com vendas em **mais de um mês distinto**; "novo" é o vendedor
# MAGIC com vendas em **apenas um mês** dentro do período observado. Em um cenário
# MAGIC real com histórico multianual, a definição ideal seria "vendeu em anos
# MAGIC anteriores" vs. "primeira venda no período corrente".

# COMMAND ----------

meses_por_vendedor = (
    df_gold_fact_sales.filter(F.col("seller_id") != -1)
    .groupBy("seller_id")
    .agg(F.countDistinct("year", "month").alias("meses_distintos_com_venda"))
)

gold_kpi_vendedores_recorrentes_novos = (
    meses_por_vendedor
    .withColumn(
        "classificacao",
        F.when(F.col("meses_distintos_com_venda") > 1, "Recorrente").otherwise("Novo"),
    )
    .join(df_dim_seller, on="seller_id", how="left")
    .select("seller_id", "seller_name", "meses_distintos_com_venda", "classificacao")
    .orderBy(F.col("meses_distintos_com_venda").desc())
)
gold_kpi_vendedores_recorrentes_novos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_vendedores_recorrentes_novos")
display(gold_kpi_vendedores_recorrentes_novos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.7 Percentual de pedidos cancelados

# COMMAND ----------

total_pedidos = df_gold_fact_sales.count()
pedidos_cancelados = df_gold_fact_sales.filter(F.col("status") == "cancelled").count()
pct_cancelados = round((pedidos_cancelados / total_pedidos) * 100, 2) if total_pedidos else 0

gold_kpi_pct_cancelados = spark.createDataFrame(
    [(total_pedidos, pedidos_cancelados, pct_cancelados)],
    schema=["total_pedidos", "pedidos_cancelados", "percentual_cancelados"],
)
gold_kpi_pct_cancelados.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_pct_cancelados")
display(gold_kpi_pct_cancelados)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.8 Faturamento por estado
# MAGIC
# MAGIC Estado vem da dimensão de vendedor (`dim_seller.state`) -- o dataset não
# MAGIC traz estado do cliente/comprador, apenas do vendedor.

# COMMAND ----------

gold_kpi_faturamento_estado = (
    df_gold_fact_sales.join(df_dim_seller, on="seller_id", how="left")
    .groupBy("state")
    .agg(F.round(F.sum("net_amount"), 2).alias("receita_total"))
    .orderBy(F.col("receita_total").desc())
)
gold_kpi_faturamento_estado.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_faturamento_estado")
display(gold_kpi_faturamento_estado)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.9 Vendedores inativos (>30 dias sem venda)
# MAGIC
# MAGIC **Premissa assumida:** como o dataset é histórico (não há execução em
# MAGIC tempo real), a "data de referência" usada para calcular inatividade é a
# MAGIC **data mais recente presente no próprio dataset** (`MAX(sale_date)`), não
# MAGIC a data real de hoje. Em produção, essa referência seria `current_date()`.

# COMMAND ----------

data_referencia = df_gold_fact_sales.agg(F.max("sale_date")).collect()[0][0]
print(f"Data de referência usada para cálculo de inatividade: {data_referencia}")

ultima_venda_por_vendedor = (
    df_gold_fact_sales.filter(F.col("seller_id") != -1)
    .groupBy("seller_id")
    .agg(F.max("sale_date").alias("ultima_venda"))
    .withColumn("dias_sem_vender", F.datediff(F.lit(data_referencia), F.col("ultima_venda")))
)

gold_kpi_vendedores_inativos = (
    ultima_venda_por_vendedor
    .withColumn("inativo", F.col("dias_sem_vender") > 30)
    .join(df_dim_seller, on="seller_id", how="left")
    .select("seller_id", "seller_name", "ultima_venda", "dias_sem_vender", "inativo")
    .orderBy(F.col("dias_sem_vender").desc())
)
gold_kpi_vendedores_inativos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_vendedores_inativos")
display(gold_kpi_vendedores_inativos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.10 Diferença percentual entre mês atual e mês anterior, por vendedor
# MAGIC
# MAGIC **Achado durante a validação:** usar `lag()` diretamente sobre os meses em
# MAGIC que o vendedor teve venda produz uma comparação enganosa quando existe um
# MAGIC mês sem nenhuma venda no meio da sequência -- o `lag()` simplesmente pula
# MAGIC para o mês anterior disponível (ex.: compara março com janeiro, ignorando
# MAGIC que fevereiro não teve movimentação), sem deixar isso explícito.
# MAGIC
# MAGIC **Correção:** construímos a combinação completa de **todos os
# MAGIC vendedores x todos os 12 meses** (cross join com `gold_dim_time`),
# MAGIC preenchendo `0` para meses sem venda. Assim, a comparação "mês anterior"
# MAGIC sempre se refere ao mês calendário imediatamente anterior, mesmo que
# MAGIC tenha sido um mês sem nenhuma movimentação.

# COMMAND ----------

vendedores_ativos = df_dim_seller.filter(F.col("seller_id") != -1).select("seller_id")
todos_periodos = df_gold_dim_time.select("year", "month")

# Grade completa: todo vendedor cruzado com todo período do calendário
grade_completa = vendedores_ativos.crossJoin(todos_periodos)

receita_mensal_vendedor = (
    df_gold_fact_sales.filter(F.col("seller_id") != -1)
    .groupBy("seller_id", "year", "month")
    .agg(F.round(F.sum("net_amount"), 2).alias("receita_mes"))
)

# Left join da grade completa com a receita real -- meses sem venda ficam
# como null na receita_mes, e são preenchidos com 0 em seguida
receita_mensal_completa = (
    grade_completa
    .join(receita_mensal_vendedor, on=["seller_id", "year", "month"], how="left")
    .withColumn("receita_mes", F.coalesce(F.col("receita_mes"), F.lit(0.0)))
)

janela_vendedor_tempo = Window.partitionBy("seller_id").orderBy("year", "month")

gold_kpi_variacao_mensal_vendedor = (
    receita_mensal_completa
    .withColumn("receita_mes_anterior", F.lag("receita_mes").over(janela_vendedor_tempo))
    .withColumn(
        "variacao_percentual",
        F.when(
            F.col("receita_mes_anterior").isNotNull() & (F.col("receita_mes_anterior") != 0),
            F.round(((F.col("receita_mes") - F.col("receita_mes_anterior")) / F.col("receita_mes_anterior")) * 100, 2),
        ),
    )
    .join(df_dim_seller.select("seller_id", "seller_name"), on="seller_id", how="left")
    .select("seller_id", "seller_name", "year", "month", "receita_mes", "receita_mes_anterior", "variacao_percentual")
    .orderBy("seller_id", "year", "month")
)
gold_kpi_variacao_mensal_vendedor.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_variacao_mensal_vendedor")
display(gold_kpi_variacao_mensal_vendedor)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.11 Vendedores com queda de vendas por 3 meses consecutivos
# MAGIC
# MAGIC Reaproveita a tabela de variação mensal (4.10): marca cada mês em que a
# MAGIC receita caiu em relação ao mês anterior, depois verifica se existem 3
# MAGIC quedas consecutivas na sequência de meses de cada vendedor.

# COMMAND ----------

df_com_queda = gold_kpi_variacao_mensal_vendedor.withColumn(
    "teve_queda", (F.col("variacao_percentual") < 0).cast("int")
)

janela_sequencia = Window.partitionBy("seller_id").orderBy("year", "month")

# Técnica "gaps and islands": subtrai um contador sequencial do total
# acumulado de quedas -- quando o resultado se repete por 3 linhas seguidas,
# significa que houve 3 quedas consecutivas sem interrupção.
df_com_queda = (
    df_com_queda
    .withColumn("indice_mes", F.row_number().over(janela_sequencia))
    .withColumn("quedas_acumuladas", F.sum("teve_queda").over(janela_sequencia))
    .withColumn("grupo_sequencia", F.col("indice_mes") - F.col("quedas_acumuladas"))
)

tamanho_sequencia_queda = (
    df_com_queda.filter(F.col("teve_queda") == 1)
    .groupBy("seller_id", "grupo_sequencia")
    .agg(
        F.count("*").alias("meses_consecutivos_em_queda"),
        F.min("year").alias("ano_inicio"), F.min("month").alias("mes_inicio"),
        F.max("year").alias("ano_fim"), F.max("month").alias("mes_fim"),
    )
)

gold_kpi_quedas_consecutivas = (
    tamanho_sequencia_queda
    .filter(F.col("meses_consecutivos_em_queda") >= 3)
    .join(df_dim_seller.select("seller_id", "seller_name"), on="seller_id", how="left")
    .select("seller_id", "seller_name", "meses_consecutivos_em_queda", "ano_inicio", "mes_inicio", "ano_fim", "mes_fim")
    .orderBy(F.col("meses_consecutivos_em_queda").desc())
)
gold_kpi_quedas_consecutivas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_kpi_quedas_consecutivas")

print(f"Vendedores com 3+ meses consecutivos de queda: {gold_kpi_quedas_consecutivas.count()}")
display(gold_kpi_quedas_consecutivas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo das tabelas Gold criadas
# MAGIC
# MAGIC Todas prontas para conexão direta no Power BI/Excel via Databricks
# MAGIC Connector ou exportação.

# COMMAND ----------

tabelas_gold = [
    "gold_fact_sales", "gold_dim_seller", "gold_dim_product", "gold_dim_time",
    "gold_kpi_receita_mensal", "gold_kpi_ticket_medio",
    "gold_kpi_top5_produtos_receita", "gold_kpi_top5_produtos_quantidade",
    "gold_kpi_top5_vendedores_receita", "gold_kpi_vendedores_recorrentes_novos",
    "gold_kpi_pct_cancelados", "gold_kpi_faturamento_estado",
    "gold_kpi_vendedores_inativos", "gold_kpi_variacao_mensal_vendedor",
    "gold_kpi_quedas_consecutivas", "gold_kpi_qualidade_dados",
    "gold_kpi_vendedores_sem_cadastro",
]

print("Tabelas Gold disponíveis para consumo analítico:")
for t in tabelas_gold:
    print(f"  - {CATALOG}.{SCHEMA}.{t}")
