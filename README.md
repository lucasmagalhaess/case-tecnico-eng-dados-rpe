# Case Técnico — Analista de Dados Pleno (RPE)

Pipeline de dados em Databricks (arquitetura medallion) e entrega analítica a
partir de arquivos de vendas mensais de vendedores, com dados propositalmente
sujos (duplicidade, dimensões incompletas, schema inconsistente, dados
atrasados).

## Sumário

- [Arquitetura](#arquitetura)
- [Estrutura do repositório](#estrutura-do-repositório)
- [Como executar](#como-executar)
- [Decisões de tratamento de dados](#decisões-de-tratamento-de-dados)
- [Modelagem (Star Schema)](#modelagem-star-schema)
- [Estratégia incremental](#estratégia-incremental)
- [Orquestração](#orquestração)
- [Qualidade e confiabilidade dos dados](#qualidade-e-confiabilidade-dos-dados)
- [Perguntas analíticas](#perguntas-analíticas)
- [Dashboard](#dashboard)

## Arquitetura

```
CSV (Volume Unity Catalog)
        │
        ▼
   ┌─────────┐   append, sem transformação, quarentena de arquivos inválidos
   │ BRONZE  │   extração de seller_id/year/month do nome do arquivo
   └────┬────┘
        │
        ▼
   ┌─────────┐   incremental (watermark + MERGE), deduplicação, tipagem,
   │ SILVER  │   reconciliação de dimensões incompletas, qualidade
   └────┬────┘
        │
        ▼
   ┌─────────┐   Star Schema final + 11 métricas de negócio
   │  GOLD   │   prontas para consumo em ferramenta de BI
   └────┬────┘
        │
        ▼
   Dashboard (Google Apps Script)
```

Todas as camadas são persistidas em **Delta Lake**, sobre um Volume gerenciado
pelo Unity Catalog no Databricks Free Edition (sem necessidade de conta AWS
própria).

## Estrutura do repositório

```
case-tecnico-rpe/
├── README.md                              <- este arquivo
├── notebooks/
│   ├── 01_bronze_ingestion.py             <- camada Bronze
│   ├── 02_silver_transformation.py        <- camada Silver (incremental)
│   └── 03_gold_analytics.py               <- camada Gold + KPIs
├── docs/
│   └── 01_analise_exploratoria_decisoes.md <- achados e decisões de tratamento
├── orchestration/
│   └── job_config.json                    <- documentação do Job (Bronze->Silver->Gold)
└── dashboard/
    └── (dashboard e instruções de conexão)
```

## Como executar

1. Criar conta gratuita no [Databricks Free Edition](https://login.databricks.com/signup).
2. Criar um Volume gerenciado em `workspace.default.raw_sales` e subir os
   arquivos CSV fornecidos (vendas + `dim_product.csv` + `dim_seller.csv`).
3. Importar os 3 notebooks da pasta `notebooks/` para o Workspace (via
   **Import**, mantém a formatação de células automaticamente).
4. Executar na ordem: `01_bronze_ingestion` → `02_silver_transformation` →
   `03_gold_analytics`.
5. (Opcional) Criar o Job de orquestração conforme `orchestration/job_config.json`,
   encadeando as 3 tasks com dependência sequencial.

Cada notebook contém checagens de qualidade (`assert`) que interrompem a
execução caso algo inesperado seja detectado, em vez de prosseguir com dado
potencialmente inconsistente.

## Decisões de tratamento de dados

Toda a análise exploratória inicial e as decisões tomadas para cada cenário
proposto no enunciado (duplicidade, dimensões incompletas, schema evoluindo,
produtos/vendedores sem cadastro, dados atrasados) estão documentadas em
detalhe em [`docs/01_analise_exploratoria_decisoes.md`](docs/01_analise_exploratoria_decisoes.md).

Resumo dos principais achados:

| Cenário | Tratamento |
|---|---|
| Dados duplicados | Deduplicação por `seller_id+year+month+order_id` (não apenas `order_id` — ver nota abaixo), mantendo o registro mais recente por `ingestion_timestamp` |
| `order_id` não é chave única global | Descoberto durante validação: o mesmo `order_id` pode se repetir em meses diferentes do mesmo vendedor, representando vendas reais distintas. Corrigido para chave composta. |
| Dimensões incompletas | Left join + registro sentinela "unknown member" (`seller_id`/`product_id` = -1) para não descartar vendas legítimas |
| Vendedor/produto sem cadastro | Mesma lógica acima — reportado separadamente como métrica de qualidade, excluído dos rankings Top 5 de negócio |
| Evolução de schema (`_v2`) | Ingestão tolerante na Bronze (`allowMissingColumns`), reconciliação de nulos na Silver (`discount`→0, timestamp com fallback) |
| Arquivo com schema inesperado | Quarentena já na Bronze (`INVALID_FILE.csv`, `abc_2025_99_sales.csv`), não promovido às camadas seguintes |
| Valores malformados (strings vazias em colunas numéricas) | `try_cast` em vez de `cast` rígido — converte para `null` em vez de derrubar o pipeline; linhas afetadas são isoladas e quantificadas |
| Meses sem movimentação | Interpretado como zero vendas (não preenchido artificialmente), tratado explicitamente na análise de variação mensal via grade completa vendedor×período |

## Modelagem (Star Schema)

Fato de vendas (`gold_fact_sales`) no centro, com dimensões de vendedor
(`gold_dim_seller`), produto (`gold_dim_product`) e tempo (`gold_dim_time`).

**Por que Star Schema, e não Snowflake:**
- **Facilidade de entendimento:** é o padrão mais reconhecido em ferramentas de
  BI self-service — qualquer pessoa de negócio entende "fato no centro,
  dimensões ao redor" sem precisar navegar por sub-dimensões aninhadas.
- **Performance:** menos joins que um modelo normalizado, já que as dimensões
  não se ramificam.
- **Manutenção:** adicionar uma nova dimensão ou métrica não exige
  reestruturar o fato, só um novo join.

As dimensões de vendedor e produto incluem um registro sentinela (`-1`,
"Não cadastrado") — um padrão de modelagem dimensional conhecido como
*"unknown member"*, que evita valores nulos em joins e evita perder receita
real de vendas sem correspondência cadastral.

## Estratégia incremental

A camada Silver não reprocessa a Bronze inteira a cada execução. Em vez
disso:

1. Uma tabela de controle (`silver_load_control`) guarda o **watermark**
   (timestamp da última linha processada com sucesso).
2. Cada execução lê da Bronze **apenas** as linhas com `_ingested_at` posterior
   a esse watermark.
3. Os dados novos são inseridos/atualizados na Silver via **`MERGE INTO`**
   (upsert): se a chave `seller_id+year+month+order_id` já existe, atualiza
   somente se o novo registro for mais recente (`ingestion_timestamp`); caso
   contrário, insere.
4. O watermark avança ao final de cada execução bem-sucedida.

Essa abordagem também resolve **dados atrasados (late arriving data)**: uma
correção que chega em uma execução posterior é identificada pela chave
composta e aplicada via MERGE, sem duplicar o registro original.

## Orquestração

Job no Databricks encadeando as 3 camadas com dependência sequencial
(`bronze_ingestion` → `silver_transformation` → `gold_analytics`), cada task
com `max_retries=2` e intervalo de 60s entre tentativas. Configuração
documentada em [`orchestration/job_config.json`](orchestration/job_config.json).

Como cada camada persiste seu resultado em Delta antes da próxima iniciar, e a
Silver é incremental/idempotente via MERGE, reexecutar uma task falha não
duplica nem perde dado já processado com sucesso nas etapas anteriores.

## Qualidade e confiabilidade dos dados

- **Validação de qualidade pós-tratamento:** cada notebook contém `assert`
  automatizados checando contagem de nulos em colunas essenciais, ausência de
  duplicidade remanescente, e conferência de totais de receita entre camadas
  (a soma nunca pode divergir além do valor de duplicatas reais removidas).
- **Identificação de arquivos com problema:** arquivos com nome fora do padrão
  ou schema incompatível com o mínimo esperado (`order_id`, `product_id`) são
  desviados para uma tabela de quarentena (`bronze_quarantine_log`) com o
  motivo registrado, sem interromper o processamento dos demais arquivos.
- **Evitar contagem duplicada em reprocessamento:** a estratégia de MERGE por
  chave composta garante idempotência — reexecutar o pipeline sobre os mesmos
  arquivos não duplica vendas (validado executando o Job de orquestração
  múltiplas vezes e conferindo que a contagem de linhas na Silver permanece
  estável).

## Perguntas analíticas

Cada uma das 11 perguntas do enunciado é respondida por uma tabela Delta
própria (`gold_kpi_*`), calculada em `notebooks/03_gold_analytics.py`, pronta
para conexão direta com a ferramenta de BI:

1. Receita total por mês — `gold_kpi_receita_mensal`
2. Ticket médio por pedido — `gold_kpi_ticket_medio`
3. Top 5 produtos por receita — `gold_kpi_top5_produtos_receita`
4. Top 5 produtos por quantidade vendida — `gold_kpi_top5_produtos_quantidade`
5. Top 5 vendedores por receita — `gold_kpi_top5_vendedores_receita`
6. Vendedores recorrentes vs. novos — `gold_kpi_vendedores_recorrentes_novos`
7. Percentual de pedidos cancelados — `gold_kpi_pct_cancelados`
8. Faturamento por estado — `gold_kpi_faturamento_estado`
9. Vendedores inativos (>30 dias) — `gold_kpi_vendedores_inativos`
10. Variação percentual mês a mês por vendedor — `gold_kpi_variacao_mensal_vendedor`
11. Vendedores com queda em 3+ meses consecutivos — `gold_kpi_quedas_consecutivas`

Premissas assumidas em perguntas ambíguas (ex.: definição de "recorrente",
data de referência para inatividade) estão documentadas como comentários no
próprio notebook, na célula correspondente.

## Dashboard

Dashboard construído em **Google Apps Script**, conectado às tabelas
`gold_kpi_*` via API REST do Databricks (SQL Statement Execution API). Ver
detalhes e instruções de configuração em `dashboard/`.
