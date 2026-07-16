# Análise Exploratória e Decisões de Tratamento — Case Técnico RPE

## 1. Objetivo desta etapa

Antes de construir o pipeline Bronze/Silver/Gold, foi feita uma inspeção manual do
schema e do conteúdo de uma amostra dos 33 arquivos disponibilizados no Volume
`/Volumes/workspace/default/raw_sales/`, com o objetivo de identificar os cenários
mencionados no enunciado (duplicidade, dimensões incompletas, produtos/vendedores
sem cadastro, evolução de schema, dados atrasados) antes de decidir como tratá-los.

## 2. Inventário de arquivos

- **31 arquivos de vendas** no padrão `SELLER_ID_YYYY_MM_sales.csv`, cobrindo os
  vendedores `1`, `3`, `4` e `5`, com meses não contínuos (nem todo vendedor tem
  todos os 12 meses de 2025 — ex.: seller `4` só tem o arquivo de `01`).
- **2 arquivos de dimensão**: `dim_product.csv` e `dim_seller.csv`.
- **2 arquivos fora do padrão**: `INVALID_FILE.csv` e `abc_2025_99_sales.csv`.

## 3. Schema identificado

| Arquivo(s) | Colunas |
|---|---|
| Vendas "padrão" (ex.: `1_2025_01_sales.csv`, `3_2025_02_sales.csv`, `5_2025_12_sales.csv`) | `order_id`, `product_id`, `quantity`, `amount`, `status`, `sale_date`, `discount`, `ingestion_timestamp` |
| `1_2025_01_sales_v2.csv` | `order_id`, `product_id`, `quantity`, `amount`, `status`, `sale_date` — **sem `discount` e sem `ingestion_timestamp`** |
| `INVALID_FILE.csv` e `abc_2025_99_sales.csv` | `invalid_col`, `value` — schema totalmente distinto, não é dado de vendas |
| `dim_product.csv` | `product_id`, `product_name`, `category` (3 produtos cadastrados: id 1, 2, 3) |
| `dim_seller.csv` | `seller_id`, `seller_name`, `state` (3 vendedores cadastrados: id 1, 2, 4) |

## 4. Cenários identificados e decisão de tratamento

### 4.1 Evolução de schema (`1_2025_01_sales_v2.csv`)

**Achado:** o arquivo `_v2` não é uma versão "mais completa" do original — ao
contrário, tem 2 colunas a menos (`discount` e `ingestion_timestamp` ausentes) e
apenas 1 registro (`order_id = 999999`), número fora do padrão de numeração
observado nos demais arquivos, o que pode indicar um caso de teste incluído
propositalmente no dataset (não há como confirmar com certeza).

**Decisão:** tratar como caso de **schema incompleto**, não como correção legítima.
Camada Bronze ingere o arquivo normalmente (preservando o dado como recebido, sem
julgamento), mas na Silver ele é sinalizado com schema divergente. Como critério de
prevalência entre `order_id` duplicados, será priorizado o registro com maior
completude de colunas e `ingestion_timestamp` mais recente — o que na prática
mantém o arquivo original (sem sufixo) como fonte de verdade para o `order_id`
comum, e o registro exclusivo do `_v2` (`999999`) é mantido como eventual novo
pedido, sinalizado por schema incompleto.

### 4.2 Arquivos com schema inesperado (`INVALID_FILE.csv`, `abc_2025_99_sales.csv`)

**Achado:** ambos têm colunas `invalid_col` / `value`, sem nenhuma relação com o
domínio de vendas. `abc_2025_99_sales.csv` também viola o padrão de nomenclatura
(`SELLER_ID` não numérico, mês `99` inválido).

**Decisão:** esses arquivos são desviados para uma rota de **quarentena** já na
camada Bronze — não são promovidos para Silver/Gold. Ficam registrados em uma
tabela/log de arquivos rejeitados, com o motivo (`schema inesperado` /
`nome fora do padrão`), permitindo auditoria posterior sem travar o pipeline.

### 4.3 Vendedores sem cadastro / vendedor cadastrado sem vendas

**Achado (revisado após inspeção completa de `dim_seller`, não apenas amostra):**
`dim_seller.csv` tem, na verdade, **5 registros** (sellers `1`, `2`, `4`, `5`, `6`) — a
inspeção inicial por amostra (3 primeiras linhas) havia sugerido erroneamente que
apenas os sellers 1, 2 e 4 estavam cadastrados. Confrontando com os arquivos de
venda:
- O seller **`3`** vende (existem arquivos `3_2025_XX_sales.csv`), mas **não tem
  cadastro** em `dim_seller`.
- O seller **`6`** está cadastrado em `dim_seller`, mas **não aparece em nenhum
  arquivo de venda** — é um vendedor cadastrado sem movimentação, o cenário
  inverso.

**Decisão:** para o seller sem cadastro (`3`), mantém-se o left join (vendas →
dimensão) com registro "genérico" (`seller_name = "Não cadastrado"`,
`state = "Desconhecido"`), como descrito abaixo. Para o seller cadastrado sem
vendas (`6`), nenhum tratamento especial é necessário — ele simplesmente não
aparece nas métricas de vendas (não há venda para agregar), o que é o
comportamento correto e esperado.

### 4.4 Produtos sem cadastro

**Achado:** `dim_product.csv` tem, na verdade, **48 produtos cadastrados** (não
apenas 3, como sugeriu a amostra inicial de 3 linhas). Ainda assim, é necessário
confirmar na Silver se todos os `product_id` vendidos têm correspondência nessa
lista mais completa — a proporção real de "produto sem cadastro" pode ser menor
do que a suposição inicial baseada em amostra.

**Decisão:** mesmo tratamento do item anterior — join do tipo `left`, com produto
"genérico" (`product_name = "Não cadastrado"`, `category = "Desconhecida"`) para
não descartar vendas na Gold, aplicado a qualquer `product_id` vendido que não
conste nos 48 cadastrados.

### 4.5 Achados adicionais de qualidade em `dim_product`

**`product_id` como string, não inteiro:** ao ordenar a dimensão por `product_id`,
o resultado veio `1, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 21...` em vez de
`1, 2, 3, 4...` — sinal de que a coluna está tipada como texto, e a ordenação
está sendo feita caractere por caractere. Isso não quebra o join em si (desde que
o tipo seja consistente entre fato e dimensão), mas deve ser corrigido na Silver
(cast explícito para inteiro), evitando bugs sutis em ordenações e comparações
numéricas nas camadas seguintes.

**Inconsistência de capitalização em `product_name`:** os nomes de produto
aparecem com padrões de escrita distintos entre registros — por exemplo,
`Notebook_1`, `notebook_2`, `NOTEBOOK_14`, `notebook_13`. Isso não afeta o join
(feito por `product_id`), mas inviabilizaria qualquer agregação futura feita por
nome de produto em vez de por ID (o sistema trataria variações de capitalização
como produtos diferentes). Decisão: padronizar a capitalização de
`product_name` na Silver (ex.: `initcap` ou `lower`), por precaução e clareza de
apresentação no dashboard.

### 4.6 Meses sem movimentação

**Achado:** nem todo vendedor tem arquivo para todos os meses de 2025 (ex.: seller
`4` só tem janeiro).

**Decisão:** a ausência de arquivo é interpretada como **ausência de venda no
período**, não como erro. Isso é relevante para as perguntas analíticas de
"vendedores inativos" e "queda de vendas por 3 meses consecutivos" — a métrica de
inatividade deve considerar a ausência de registros no período como zero de venda,
não como dado faltante a ser preenchido artificialmente.

### 4.7 Duplicidade / reprocessamento

**Achado (revisado após inspeção manual das linhas duplicadas):** a suposição
inicial era que `order_id` seria uma chave única global. Ao inspecionar os
pedidos com `order_id` repetido, identificamos **dois padrões distintos**:

1. **Duplicata real:** mesmo `seller_id`, `year`, `month`, `order_id`, e todos
   os demais valores (status, amount, sale_date) idênticos — o mesmo registro
   aparecendo mais de uma vez.
2. **Colisão de ID entre períodos diferentes:** mesmo `order_id`, mas
   `seller_id`/período e valores completamente diferentes — ou seja, **vendas
   reais e distintas** que coincidentemente compartilham o número de pedido
   (identificado em 53 dos 192 `order_id` com repetição). Isso é coerente com
   o próprio padrão de nomenclatura dos arquivos, que já é escopado por
   vendedor e período — sugerindo que a numeração de `order_id` também segue
   essa mesma lógica de escopo, não sendo garantidamente única entre
   diferentes arquivos.

**Decisão:** a chave de deduplicação foi corrigida de `order_id` (isolado) para
a combinação **`seller_id + year + month + order_id`**. Isso preserva
corretamente as vendas legítimas de períodos diferentes, ao mesmo tempo em que
remove duplicatas reais dentro do mesmo escopo vendedor+período. A
sobrevivência entre duplicatas reais continua sendo decidida pelo
`ingestion_timestamp` mais recente, resolvendo também a idempotência em caso
de reexecução do pipeline sobre os mesmos arquivos.

## 5. Resumo para apresentação

| Cenário do enunciado | Onde foi identificado | Tratamento |
|---|---|---|
| Dados duplicados | `order_id` repetido dentro do mesmo seller+período | Deduplicação por `seller_id+year+month+order_id`, mantendo o mais recente por `ingestion_timestamp` |
| Dimensões incompletas | `dim_seller` (5 registros) e `dim_product` (48 registros) não cobrem todos os IDs vendidos | Left join + registro "genérico" para não cadastrados |
| Produtos sem cadastro | IDs de produto vendidos fora dos 48 cadastrados em `dim_product` | Left join + categoria "Desconhecida" |
| Vendedor sem cadastro | Seller `3` vende mas não está em `dim_seller` | Left join + vendedor "Não cadastrado" |
| Vendedor cadastrado sem vendas | Seller `6` está em `dim_seller` mas não tem nenhum arquivo de venda | Nenhum tratamento especial — simplesmente não aparece nas métricas de venda |
| Meses sem movimentação | Ausência de arquivo para seller/mês | Interpretado como zero vendas, não como erro |
| Evolução de schema | `1_2025_01_sales_v2.csv` com 2 colunas a menos | Ingestão tolerante na Bronze + reconciliação por completude na Silver |
| Dados atrasados (late arriving) | `ingestion_timestamp` divergente do `sale_date` | Merge/upsert por `order_id` prioriza `ingestion_timestamp` mais recente |
| Arquivo com schema inesperado | `INVALID_FILE.csv`, `abc_2025_99_sales.csv` | Quarentena na Bronze, não promovido às camadas seguintes |
| `product_id` tipado como string | Ordenação lexicográfica incorreta em `dim_product` (`1, 11, 12...`) | Cast explícito para inteiro na Silver |
| `order_id` não é chave única global | 53 de 192 `order_id` repetidos eram, na verdade, vendas distintas de períodos diferentes com número coincidente | Chave de deduplicação composta (`seller_id+year+month+order_id`) em vez de `order_id` isolado |
| Capitalização inconsistente em `product_name` | `Notebook_1`, `notebook_2`, `NOTEBOOK_14` no mesmo dataset | Padronização de capitalização na Silver (ex.: `initcap`) |
