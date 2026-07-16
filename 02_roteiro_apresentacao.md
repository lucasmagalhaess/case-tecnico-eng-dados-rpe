# Roteiro de Apresentação — Case Técnico RPE

> Guia de apoio, não um texto para decorar. Cada seção tem o essencial pra
> você falar com naturalidade, mais os números exatos pra não precisar
> memorizar.

---

## 1. Abertura (30s)

- O case pedia um pipeline Bronze/Silver/Gold em Databricks, com dados
  propositalmente "sujos", + entrega analítica com 11 perguntas de negócio.
- **Frase de abertura sugerida:** *"Além de resolver o que foi pedido, tratei
  isso como se fosse produção de verdade — testei cada decisão, não só
  implementei. Vou mostrar tanto o resultado quanto alguns problemas reais
  que só apareceram na validação, porque acho que isso mostra melhor como eu
  trabalho."*

---

## 2. Arquitetura geral (1min)

Desenha ou mostra o diagrama do README:
```
CSV → Bronze (bruto) → Silver (incremental) → Gold (Star Schema + KPIs) → Dashboard
```

- **Bronze:** ingestão fiel, sem transformação. Só extrai `seller_id`/`year`/`month`
  do **nome do arquivo** (não estava no conteúdo) e separa arquivos inválidos
  em quarentena.
- **Silver:** aqui mora toda a lógica de qualidade — deduplicação, tipagem,
  reconciliação de cadastros incompletos. É **incremental** (watermark + MERGE),
  não reprocessa tudo a cada execução.
- **Gold:** modelagem final em Star Schema + as 11 métricas, já prontas como
  tabelas para consumo direto no dashboard.

---

## 3. Achados da análise exploratória (2min)

Antes de codar, investiguei os arquivos manualmente. Principais achados:

| Achado | Como percebi |
|---|---|
| `1_2025_01_sales_v2.csv` tinha 2 colunas a menos | Comparei schema de vários arquivos lado a lado |
| `INVALID_FILE.csv` e `abc_2025_99_sales.csv` eram lixo proposital | Schema (`invalid_col`, `value`) sem relação com vendas |
| `dim_seller` tinha 5 registros, não 3 | Amostra inicial (`nrows=3`) enganava — vi a tabela completa |
| `dim_product` tinha 48 registros, não 3 | Mesma causa |
| `product_id` vinha como texto, não número | Ordenação saiu errada (`1, 11, 12...` em vez de `1, 2, 3...`) |
| Nomes de produto com capitalização inconsistente | `Notebook_1`, `notebook_2`, `NOTEBOOK_14` no mesmo dataset |

**Frase-chave:** *"Não tratei a amostra inicial como verdade absoluta — várias
premissas que pareciam óbvias no começo mudaram quando olhei o dado completo."*

---

## 4. Decisões de modelagem (2min)

### Star Schema
- Fato de vendas no centro + dimensões de vendedor, produto, tempo.
- Justificativa: mais simples pra BI, menos joins, mais fácil de manter que
  snowflake.

### Padrão "unknown member" (-1)
- Vendedor/produto sem cadastro não vira `null` nem some do relatório —
  aponta para um registro sentinela (`-1`, "Não cadastrado").
- **Por quê:** `null` quebra somas e gráficos de BI; inner join perderia
  receita real. Left join + sentinela resolve os dois problemas de uma vez.

### Left join, não inner ou right
- Inner perderia vendas sem cadastro (perigoso: subestima receita real).
- Right priorizaria o cadastro, descartando vendas legítimas.
- Left mantém 100% das vendas, sinalizando quem não tem cadastro.

---

## 5. Achados durante a validação (o mais importante — 3min)

Essa é a parte que mais vale destacar: **não confiei no código só porque
rodou sem erro.**

### `order_id` não é chave única global
- Suposição inicial: `order_id` identifica um pedido de forma única.
- **Ao inspecionar manualmente** os pedidos "duplicados", achei 2 padrões
  diferentes: duplicata real (mesmo período, dados idênticos) e **colisão de
  ID entre meses diferentes** (mesmo número, vendas reais distintas).
- 53 dos 192 `order_id` repetidos eram colisões, não duplicatas.
- **Correção:** chave composta `seller_id+year+month+order_id`.
- **Prova:** 4 testes específicos depois da correção, incluindo conferência
  de que a receita só caiu pelo valor exato das duplicatas reais (R$ 38.901,48
  em 144 linhas).

### Valores malformados quebrando o pipeline
- `cast()` rígido quebrava com string vazia numa coluna numérica.
- Troquei por `try_cast()` — converte pra `null` em vez de derrubar tudo — e
  isolei essas linhas numa "quarentena da Silver" para não contaminar métricas.

### Grade de tempo incompleta distorcendo variação mensal
- `lag()` simples pulava meses sem venda, comparando março com janeiro (não
  com fevereiro) sem avisar.
- Corrigi construindo uma grade completa vendedor × todos os 12 meses,
  preenchendo zero onde não havia venda.

### Dupla contagem na métrica de qualidade do dashboard
- Somar "receita produto não cadastrado" + "receita vendedor não cadastrado"
  contava 2x qualquer venda com os dois problemas ao mesmo tempo (31 vendas).
- Corrigido para filtro `OR` único direto no fato.

**Frase-chave:** *"Cada um desses foi encontrado testando, não pensando bonito
antes de rodar. Prefiro mostrar isso do que fingir que saiu perfeito de
primeira."*

---

## 6. Estratégia incremental (1min30s)

- Perguntei a mim mesmo (e me perguntaram): reprocessar tudo sempre é
  suficiente pro requisito, mas não é o diferencial que o enunciado pede.
- Implementei watermark + `MERGE INTO`: cada execução só processa o que é
  novo desde a última rodada.
- **Prova ao vivo, se quiser mostrar:** rodei a Silver duas vezes seguidas —
  a segunda reconheceu "nada novo" e parou sem reprocessar.
- Isso também resolve **dados atrasados (late arriving data)** "de graça":
  uma correção que chega depois é identificada pela chave composta e
  atualizada via MERGE, sem duplicar.

---

## 7. Orquestração (1min)

- Job no Databricks: `bronze_ingestion → silver_transformation → gold_analytics`,
  com dependência sequencial e `max_retries=2`.
- Testado rodando múltiplas vezes — contagem da Silver permanece estável
  (idempotência confirmada via `SELECT COUNT(*)`).
- Configuração documentada em `orchestration/job_config.json`, versionada no
  Git (não é só clique na tela sem registro).

---

## 8. Qualidade e testes (1min30s)

- **Automatizados:** `assert` em Bronze e Silver — zero nulos essenciais,
  zero duplicidade remanescente, soma de receita batendo entre camadas.
- **Manuais:** os achados da seção 5 vieram de inspeção ativa, não só de
  testes formais passando verde.
- **Dashboard:** cada métrica exibida foi reconferida por query SQL direta,
  depois de pronto — nenhuma divergência na validação final.

---

## 9. Dashboard (2min)

- Requisito central do enunciado (Power BI/Excel) foi substituído por
  **Google Apps Script**, autorizado previamente pela RPE.
- Identidade visual própria: paleta azul-marinho + laranja da marca,
  tipografia própria, card "recibo" de resumo (referência ao negócio de
  pagamentos/varejo da RPE).
- 4 abas: Visão Geral, Produtos, Vendedores, Regional & Qualidade — cobrindo
  as 11 perguntas do enunciado + painel de qualidade de dado.
- Conecta ao vivo na API SQL do Databricks (botão "Atualizar dados").
- Token de acesso guardado via Script Properties, não commitado no código.

---

## 10. Perguntas prováveis + respostas curtas

**"Por que o -1 na dimensão?"**
> Padrão de modelagem "unknown member": evita perder receita real de vendas
> sem cadastro, sem gerar null que quebraria somas no BI.

**"Por que left join e não inner?"**
> Inner descartaria vendas legítimas sem correspondência na dimensão —
> subestimaria a receita real.

**"Por que chave composta na deduplicação, e não só order_id?"**
> Descobri testando que order_id se repete entre meses diferentes do mesmo
> vendedor, representando vendas reais distintas — não duplicatas.

**"O que é 'quarentena'?"**
> Arquivos que não correspondem ao domínio de vendas (nome ou schema
> inválido) são isolados já na Bronze, registrados com o motivo, sem travar
> o resto do pipeline.

**"Por que estratégia incremental, e não overwrite?"**
> overwrite é suficiente pro requisito obrigatório, mas o enunciado cita
> incremental como diferencial — implementei watermark + MERGE, testável e
> comprovadamente idempotente.

**"O amount já vem líquido de desconto?"**
> Premissa assumida e documentada: o enunciado não esclarece, então tratei
> `amount` como valor final e `discount` como informação complementar,
> deixando isso explícito no notebook.

---

## 11. Encerramento

- Repositório completo no GitHub, com README, documentação de decisões,
  notebooks comentados, config de orquestração e dashboard.
- **Se perguntarem "o que faria diferente com mais tempo":**
  CI/CD (sincronização automática GitHub → Databricks via Asset Bundles),
  CDC de verdade caso a fonte fosse um banco operacional, e testes
  automatizados mais formais (ex.: `pytest` sobre funções de transformação
  isoladas).
