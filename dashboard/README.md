# Dashboard — Google Apps Script

Dashboard de vendas construído como Web App em Google Apps Script, conectado
ao vivo às tabelas `gold_kpi_*` do Databricks via **SQL Statement Execution
API**. Substitui a entrega em Power BI/Excel prevista originalmente no
enunciado, mediante autorização prévia da RPE.

## Arquivos

| Arquivo | Papel |
|---|---|
| `Code.gs` | Backend: autentica e consulta o Databricks, monta o payload consumido pelo front-end |
| `Index.html` | Front-end: HTML/CSS/JS, identidade visual RPE, 4 abas, gráficos (Chart.js) |

## O que o dashboard cobre

- As 11 perguntas analíticas do enunciado (receita mensal, ticket médio,
  Top 5 produtos/vendedores, recorrência, cancelamento, faturamento por
  estado, inatividade, variação mensal, quedas consecutivas)
- Um painel de **Qualidade dos Dados**, com percentual de vendas sem
  cadastro completo e identificação de quais `seller_id` precisam ser
  regularizados junto ao time de cadastro

Organizado em 4 abas: **Visão Geral**, **Produtos**, **Vendedores**,
**Regional & Qualidade**.

## Como configurar do zero

1. Acesse [script.google.com](https://script.google.com) → **Novo projeto**.
2. Cole o conteúdo de `Code.gs` no arquivo de código padrão do projeto.
3. Crie um novo arquivo HTML chamado `Index` e cole o conteúdo de
   `Index.html`.
4. Em `Code.gs`, ajuste as constantes `DATABRICKS_HOST` e `WAREHOUSE_ID`
   para os valores do seu workspace (Databricks → SQL Warehouses → seu
   warehouse → Connection details).
5. **Nunca cole o token diretamente no código.** Vá em
   **Configurações do projeto** (ícone de engrenagem) → **Propriedades do
   script** → adicione uma propriedade `DATABRICKS_TOKEN` com o seu
   Personal Access Token do Databricks.
6. **Implantar** → **Nova implantação** → tipo **App da Web** → Executar
   como "Eu" → Acesso "Qualquer pessoa" (ou restrinja conforme necessário).
7. Autorize as permissões solicitadas e copie a URL gerada.

## Atualizando o dashboard após mudanças no código

Alterações salvas no editor **não atualizam automaticamente** a URL já
publicada — é preciso criar uma nova versão:
**Implantar → Gerenciar implantações → editar (ícone de lápis) → Versão:
"Nova versão" → Implantar.**

## Pré-requisito no Databricks

As tabelas `gold_kpi_*` consultadas por este dashboard são geradas pelo
notebook `notebooks/03_gold_analytics.py`. Execute o pipeline completo
(Bronze → Silver → Gold, manualmente ou via Job) antes de usar o botão
"Atualizar dados" pela primeira vez.
