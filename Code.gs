/**
 * ============================================================================
 * Dashboard RPE — Backend (Code.gs)
 * ============================================================================
 * Conecta na SQL Statement Execution API do Databricks, consulta as tabelas
 * gold_kpi_* geradas pelo pipeline (notebooks/03_gold_analytics.py) e
 * devolve os dados já estruturados para o front-end (Index.html) renderizar.
 *
 * CONFIGURAÇÃO NECESSÁRIA ANTES DE USAR:
 * 1. Preencha DATABRICKS_HOST e WAREHOUSE_ID abaixo com os valores do seu
 *    workspace (Databricks -> SQL Warehouses -> seu warehouse -> Connection
 *    details).
 * 2. NÃO cole o token aqui diretamente. Em vez disso:
 *    - No editor do Apps Script, vá em "Configurações do projeto" (ícone de
 *      engrenagem) -> "Propriedades do script" -> "Adicionar propriedade do
 *      script"
 *    - Nome: DATABRICKS_TOKEN
 *    - Valor: cole seu Personal Access Token do Databricks
 *    Isso evita que o token fique visível no código-fonte / versionado no Git.
 * ============================================================================
 */

const CONFIG = {
  DATABRICKS_HOST: 'dbc-3850e46f-8340.cloud.databricks.com',
  WAREHOUSE_ID: '1045286bcb13a57e',
  CATALOG: 'workspace',
  SCHEMA: 'default',
};

/**
 * Ponto de entrada do Web App. Serve a página HTML principal.
 */
function doGet() {
  return HtmlService.createTemplateFromFile('Index')
    .evaluate()
    .setTitle('RPE — Relatório de Vendas')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/**
 * Permite incluir arquivos HTML/CSS/JS parciais dentro do template principal
 * (não usado nesta versão de arquivo único, mas mantido caso o dashboard
 * seja dividido em múltiplos arquivos futuramente).
 */
function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

/**
 * Executa uma query SQL contra o Databricks SQL Warehouse via API REST e
 * retorna os dados já convertidos em uma lista de objetos (um por linha),
 * usando os nomes das colunas retornados pela própria API.
 */
function runDatabricksQuery(sql) {
  const token = PropertiesService.getScriptProperties().getProperty('DATABRICKS_TOKEN');
  if (!token) {
    throw new Error(
      'Token do Databricks não configurado. Vá em Configurações do projeto > ' +
      'Propriedades do script e adicione DATABRICKS_TOKEN.'
    );
  }

  const url = `https://${CONFIG.DATABRICKS_HOST}/api/2.0/sql/statements`;
  const payload = {
    warehouse_id: CONFIG.WAREHOUSE_ID,
    statement: sql,
    wait_timeout: '30s',
  };

  const options = {
    method: 'post',
    contentType: 'application/json',
    headers: { Authorization: 'Bearer ' + token },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };

  const response = UrlFetchApp.fetch(url, options);
  const statusCode = response.getResponseCode();
  const body = JSON.parse(response.getContentText());

  if (statusCode !== 200) {
    throw new Error(
      `Erro na API do Databricks (status ${statusCode}): ${JSON.stringify(body)}`
    );
  }

  if (body.status && body.status.state === 'FAILED') {
    throw new Error(`Query falhou: ${JSON.stringify(body.status.error)}`);
  }

  const columns = body.manifest.schema.columns.map((c) => c.name);
  const rows = (body.result && body.result.data_array) || [];

  return rows.map((row) => {
    const obj = {};
    columns.forEach((colName, i) => {
      obj[colName] = row[i];
    });
    return obj;
  });
}

/**
 * Busca todos os dados necessários para o dashboard de uma vez, consultando
 * cada tabela gold_kpi_* correspondente. Chamado pelo front-end via
 * google.script.run.
 */
function getDashboardData() {
  const t = (nome) => `${CONFIG.CATALOG}.${CONFIG.SCHEMA}.${nome}`;

  try {
    const receitaMensal = runDatabricksQuery(
      `SELECT * FROM ${t('gold_kpi_receita_mensal')} ORDER BY year, month`
    );
    const ticketMedio = runDatabricksQuery(`SELECT * FROM ${t('gold_kpi_ticket_medio')}`);
    const top5ProdutosReceita = runDatabricksQuery(
      `SELECT * FROM ${t('gold_kpi_top5_produtos_receita')} ORDER BY receita_total DESC`
    );
    const top5ProdutosQuantidade = runDatabricksQuery(
      `SELECT * FROM ${t('gold_kpi_top5_produtos_quantidade')} ORDER BY quantidade_total DESC`
    );
    const top5VendedoresReceita = runDatabricksQuery(
      `SELECT * FROM ${t('gold_kpi_top5_vendedores_receita')} ORDER BY receita_total DESC`
    );
    const vendedoresRecorrentesNovos = runDatabricksQuery(
      `SELECT * FROM ${t('gold_kpi_vendedores_recorrentes_novos')} ORDER BY meses_distintos_com_venda DESC`
    );
    const pctCancelados = runDatabricksQuery(`SELECT * FROM ${t('gold_kpi_pct_cancelados')}`);
    const faturamentoEstado = runDatabricksQuery(
      `SELECT * FROM ${t('gold_kpi_faturamento_estado')} ORDER BY receita_total DESC`
    );
    const vendedoresInativos = runDatabricksQuery(
      `SELECT * FROM ${t('gold_kpi_vendedores_inativos')} ORDER BY dias_sem_vender DESC`
    );
    const variacaoMensalVendedor = runDatabricksQuery(
      `SELECT * FROM ${t('gold_kpi_variacao_mensal_vendedor')} ORDER BY seller_id, year, month`
    );
    const quedasConsecutivas = runDatabricksQuery(
      `SELECT * FROM ${t('gold_kpi_quedas_consecutivas')} ORDER BY meses_consecutivos_em_queda DESC`
    );
    const qualidadeDados = runDatabricksQuery(`SELECT * FROM ${t('gold_kpi_qualidade_dados')}`);

    return {
      ok: true,
      atualizadoEm: new Date().toISOString(),
      receitaMensal,
      ticketMedio: ticketMedio[0] || {},
      top5ProdutosReceita,
      top5ProdutosQuantidade,
      top5VendedoresReceita,
      vendedoresRecorrentesNovos,
      pctCancelados: pctCancelados[0] || {},
      faturamentoEstado,
      vendedoresInativos,
      variacaoMensalVendedor,
      quedasConsecutivas,
      qualidadeDados: qualidadeDados[0] || {},
    };
  } catch (erro) {
    return { ok: false, erro: erro.message };
  }
}
