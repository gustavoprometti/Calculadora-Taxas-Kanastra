-- Tabela para registrar histórico de waivers aplicados
CREATE TABLE IF NOT EXISTS `kanastra-live.finance.historico_waivers` (
  id STRING NOT NULL,
  data_aplicacao TIMESTAMP NOT NULL,
  usuario STRING,
  fund_name STRING NOT NULL,
  valor_waiver FLOAT64 NOT NULL,
  tipo_waiver STRING NOT NULL,
  data_inicio DATE,
  data_fim DATE,
  observacao STRING
);
