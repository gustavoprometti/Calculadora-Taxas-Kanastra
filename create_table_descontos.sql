-- Criar tabela de descontos no BigQuery
-- Esta tabela armazena os descontos aprovados que serão aplicados pela calculadora

CREATE TABLE IF NOT EXISTS `kanastra-live.finance.descontos` (
  id STRING NOT NULL,
  data_aplicacao TIMESTAMP NOT NULL,
  usuario STRING,  -- Quem criou/aprovou
  fund_id INT64 NOT NULL,  -- ID do fundo
  fund_name STRING,  -- Nome do fundo
  valor_desconto FLOAT64 NOT NULL,  -- Valor do desconto em R$
  tipo_desconto STRING NOT NULL,  -- 'Percentual' ou 'Fixo'
  percentual_desconto FLOAT64,  -- Se tipo=Percentual, valor em %
  origem STRING NOT NULL,  -- 'juridico' ou 'comercial'
  data_inicio DATE NOT NULL,  -- Início da vigência
  data_fim DATE,  -- Fim da vigência (NULL = indefinido)
  servico STRING,  -- Serviço específico ou NULL para todos
  observacao STRING,  -- Motivo/justificativa do desconto
  documento_referencia STRING  -- Número do processo, contrato, etc.
);

-- Índices para melhor performance
CREATE INDEX IF NOT EXISTS idx_descontos_fund_data 
ON `kanastra-live.finance.descontos`(fund_id, data_inicio, data_fim);

CREATE INDEX IF NOT EXISTS idx_descontos_origem 
ON `kanastra-live.finance.descontos`(origem, data_inicio);

-- Comentários:
-- id: UUID único do desconto
-- data_aplicacao: Quando foi aprovado e inserido
-- usuario: Quem aprovou o desconto
-- fund_id: ID do fundo que receberá o desconto
-- fund_name: Nome do fundo (para facilitar consultas)
-- valor_desconto: Valor em R$ (para tipo Fixo) ou base de cálculo
-- tipo_desconto: 'Percentual' (ex: 10% de desconto) ou 'Fixo' (ex: R$ 5.000)
-- percentual_desconto: Se tipo=Percentual, guardar % (ex: 10.0 = 10%)
-- origem: 'juridico' (ordem judicial) ou 'comercial' (acordo comercial)
-- data_inicio: Data em que o desconto começa a vigorar
-- data_fim: Data em que o desconto termina (NULL = indefinido)
-- servico: Se desconto aplica apenas a um serviço específico, NULL = todos
-- observacao: Justificativa, motivo, contexto
-- documento_referencia: Número do processo, contrato, ofício, etc.

-- Uso na calculadora:
-- SELECT * FROM finance.descontos
-- WHERE fund_id = ?
-- AND reference_dt >= data_inicio
-- AND (data_fim IS NULL OR reference_dt <= data_fim)
-- AND (servico IS NULL OR servico = ?)
