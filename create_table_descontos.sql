-- Criar tabela UNIFICADA de descontos e waivers no BigQuery
-- Esta tabela armazena TODOS os ajustes (waivers + descontos) que serão aplicados pela calculadora
-- Campo 'categoria' distingue: 'waiver', 'desconto_juridico', 'desconto_comercial'

CREATE TABLE IF NOT EXISTS `kanastra-live.finance.descontos` (
  id STRING NOT NULL,
  data_aplicacao TIMESTAMP NOT NULL,
  usuario STRING,  -- Quem criou/aprovou
  
  -- Identificação do fundo (fund_id para descontos, fund_name para waivers)
  fund_id INT64,  -- ID do fundo (NULL para waivers)
  fund_name STRING,  -- Nome do fundo
  
  -- Categoria do ajuste (CAMPO OBRIGATÓRIO)
  categoria STRING NOT NULL,  -- 'waiver', 'desconto_juridico', 'desconto_comercial'
  
  -- Valores do desconto/waiver
  valor_desconto FLOAT64 NOT NULL,  -- Valor em R$
  tipo_desconto STRING NOT NULL,  -- 'Percentual', 'Fixo', 'Provisionado', 'Nao_Provisionado'
  percentual_desconto FLOAT64,  -- Se tipo=Percentual, valor em %
  
  -- Origem (apenas para descontos, NULL para waivers)
  origem STRING,  -- 'juridico' ou 'comercial' (NULL para waivers)
  
  -- Período de vigência
  data_inicio DATE NOT NULL,  -- Início da vigência
  data_fim DATE,  -- Fim da vigência (NULL = indefinido)
  
  -- Especificidade
  servico STRING,  -- Serviço específico ou NULL para todos
  
  -- Metadados
  observacao STRING,  -- Motivo/justificativa
  documento_referencia STRING  -- Número do processo, contrato (para descontos)
);

-- Índices para melhor performance
CREATE INDEX IF NOT EXISTS idx_descontos_fund_id_data 
ON `kanastra-live.finance.descontos`(fund_id, data_inicio, data_fim);

CREATE INDEX IF NOT EXISTS idx_descontos_fund_name_data 
ON `kanastra-live.finance.descontos`(fund_name, data_inicio, data_fim);

CREATE INDEX IF NOT EXISTS idx_descontos_categoria 
ON `kanastra-live.finance.descontos`(categoria, data_inicio);

CREATE INDEX IF NOT EXISTS idx_descontos_origem 
ON `kanastra-live.finance.descontos`(origem, data_inicio);

-- Comentários:
-- id: UUID único do ajuste
-- data_aplicacao: Quando foi aprovado e inserido
-- usuario: Quem aprovou
-- fund_id: ID do fundo (NULL para waivers que usam fund_name)
-- fund_name: Nome do fundo
-- categoria: 'waiver' (desconto especial) / 'desconto_juridico' (ordem judicial) / 'desconto_comercial' (acordo)
-- valor_desconto: Valor em R$
-- tipo_desconto: Para waivers: 'Provisionado'/'Nao_Provisionado'. Para descontos: 'Percentual'/'Fixo'
-- percentual_desconto: Se tipo=Percentual, guardar % (ex: 10.0 = 10%)
-- origem: 'juridico' ou 'comercial' (apenas para descontos, NULL para waivers)
-- data_inicio: Data em que começa a vigorar
-- data_fim: Data em que termina (NULL = indefinido)
-- servico: Se aplica apenas a um serviço específico, NULL = todos
-- observacao: Justificativa, motivo, contexto
-- documento_referencia: Número do processo, contrato (para descontos)

-- Uso na calculadora (TABELA UNIFICADA):
-- SELECT * FROM finance.descontos
-- WHERE (fund_id = ? OR fund_name = ?)  -- fund_id para descontos, fund_name para waivers
-- AND reference_dt >= data_inicio
-- AND (data_fim IS NULL OR reference_dt <= data_fim)
-- AND (servico IS NULL OR servico = ?)
-- ORDER BY categoria, data_aplicacao
--
-- Filtrar por tipo específico:
-- WHERE categoria = 'waiver' → Waivers
-- WHERE categoria = 'desconto_juridico' → Descontos por ordem judicial
-- WHERE categoria = 'desconto_comercial' → Descontos por acordo comercial
