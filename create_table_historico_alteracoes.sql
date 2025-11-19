-- Criar tabela de histórico de alterações aprovadas no BigQuery
-- Esta tabela mantém o registro permanente de todas as alterações aprovadas

CREATE TABLE IF NOT EXISTS `kanastra-live.finance.historico_alteracoes` (
  id STRING NOT NULL,  -- UUID da alteração original
  usuario_solicitante STRING,  -- Quem criou a alteração
  usuario_aprovador STRING,  -- Quem aprovou
  timestamp_solicitacao TIMESTAMP NOT NULL,  -- Quando foi criada
  timestamp_aprovacao TIMESTAMP NOT NULL,  -- Quando foi aprovada
  tipo_operacao STRING NOT NULL,  -- INSERT, UPDATE, DELETE
  tipo_alteracao STRING NOT NULL,  -- taxa_minima, taxa_variavel, waiver, desconto
  origem STRING,  -- Para descontos: 'juridico' ou 'comercial', NULL para outros
  tabela STRING NOT NULL,  -- Tabela afetada
  dados_antes JSON,  -- Dados antes da alteração (NULL para INSERT)
  dados_depois JSON NOT NULL,  -- Dados após a alteração
  solicitacao_id STRING,  -- Agrupa linhas relacionadas
  observacao STRING  -- Comentários adicionais
);

-- Índices para melhor performance
CREATE INDEX IF NOT EXISTS idx_historico_timestamp 
ON `kanastra-live.finance.historico_alteracoes`(timestamp_aprovacao DESC);

CREATE INDEX IF NOT EXISTS idx_historico_tipo 
ON `kanastra-live.finance.historico_alteracoes`(tipo_alteracao, timestamp_aprovacao DESC);

CREATE INDEX IF NOT EXISTS idx_historico_solicitacao 
ON `kanastra-live.finance.historico_alteracoes`(solicitacao_id);

-- Comentários:
-- id: UUID único da alteração (mesmo id da tabela alteracoes_pendentes)
-- usuario_solicitante: Quem criou a solicitação
-- usuario_aprovador: Quem aprovou a alteração
-- timestamp_solicitacao: Data/hora da criação da solicitação
-- timestamp_aprovacao: Data/hora da aprovação
-- tipo_operacao: INSERT (criação), UPDATE (alteração), DELETE (remoção)
-- tipo_alteracao: Categoria (taxa_minima, taxa_variavel, waiver, desconto)
-- origem: Origem do desconto (juridico/comercial) - obrigatório apenas para descontos
-- tabela: Nome da tabela do BigQuery afetada
-- dados_antes: Estado anterior (para UPDATE), NULL para INSERT
-- dados_depois: Estado novo (para INSERT/UPDATE)
-- solicitacao_id: Agrupa múltiplas linhas da mesma solicitação
-- observacao: Comentários opcionais
