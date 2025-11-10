-- Criar tabela de alterações pendentes no BigQuery
-- Execute este SQL no BigQuery Console ou via Python

CREATE TABLE IF NOT EXISTS `kanastra-live.dw_finance.alteracoes_pendentes` (
  id STRING NOT NULL,
  usuario STRING,
  timestamp TIMESTAMP NOT NULL,
  tipo_alteracao STRING NOT NULL,  -- INSERT ou UPDATE
  tabela STRING NOT NULL,  -- fee_minimo ou fee_variavel
  dados JSON NOT NULL,  -- Dados da alteração em formato JSON
  status STRING DEFAULT 'PENDENTE'  -- PENDENTE, APROVADO, REJEITADO
);

-- Comentários:
-- id: UUID único para cada alteração
-- usuario: Email ou nome do usuário que criou a alteração
-- timestamp: Quando foi criada
-- tipo_alteracao: INSERT ou UPDATE
-- tabela: Qual tabela será afetada
-- dados: JSON com todos os campos da alteração
-- status: Controle de aprovação
