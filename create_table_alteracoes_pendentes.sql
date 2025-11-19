-- Criar tabela de alterações pendentes no BigQuery
-- Execute este SQL no BigQuery Console ou via Python

CREATE TABLE IF NOT EXISTS `kanastra-live.finance.alteracoes_pendentes` (
  id STRING NOT NULL,
  usuario STRING,
  timestamp TIMESTAMP NOT NULL,
  tipo_alteracao STRING NOT NULL,  -- INSERT ou UPDATE
  tabela STRING NOT NULL,  -- fee_minimo, fee_variavel ou waiver
  dados JSON NOT NULL,  -- Dados da alteração em formato JSON
  status STRING DEFAULT 'PENDENTE',  -- PENDENTE, APROVADO, REJEITADO
  solicitacao_id STRING,  -- UUID para agrupar linhas relacionadas (ex: 2 linhas de taxa mínima)
  aprovador_por STRING  -- Quem aprovou/rejeitou
);

-- Comentários:
-- id: UUID único para cada linha de alteração
-- usuario: Email ou nome do usuário que criou a alteração
-- timestamp: Quando foi criada
-- tipo_alteracao: INSERT ou UPDATE
-- tabela: Qual tabela será afetada (fee_minimo, fee_variavel, waiver)
-- dados: JSON com todos os campos da alteração
-- status: Controle de aprovação (PENDENTE, APROVADO, REJEITADO)
-- solicitacao_id: UUID comum para agrupar alterações relacionadas na mesma solicitação
--                  Exemplo: taxa mínima cria 2 linhas com mesmo solicitacao_id
-- aprovador_por: Nome do aprovador que processou a solicitação

-- Para adicionar a coluna solicitacao_id em tabela existente:
ALTER TABLE `kanastra-live.finance.alteracoes_pendentes`
ADD COLUMN IF NOT EXISTS solicitacao_id STRING;
