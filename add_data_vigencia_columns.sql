-- Adicionar colunas de data_inicio e data_fim nas tabelas de taxas
-- Execute este SQL no BigQuery Console

-- Tabela fee_minimo
ALTER TABLE `kanastra-live.finance.fee_minimo`
ADD COLUMN IF NOT EXISTS data_inicio DATE;

ALTER TABLE `kanastra-live.finance.fee_minimo`
ADD COLUMN IF NOT EXISTS data_fim DATE;

-- Tabela fee_variavel
ALTER TABLE `kanastra-live.finance.fee_variavel`
ADD COLUMN IF NOT EXISTS data_inicio DATE;

ALTER TABLE `kanastra-live.finance.fee_variavel`
ADD COLUMN IF NOT EXISTS data_fim DATE;

-- Comentários:
-- data_inicio: Data em que a taxa começa a vigorar (obrigatória)
-- data_fim: Data em que a taxa deixa de vigorar (NULL = vigência indefinida)
-- 
-- Uso na calculadora:
-- WHERE reference_dt >= data_inicio 
-- AND (data_fim IS NULL OR reference_dt <= data_fim)
