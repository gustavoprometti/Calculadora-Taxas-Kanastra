-- Adicionar colunas de categorização nas alterações pendentes
-- Execute este SQL no BigQuery Console

-- Adicionar coluna tipo_alteracao (categoria)
ALTER TABLE `kanastra-live.finance.alteracoes_pendentes`
ADD COLUMN IF NOT EXISTS tipo_alteracao_categoria STRING;

-- Adicionar coluna origem (para descontos)
ALTER TABLE `kanastra-live.finance.alteracoes_pendentes`
ADD COLUMN IF NOT EXISTS origem STRING;

-- Comentários:
-- tipo_alteracao: Mantém INSERT/UPDATE (tipo de operação)
-- tipo_alteracao_categoria: Nova coluna para categoria (taxa_minima, taxa_variavel, waiver, desconto)
-- origem: Para descontos - indica se veio de 'juridico' ou 'comercial'
--
-- Uso:
-- - Taxa Mínima: tipo_alteracao='INSERT', tipo_alteracao_categoria='taxa_minima', origem=NULL
-- - Taxa Variável: tipo_alteracao='UPDATE', tipo_alteracao_categoria='taxa_variavel', origem=NULL
-- - Waiver: tipo_alteracao='INSERT', tipo_alteracao_categoria='waiver', origem=NULL
-- - Desconto Jurídico: tipo_alteracao='INSERT', tipo_alteracao_categoria='desconto', origem='juridico'
-- - Desconto Comercial: tipo_alteracao='INSERT', tipo_alteracao_categoria='desconto', origem='comercial'
