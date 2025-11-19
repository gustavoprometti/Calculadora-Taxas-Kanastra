# Fluxo de Dados - Sistema de Taxas Kanastra

## 📊 Arquitetura de Dados

### Tabelas de Origem (Alimentam a Calculadora)

```
┌─────────────────────────────────────────────────────────────────┐
│                    TABELAS USADAS PELA CALCULADORA              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. finance.fee_minimo                                          │
│     - Taxas mínimas fixas por fundo/serviço                     │
│     - Campos: fund_id, servico, fee_min, data_inicio, data_fim  │
│     - Vigência: WHERE ref_dt >= data_inicio AND                 │
│                      (data_fim IS NULL OR ref_dt <= data_fim)   │
│                                                                  │
│  2. finance.fee_variavel                                        │
│     - Taxas variáveis por faixas de PL                          │
│     - Campos: fund_id, servico, faixa, fee_variavel,            │
│               data_inicio, data_fim                              │
│     - Aplicação progressiva por faixa de patrimônio             │
│                                                                  │
│  3. finance.descontos (TABELA UNIFICADA)                        │
│     - Waivers + Descontos jurídicos + Descontos comerciais      │
│     - Campo categoria: 'waiver', 'desconto_juridico',           │
│                        'desconto_comercial'                      │
│     - Campos: fund_id (descontos), fund_name (waivers),         │
│               valor_desconto, tipo_desconto, origem,             │
│               data_inicio, data_fim                              │
│     - Tipos waiver: Provisionado/Nao_Provisionado               │
│     - Tipos desconto: Fixo (R$) ou Percentual (%)               │
│     - Origem: 'juridico' (ordem judicial) ou 'comercial'        │
│     - Deduz do total calculado                                  │
│                                                                  │
│  DEPRECATED:                                                     │
│  - finance.historico_waivers → Migrado para finance.descontos   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## 🔄 Workflow de Aprovação

### Fluxo Completo: Criação → Aprovação → Produção

```
┌─────────────────────────────────────────────────────────────────────┐
│ FASE 1: CRIAÇÃO (Editor ou Aprovador)                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Dashboard de Gestão (dashboard_gestao_taxas.py)                    │
│  ├─ Aba "Taxas": Criar/Editar Taxa Mínima ou Variável              │
│  ├─ Aba "Waivers": Criar Waiver                                     │
│  └─ Aba "Descontos": Criar Desconto (Jurídico/Comercial)           │
│                                                                      │
│  ↓ salvar_alteracao_pendente()                                      │
│                                                                      │
│  finance.alteracoes_pendentes                                       │
│  ├─ status: PENDENTE                                                │
│  ├─ tipo_operacao: INSERT/UPDATE                                    │
│  ├─ tipo_alteracao_categoria: taxa_minima/taxa_variavel/            │
│  │                             waiver/desconto                       │
│  ├─ origem: NULL (taxas/waivers) ou juridico/comercial (descontos) │
│  ├─ solicitacao_id: Agrupa linhas relacionadas                      │
│  └─ dados: JSON com todos os campos                                 │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

                              ↓

┌─────────────────────────────────────────────────────────────────────┐
│ FASE 2: APROVAÇÃO (Apenas Aprovador)                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Painel de Aprovação (dashboard_gestao_taxas.py)                    │
│  ├─ Exibe solicitações agrupadas por solicitacao_id                │
│  ├─ Mostra todas as linhas de cada solicitação                      │
│  └─ Botão "Aprovar Solicitação Completa"                            │
│                                                                      │
│  ↓ Ao clicar em Aprovar:                                            │
│                                                                      │
│  1. Executa INSERT/UPDATE nas tabelas de PRODUÇÃO:                  │
│     ├─ Taxa Mínima → finance.fee_minimo                             │
│     ├─ Taxa Variável → finance.fee_variavel                         │
│     ├─ Waiver → finance.descontos (categoria='waiver')              │
│     └─ Desconto → finance.descontos (categoria='desconto_X')        │
│                                                                      │
│  2. Salva no histórico (audit trail):                               │
│     └─ finance.historico_alteracoes                                 │
│        ├─ usuario_solicitante: Quem criou                           │
│        ├─ usuario_aprovador: Quem aprovou                           │
│        ├─ timestamp_solicitacao: Quando foi criada                  │
│        ├─ timestamp_aprovacao: Quando foi aprovada                  │
│        ├─ tipo_operacao: INSERT/UPDATE/DELETE                       │
│        ├─ tipo_alteracao: Categoria                                 │
│        ├─ origem: juridico/comercial (descontos)                    │
│        ├─ dados_antes: Estado anterior (NULL para INSERT)           │
│        └─ dados_depois: Estado novo                                 │
│                                                                      │
│  3. Atualiza status na tabela pendentes:                            │
│     └─ finance.alteracoes_pendentes                                 │
│        ├─ status: PENDENTE → APROVADO                               │
│        └─ aprovador_por: Nome do aprovador                          │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

                              ↓

┌─────────────────────────────────────────────────────────────────────┐
│ FASE 3: CONSUMO (Calculadora)                                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Dashboard de Cálculo (dashboard_sql_streamlit.py)                  │
│  Executa query principal (Calculadora 5.0.sql)                      │
│                                                                      │
│  1. Busca taxas ativas:                                             │
│     ├─ FROM finance.fee_minimo                                      │
│     │   WHERE reference_dt >= data_inicio                           │
│     │   AND (data_fim IS NULL OR reference_dt <= data_fim)          │
│     │                                                                │
│     └─ FROM finance.fee_variavel                                    │
│         WHERE reference_dt >= data_inicio                           │
│         AND (data_fim IS NULL OR reference_dt <= data_fim)          │
│                                                                      │
│  2. Calcula taxa efetiva:                                           │
│     └─ MAX(taxa_variavel_por_faixa, taxa_minima)                    │
│                                                                      │
│  3. Aplica ajustes (TABELA UNIFICADA):                             │
│     └─ FROM finance.descontos                                       │
│         WHERE (fund_id = ? OR fund_name = ?)                        │
│         AND reference_dt BETWEEN data_inicio AND data_fim           │
│         AND (servico IS NULL OR servico = ?)                        │
│                                                                      │
│         Categoria 'waiver':                                         │
│           - tipo='Provisionado': distribui valor por registros      │
│           - tipo='Nao_Provisionado': aplica no último registro      │
│                                                                      │
│         Categoria 'desconto_juridico' ou 'desconto_comercial':      │
│           - tipo='Fixo': deduz valor em R$                          │
│           - tipo='Percentual': aplica % de desconto                 │
│                                                                      │
│  4. Resultado final:                                                │
│     └─ Taxa calculada - Waivers - Descontos = Taxa Final           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## 📋 Categorias de Alterações

| Categoria | Tipo Operação | Origem | Tabela Destino | Categoria na Tabela | Usado na Calculadora |
|-----------|---------------|--------|----------------|---------------------|----------------------|
| taxa_minima | INSERT/UPDATE | NULL | fee_minimo | N/A | ✅ Sim |
| taxa_variavel | INSERT/UPDATE | NULL | fee_variavel | N/A | ✅ Sim |
| waiver | INSERT | NULL | descontos | 'waiver' | ✅ Sim |
| desconto | INSERT | juridico | descontos | 'desconto_juridico' | ✅ Sim |
| desconto | INSERT | comercial | descontos | 'desconto_comercial' | ✅ Sim |

## 🔍 Rastreabilidade Completa

### Origem dos Descontos

**Jurídico (origem='juridico')**
- Ordens judiciais
- Processos administrativos
- Decisões obrigatórias
- Documento de referência: número do processo

**Comercial (origem='comercial')**
- Acordos comerciais
- Negociações com clientes
- Descontos estratégicos
- Documento de referência: número do contrato

### Histórico de Alterações

Toda aprovação gera registro em `finance.historico_alteracoes`:
- **Quem**: usuario_solicitante + usuario_aprovador
- **Quando**: timestamp_solicitacao + timestamp_aprovacao
- **O quê**: tipo_operacao + tipo_alteracao + origem
- **Onde**: tabela + solicitacao_id
- **Como estava**: dados_antes (NULL para INSERT)
- **Como ficou**: dados_depois

## 🎯 Resumo: Onde a Calculadora Busca Dados

```sql
-- TAXAS
SELECT * FROM finance.fee_minimo
WHERE reference_dt >= data_inicio 
  AND (data_fim IS NULL OR reference_dt <= data_fim);

SELECT * FROM finance.fee_variavel
WHERE reference_dt >= data_inicio 
  AND (data_fim IS NULL OR reference_dt <= data_fim);

-- AJUSTES (TABELA UNIFICADA: WAIVERS + DESCONTOS)
SELECT * FROM finance.descontos
WHERE (fund_id = ? OR fund_name = ?)  -- fund_id para descontos, fund_name para waivers
  AND reference_dt >= data_inicio 
  AND (data_fim IS NULL OR reference_dt <= data_fim)
  AND (servico IS NULL OR servico = ?);

-- Filtrar por tipo específico:
-- WHERE categoria = 'waiver' → Waivers (Provisionado/Nao_Provisionado)
-- WHERE categoria = 'desconto_juridico' → Descontos por ordem judicial
-- WHERE categoria = 'desconto_comercial' → Descontos por acordo comercial
```

**Todas as tabelas** são alimentadas pelo workflow de aprovação!

---

## 📊 Simplificação Arquitetural

### Antes (2 tabelas):
- `finance.historico_waivers` → Waivers
- `finance.descontos` → Descontos jurídicos/comerciais

### Depois (1 tabela unificada):
- `finance.descontos` → Waivers + Descontos (campo `categoria` distingue)

**Benefícios:**
- ✅ Query única na calculadora para todos os ajustes
- ✅ Estrutura de dados consistente
- ✅ Facilita manutenção e auditoria
- ✅ Evita duplicação de lógica
