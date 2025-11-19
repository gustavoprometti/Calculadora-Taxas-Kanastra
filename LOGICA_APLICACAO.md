# Lógica de Aplicação - Taxas vs Ajustes

## 🎯 Dois Fluxos Distintos

### 1️⃣ **TAXAS (Base Permanente)**
**Tabelas:** `finance.fee_minimo` e `finance.fee_variavel`

```
Criação/Alteração → INSERT/UPDATE direto nas tabelas
                  ↓
         Calculadora lê SEMPRE
                  ↓
         Taxas aplicadas automaticamente
```

**Características:**
- ✅ Alteração **permanente** na estrutura de taxas
- ✅ Vigência controlada por `data_inicio`/`data_fim`
- ✅ Calculadora busca taxa ativa para a data de referência
- ✅ Exemplos: Mudar taxa mínima de R$ 1.000 → R$ 1.500

---

### 2️⃣ **AJUSTES (Descontos Temporários)**
**Tabela:** `finance.descontos` (UNIFICADA)

```
Waiver/Desconto criado → INSERT em finance.descontos
                       ↓
         Calculadora APLICA durante período de vigência
                       ↓
         Deduz do total calculado (não altera taxas base)
```

**Características:**
- ✅ **NÃO altera** as tabelas de taxas (`fee_minimo`/`fee_variavel`)
- ✅ Aplicado **APENAS durante período definido** (`data_inicio` a `data_fim`)
- ✅ Deduz do valor calculado pelas taxas base
- ✅ Categorias: `waiver`, `desconto_juridico`, `desconto_comercial`

---

## 📊 Estrutura da Tabela `finance.descontos`

### Campos Principais

| Campo | Descrição | Valores Possíveis |
|-------|-----------|-------------------|
| `categoria` | Tipo de ajuste | `waiver`, `desconto_juridico`, `desconto_comercial` |
| `tipo_desconto` | **Forma de cálculo** | `Fixo` (R$), `Percentual` (%) |
| `forma_aplicacao` | **Como distribuir** | `Provisionado`, `Nao_Provisionado` |
| `origem` | Fonte do desconto | `juridico`, `comercial`, NULL (waivers) |

### Combinações Possíveis

#### **Waivers** (categoria = 'waiver')
```
tipo_desconto = 'Fixo' (sempre valor em R$)
forma_aplicacao = 'Provisionado' OU 'Nao_Provisionado'
origem = NULL
```

#### **Descontos Jurídicos** (categoria = 'desconto_juridico')
```
tipo_desconto = 'Fixo' OU 'Percentual'
forma_aplicacao = 'Provisionado' OU 'Nao_Provisionado'
origem = 'juridico'
```

#### **Descontos Comerciais** (categoria = 'desconto_comercial')
```
tipo_desconto = 'Fixo' OU 'Percentual'
forma_aplicacao = 'Provisionado' OU 'Nao_Provisionado'
origem = 'comercial'
```

---

## 🔢 Exemplos Práticos

### Exemplo 1: Waiver Provisionado
```
Fundo: ABC Investimentos
Valor: R$ 10.000
Tipo: Fixo
Forma: Provisionado
Período: 01/01/2025 a 31/03/2025 (90 dias)

Cálculo:
- Calculadora encontra 90 registros no período
- R$ 10.000 / 90 = R$ 111,11 por registro
- Cada dia tem R$ 111,11 deduzido da taxa calculada
```

### Exemplo 2: Desconto Jurídico Não Provisionado
```
Fundo: XYZ Partners (ID: 42)
Valor: R$ 50.000
Tipo: Fixo
Forma: Nao_Provisionado
Origem: juridico
Período: 01/02/2025 a 28/02/2025 (28 dias)

Cálculo:
- Calculadora identifica último registro do período (28/02/2025)
- R$ 50.000 deduzido APENAS do último registro
- Demais dias: sem ajuste
```

### Exemplo 3: Desconto Comercial Percentual Provisionado
```
Fundo: DEF Capital (ID: 15)
Percentual: 15%
Tipo: Percentual
Forma: Provisionado
Origem: comercial
Período: 01/01/2025 a 30/06/2025 (180 dias)

Cálculo:
- Para cada dia do período:
  1. Calcula taxa normal (ex: R$ 1.000)
  2. Aplica desconto de 15%: R$ 1.000 * 0.85 = R$ 850
  3. Taxa final: R$ 850
```

---

## 🔄 Fluxo na Calculadora

### Passo a Passo

```sql
-- 1. Buscar taxas base (fee_minimo/fee_variavel)
SELECT fee_min, fee_var FROM finance.fee_minimo, finance.fee_variavel
WHERE reference_dt >= data_inicio 
  AND (data_fim IS NULL OR reference_dt <= data_fim);

-- 2. Calcular taxa efetiva
taxa_efetiva = MAX(taxa_variavel_por_faixa, taxa_minima)

-- 3. Buscar ajustes ativos para o período
SELECT * FROM finance.descontos
WHERE (fund_id = ? OR fund_name = ?)
  AND reference_dt >= data_inicio 
  AND (data_fim IS NULL OR reference_dt <= data_fim)
  AND (servico IS NULL OR servico = ?);

-- 4. Aplicar ajustes
FOR EACH ajuste IN ajustes:
  IF ajuste.tipo_desconto == 'Percentual':
    desconto = taxa_efetiva * (ajuste.percentual_desconto / 100)
  ELSE:  -- Fixo
    IF ajuste.forma_aplicacao == 'Provisionado':
      desconto = ajuste.valor_desconto / COUNT(registros_periodo)
    ELSE:  -- Nao_Provisionado
      IF registro_atual == ultimo_registro:
        desconto = ajuste.valor_desconto
      ELSE:
        desconto = 0

-- 5. Taxa final
taxa_final = taxa_efetiva - SUM(descontos)
```

---

## ✅ Resumo Conceitual

| Aspecto | Taxas (fee_minimo/fee_variavel) | Ajustes (descontos) |
|---------|--------------------------------|---------------------|
| **O que faz** | Define estrutura de cobrança | Aplica redução temporária |
| **Quando aplica** | Sempre (vigência permanente) | Apenas durante período |
| **Como altera** | Modifica taxa base | Deduz do valor calculado |
| **Aprovação** | Workflow → INSERT/UPDATE direto | Workflow → INSERT em descontos |
| **Exemplo** | "Taxa de administração = 0.5%" | "Desconto de R$ 5.000 em março" |
| **Uso típico** | Regulamento do fundo | Acordos, ordens judiciais |

**Metáfora:** 
- **Taxas** = Preço da etiqueta (permanente)
- **Ajustes** = Cupom de desconto (temporário)
