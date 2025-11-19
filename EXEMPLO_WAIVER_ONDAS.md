# Exemplo: Waiver em Ondas (Progressivo)

## 🌊 Conceito de Waiver em Ondas

Waivers são frequentemente aplicados em **fases progressivas**, onde o desconto diminui gradualmente ao longo do tempo até normalizar a cobrança.

---

## 📊 Exemplo Prático: Onboarding de Novo Fundo

### Cenário
Um novo fundo está sendo onboarded e receberá waiver progressivo de taxa de administração:

- **Meses 1-2**: Não cobra nada (100% waiver)
- **Meses 3-4**: Cobra metade (50% waiver)
- **Mês 5 em diante**: Cobra full (0% waiver)

---

## 🔧 Configuração no Dashboard

### **Dados Gerais**
```
Fundos: ABC Investimentos
Serviços: Administração
```

### **Onda 1: Período de Carência Total**
```
📅 Período: 01/01/2025 a 28/02/2025 (59 dias)
💰 Tipo: Percentual
📊 Percentual: 100%
🔄 Forma: Provisionado
📝 Efeito: Taxa de adm ZERADA nos primeiros 2 meses
```

### **Onda 2: Redução Gradual**
```
📅 Período: 01/03/2025 a 30/04/2025 (61 dias)
💰 Tipo: Percentual
📊 Percentual: 50%
🔄 Forma: Provisionado
📝 Efeito: Cobra METADE da taxa de adm
```

### **Onda 3: Normalização**
```
📅 Período: 01/05/2025 a 31/12/2025 (245 dias)
💰 Tipo: Percentual
📊 Percentual: 0%
🔄 Forma: Provisionado
📝 Efeito: Cobra taxa COMPLETA (sem waiver)
```

---

## 💡 Resultado Esperado

### Dados na Tabela `finance.descontos`

Serão criados **3 registros** (1 por onda):

| Linha | fund_name | categoria | tipo_desconto | percentual_desconto | forma_aplicacao | data_inicio | data_fim | servico |
|-------|-----------|-----------|---------------|---------------------|-----------------|-------------|----------|---------|
| 1 | ABC Investimentos | waiver | Percentual | 100.0 | Provisionado | 2025-01-01 | 2025-02-28 | Administração |
| 2 | ABC Investimentos | waiver | Percentual | 50.0 | Provisionado | 2025-03-01 | 2025-04-30 | Administração |
| 3 | ABC Investimentos | waiver | Percentual | 0.0 | Provisionado | 2025-05-01 | 2025-12-31 | Administração |

---

## 🧮 Cálculo na Calculadora

### Taxa Base (exemplo)
```
Taxa de Administração = 0.5% a.a. = R$ 1.000/dia
```

### Aplicação do Waiver

#### **Janeiro (Onda 1 - 100% waiver)**
```sql
SELECT * FROM finance.descontos
WHERE fund_name = 'ABC Investimentos'
  AND '2025-01-15' BETWEEN data_inicio AND data_fim
  AND servico = 'Administração';

-- Retorna: percentual_desconto = 100%
-- Cálculo: R$ 1.000 * (1 - 100/100) = R$ 0
-- Taxa cobrada: R$ 0
```

#### **Março (Onda 2 - 50% waiver)**
```sql
SELECT * FROM finance.descontos
WHERE fund_name = 'ABC Investimentos'
  AND '2025-03-15' BETWEEN data_inicio AND data_fim
  AND servico = 'Administração';

-- Retorna: percentual_desconto = 50%
-- Cálculo: R$ 1.000 * (1 - 50/100) = R$ 500
-- Taxa cobrada: R$ 500
```

#### **Maio (Onda 3 - 0% waiver)**
```sql
SELECT * FROM finance.descontos
WHERE fund_name = 'ABC Investimentos'
  AND '2025-05-15' BETWEEN data_inicio AND data_fim
  AND servico = 'Administração';

-- Retorna: percentual_desconto = 0%
-- Cálculo: R$ 1.000 * (1 - 0/100) = R$ 1.000
-- Taxa cobrada: R$ 1.000 (FULL)
```

---

## 📈 Gráfico Conceitual

```
Taxa Cobrada (R$)
│
1000 ┤                           ┌─────────────────
     │                          /
 500 ┤             ┌───────────┘
     │            /
   0 ┤───────────┘
     │
     └────┬────┬────┬────┬────┬────┬────┬────┬───> Mês
         Jan  Fev  Mar  Abr  Mai  Jun  Jul  Ago
         
         └─ 100% ─┘└─ 50% ─┘└────── 0% ──────┘
           waiver    waiver      waiver
```

---

## 🎯 Casos de Uso Comuns

### 1. **Onboarding de Novos Fundos**
```
Onda 1: 3 meses - 100% waiver (carência total)
Onda 2: 3 meses - 50% waiver (transição)
Onda 3: Indefinido - 0% waiver (normal)
```

### 2. **Acordo Comercial Temporário**
```
Onda 1: 6 meses - 75% waiver (desconto grande)
Onda 2: 6 meses - 50% waiver (redução gradual)
Onda 3: 6 meses - 25% waiver (finalização)
Onda 4: Indefinido - 0% waiver (retorno ao normal)
```

### 3. **Waiver Fixo em Etapas**
```
Onda 1: Jan-Mar - R$ 10.000/mês waiver (fixo)
Onda 2: Abr-Jun - R$ 5.000/mês waiver (fixo)
Onda 3: Jul-Dez - R$ 2.000/mês waiver (fixo)
```

---

## ✅ Vantagens do Sistema de Ondas

1. **Flexibilidade**: Configure quantas fases forem necessárias
2. **Precisão**: Cada onda tem seu próprio percentual/valor
3. **Rastreabilidade**: Cada onda é um registro separado com audit trail
4. **Simplicidade**: Calculadora aplica automaticamente baseado na data
5. **Múltiplos Serviços**: Pode aplicar ondas diferentes para serviços diferentes
6. **Agrupamento**: Todas as ondas compartilham mesmo `solicitacao_id` para aprovação em bloco

---

## 🔄 Workflow no Dashboard

1. Selecionar fundos e serviços
2. Clicar em "➕ Adicionar Onda" para cada fase
3. Configurar período, tipo e percentual/valor de cada onda
4. Revisar resumo (ex: "3 fundos × 3 ondas = 9 waivers")
5. Submeter para aprovação
6. Aprovador aprova em bloco (todas as ondas de uma vez)
7. Waivers aplicados automaticamente pela calculadora conforme período
