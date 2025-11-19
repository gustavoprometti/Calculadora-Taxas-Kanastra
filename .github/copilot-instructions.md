# Calculadora de Taxas - Kanastra

Sistema de gestão e cálculo de taxas financeiras (administração, gestão, custódia) com interface Streamlit e backend Google BigQuery.

## Arquitetura

### Componentes Principais

- **`dashboard_gestao_taxas.py`**: Interface administrativa com **3 abas** para gestão completa:
  - **Criação/Alteração de Taxas - Regulamento**: CRUD de taxas (mínimas e variáveis) com sistema de aprovação em dois níveis (editores/aprovadores)
  - **Waivers**: Criação de waivers com aprovação + histórico de waivers aprovados
  - **Descontos**: Gestão de descontos (em desenvolvimento)
- **`dashboard_sql_streamlit.py`**: Dashboard de visualização executando a query complexa de cálculo de taxas com filtros dinâmicos e provisão de waivers
- **`Calculadora 5.0.sql`**: Query SQL principal (~600 linhas) que calcula taxas diárias, acumuladas mensais, correções por índices (IGPM/IPCA/IPC-FIPE) e compara com provisões Sinqia
- **Tabelas BigQuery**:
  - `kanastra-live.finance.fee_minimo`: Taxas mínimas por fundo/serviço/faixa + **data_inicio/data_fim**
  - `kanastra-live.finance.fee_variavel`: Taxas variáveis percentuais por fundo/serviço/faixa de PL + **data_inicio/data_fim**
  - `kanastra-live.finance.alteracoes_pendentes`: Workflow de aprovação (JSON com dados, status PENDENTE/APROVADO/REJEITADO, **solicitacao_id** para agrupar linhas relacionadas)
  - `kanastra-live.finance.historico_waivers`: Registro de waivers aplicados (provisionados/não provisionados)
  - `kanastra-live.hub.funds`: Cadastro de fundos (id, name, government_id/cnpj)

## BigQuery Integration Patterns

### Autenticação Dual
```python
# Sempre use este padrão no início dos arquivos:
@st.cache_resource
def get_bigquery_client():
    try:
        # Cloud: Streamlit Secrets
        if "gcp_service_account" in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(credentials=credentials, project='kanastra-live')
    except:
        pass
    # Local: Application Default Credentials
    return bigquery.Client(project='kanastra-live')
```

### Query Parametrization
- **Datas**: Use f-strings com formato `YYYY-MM-DD` para filtros SQL
- **Fundos específicos**: Fundos 302 e 76 têm lógica especial (PL via `investment.wallet` em vez de `investment.quotas`)
- **Colunas com espaços**: Use backticks para `fund id` → `` `fund id` ``

## Sistema de Taxas

### Lógica de Cálculo (ver `Calculadora 5.0.sql`)

1. **Faixas de PL**: Taxas variáveis aplicam-se por faixas progressivas de patrimônio líquido
2. **Taxa Efetiva = MAX(taxa_variável, taxa_mínima)** calculada diariamente
3. **Correção Anual**: Taxas mínimas corrigidas anualmente por índices (IGPM/IPCA/IPC-FIPE) com fator acumulado a cada 12 meses
4. **Gross Up**: Taxas podem ser "grossed up" (divisão por `1 - gross_rate`) - configurado em `finance.gross_up`
5. **Dias úteis**: Usa tabela `investment.calendar` filtrada por `is_business_day_br = TRUE`

### Estrutura de Faixas

**Taxa Mínima** (2 linhas sempre):
- Faixa 0.0 com valor fixo
- Faixa 1000000000000000.0 (máxima) com mesmo valor

**Taxa Variável** (N faixas):
- Cada linha = limite inferior de PL + percentual
- Ex: Faixa 0 = 0.15%, Faixa 50M = 0.10%, Faixa 100M = 0.05%

## Workflow de Aprovação (`dashboard_gestao_taxas.py`)

### Perfis de Usuário
```python
USUARIOS = {
    "EricIsamo": {"perfil": "aprovador", ...},
    "ThiagoGarcia": {"perfil": "aprovador", ...},
    "GustavoPrometti": {"perfil": "editor", ...}
}
```

### Fluxo de Alterações
1. **Editor** cria/edita taxa → `salvar_alteracao_pendente()` → JSON na tabela `alteracoes_pendentes` com **solicitacao_id** único
2. **Aprovador** revisa solicitações agrupadas → Botão "Aprovar Solicitação Completa" executa INSERT/UPDATE de TODAS as linhas em bloco
3. **Agrupamento**: Múltiplas linhas relacionadas (ex: taxa mínima = 2 linhas, taxa variável = N faixas) compartilham mesmo `solicitacao_id`
4. **Período de Vigência**: Todas as taxas possuem `data_inicio` (obrigatória) e `data_fim` (NULL = indefinido)
5. **Validação crítica**: Sempre verificar se `tabela_selecionada` corresponde aos `dados_editados` carregados

### Formulários Distintos (4 tipos)
- Taxa Mínima + Criar: Gera 2 linhas (faixa 0 e máxima) com **data_inicio/data_fim** + checkbox "vigência indefinida"
- Taxa Mínima + Editar: Atualiza fee_min + **data_inicio/data_fim** de registro existente
- Taxa Variável + Criar: N linhas (usuário define quantas faixas) + **data_inicio/data_fim** aplicadas a todas
- Taxa Variável + Editar: Carrega todas as faixas de um cliente+serviço + **data_inicio/data_fim** aplicadas em lote

## Waiver Management (`dashboard_gestao_taxas.py`)

### Workflow de Criação de Waivers
1. **Editor/Aprovador** acessa aba "💰 Waivers"
2. Seleciona um ou mais fundos
3. Configura valor e tipo (Provisionado/Não Provisionado) para cada fundo
4. Define período de aplicação (data início e fim)
5. Adiciona observação opcional
6. Sistema salva como alteração pendente na tabela `alteracoes_pendentes` com `tabela='waiver'`
7. **Aprovador** revisa no painel de aprovação
8. Ao aprovar, sistema insere registro em `finance.historico_waivers`

### Tipos de Waiver
- **Provisionado**: Distribui valor proporcionalmente por todos os registros do fundo no período (usado no `dashboard_sql_streamlit.py`)
- **Não Provisionado**: Aplica valor total no último registro do fundo (usado no `dashboard_sql_streamlit.py`)

### Visualização de Histórico
- Exibe últimos 100 waivers aprovados da tabela `historico_waivers`
- Filtros por fundo e tipo
- Formatação com colunas configuradas (datas, valores monetários)

## Waiver Application (`dashboard_sql_streamlit.py`)

## Waiver Application (`dashboard_sql_streamlit.py`)

### Aplicação em DataFrame (Visualização)
```python
# Sempre aplicar APÓS filtros e ANTES de exibir dados
if waiver_info:
    for fundo in waiver_info['fundos']:
        valor = waiver_info['valores'].get(fundo, 0)
        tipo = waiver_info['tipos'].get(fundo, "Provisionado")
        
        if tipo == "Provisionado":
            valor_por_registro = valor / qtd_registros
            df_filtrado.loc[mask_fundo, col_acumulado] -= valor_por_registro
        else:
            idx_ultimo = df_filtrado[mask_fundo].index.max()
            df_filtrado.at[idx_ultimo, col_acumulado] -= valor
```

## Convenções de Código

### Caching Streamlit
- `@st.cache_resource`: Clientes BigQuery, conexões
- `@st.cache_data(ttl=300)`: Queries de dados (5 min TTL)
- **Limpar cache**: `carregar_dados_bigquery.clear()` antes de recarregar dados

### Session State
```python
# Inicializar SEMPRE no início:
if 'dados_originais' not in st.session_state:
    st.session_state.dados_originais = None
if 'usuario_logado' not in st.session_state:
    st.session_state.usuario_logado = None
```

### Navegação por Abas (`dashboard_gestao_taxas.py`)
```python
# Sistema de abas na sidebar usando st.radio
aba_selecionada = st.sidebar.radio(
    "Selecione o painel:",
    [
        "📋 Criação/Alteração de Taxas - Regulamento",
        "💰 Waivers",
        "🎯 Descontos"
    ]
)

# Renderizar conteúdo baseado na aba selecionada
if aba_selecionada == "📋 Criação/Alteração de Taxas - Regulamento":
    # Todo código de CRUD de taxas aqui (indentado)
    ...
elif aba_selecionada == "💰 Waivers":
    # Painel de waivers (em desenvolvimento)
    ...
elif aba_selecionada == "🎯 Descontos":
    # Painel de descontos (em desenvolvimento)
    ...
```

### Identidade Visual Kanastra
- **Cores**: Verde principal `#2daa82`, verde escuro `#193c32`, verde médio `#14735a`
- **Fonte**: Inter (Google Fonts)
- **Logo**: `https://www.kanastra.design/symbol-green.svg`

## Desenvolvimento Local

### Setup
```bash
# Autenticação local (ADC)
gcloud auth application-default login --project=kanastra-live

# Instalar dependências
pip install -r requirements.txt

# Executar dashboards
streamlit run dashboard_gestao_taxas.py  # Porta 8501
streamlit run dashboard_sql_streamlit.py  # Porta 8502 (se simultâneo)
```

### Debugging BigQuery
- Sempre capturar `total_bytes_processed` para monitorar custos
- Usar `st.code(sql, language="sql")` para exibir queries antes de executar
- Filtrar dados APÓS carregar (não na query) para aproveitar cache

## Pontos de Atenção

1. **Fundos especiais**: IDs 41, 6, 62, 40, 36, 98, 96, 161, 178, 187, 232, 247, 245, 164, 268, 179, 295, 274, 322, 291 usam offset de sequência diferente (`seq = seq1` vs `seq = seq1 - 1`)
2. **Coluna de serviço**: `Service` no resultado final, mas `servico` nas tabelas de configuração
3. **CNPJ**: Chamado de `government_id` em `hub.funds`
4. **Diferença absoluta**: Sempre aplicar `abs()` em comparações calculadora vs Sinqia
5. **Bloqueio de acesso**: `st.stop()` após tela de login impede acesso não autenticado

## Deployment

Produção: Streamlit Cloud com secrets configurados (`gcp_service_account` JSON)
