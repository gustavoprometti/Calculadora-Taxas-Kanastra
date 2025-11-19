"""
Dashboard Interativo - Calculadora de Taxas Kanastra
Executa a query SQL original direto no BigQuery
Powered by Streamlit
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
import uuid

# Configuração da página
st.set_page_config(
    page_title="Calculadora de Taxas - Kanastra",
    page_icon="https://www.kanastra.design/symbol.svg",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado com fontes e identidade visual Kanastra (Moderno)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Cores Kanastra */
    :root {
        --kanastra-green: #193c32;
        --tech-green-1: #1e5546;
        --tech-green-2: #14735a;
        --tech-green-3: #2daa82;
        --light-gray: #f8f9fa;
        --white: #ffffff;
    }
    
    /* Background geral */
    .main {
        background: #ffffff;
    }
    
    /* Aplicar fonte Inter em todo o dashboard */
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    /* Estilo do header com logo */
    .main-header {
        display: flex;
        align-items: center;
        gap: 20px;
        padding: 20px 0;
        border-bottom: 2px solid #2daa82;
        margin-bottom: 30px;
    }
    
    .kanastra-logo {
        height: 50px;
    }
    
    /* Títulos com fonte Inter */
    h1, h2, h3 {
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        color: #193c32;
    }
    
    /* Botões modernos com gradiente */
    .stButton>button {
        background: linear-gradient(135deg, #14735a 0%, #2daa82 100%) !important;
        color: white !important;
        font-weight: 600 !important;
        border-radius: 12px !important;
        padding: 0.75rem 2rem !important;
        border: none !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 4px 12px rgba(20, 115, 90, 0.25) !important;
    }
    
    .stButton>button:hover {
        background: linear-gradient(135deg, #2daa82 0%, #14735a 100%) !important;
        transform: translateY(-3px);
        box-shadow: 0 6px 20px rgba(20, 115, 90, 0.4) !important;
    }
    
    .stButton>button:active {
        transform: translateY(-1px);
    }
    
    /* Metrics com destaque */
    [data-testid="stMetricValue"] {
        color: #14735a;
        font-weight: 700;
    }
    
    /* Sidebar com gradiente */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #ffffff 0%, #f8f9fa 100%);
        border-right: 1px solid #e9ecef;
    }
    
    /* Divisores */
    hr {
        border: none;
        border-top: 2px solid #e9ecef;
        margin: 2rem 0;
    }
    
    /* DataFrames com bordas arredondadas */
    .dataframe {
        border-radius: 12px !important;
    }
    
    /* Alertas modernos */
    .stAlert {
        border-radius: 12px !important;
        border: none !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08) !important;
    }
    
    /* Inputs modernos */
    .stSelectbox label, .stMultiSelect label, .stDateInput label {
        color: #193c32 !important;
        font-weight: 600 !important;
        font-size: 0.95rem !important;
    }
    
    /* Select boxes com bordas arredondadas */
    .stSelectbox > div > div, .stMultiSelect > div > div {
        border-radius: 10px !important;
        border: 2px solid #e9ecef !important;
        transition: all 0.3s ease !important;
    }
    
    .stSelectbox > div > div:focus-within, .stMultiSelect > div > div:focus-within {
        border-color: #2daa82 !important;
        box-shadow: 0 0 0 3px rgba(45, 170, 130, 0.1) !important;
    }
    
    /* Date inputs */
    .stDateInput > div > div > input {
        border-radius: 10px !important;
        border: 2px solid #e9ecef !important;
    }
    
    /* Progress bar */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #14735a 0%, #2daa82 100%);
    }
    
    /* Multi-select tags */
    .stMultiSelect [data-baseweb="tag"] {
        background: linear-gradient(135deg, #14735a 0%, #2daa82 100%);
        border-radius: 8px;
    }
    
    /* Expanders */
    .streamlit-expanderHeader {
        background: white;
        border-radius: 10px;
        border: 1px solid #e9ecef;
        transition: all 0.3s ease;
    }
    
    .streamlit-expanderHeader:hover {
        border-color: #2daa82;
        box-shadow: 0 2px 8px rgba(45, 170, 130, 0.1);
    }
    
    /* Scrollbar personalizada */
    ::-webkit-scrollbar {
        width: 10px;
        height: 10px;
    }
    
    ::-webkit-scrollbar-track {
        background: #f1f1f1;
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: linear-gradient(135deg, #14735a 0%, #2daa82 100%);
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: #193c32;
    }
</style>

<div class="main-header">
    <img src="https://www.kanastra.design/symbol-green.svg" class="kanastra-logo" alt="Kanastra">
    <div>
        <h1 style="margin: 0;">Dashboard - Calculadora de Taxas 5.0</h1>
        <p style="margin: 0; color: #14735a; font-weight: 500;">Kanastra Live | Calculadora executada direto no BigQuery</p>
    </div>
</div>
""", unsafe_allow_html=True)

# Função para criar cliente BigQuery
@st.cache_resource
def get_bigquery_client():
    """
    Cria cliente BigQuery usando Streamlit Secrets (Cloud) ou ADC (Local)
    """
    try:
        # Tentar usar Streamlit Secrets (quando deployed)
        if "gcp_service_account" in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(credentials=credentials, project='kanastra-live')
    except Exception as e:
        # Se falhar com secrets, tentar ADC
        pass
    
    # Fallback: usar Application Default Credentials (local)
    try:
        return bigquery.Client(project='kanastra-live')
    except Exception as e:
        raise Exception(f"Erro ao criar cliente BigQuery: {str(e)}")

# Sidebar - Logo e Filtros
st.sidebar.image("https://www.kanastra.design/wordmark-green.svg", width=150)
st.sidebar.markdown("---")
st.sidebar.header("🔍 Filtros")

# Filtro de Data
st.sidebar.subheader("📅 Período")

# Opções de período rápido
periodo_rapido = st.sidebar.selectbox(
    "Período rápido:",
    ["Personalizado", "Hoje", "Ontem", "Últimos 7 dias", "Últimos 30 dias", "Últimos 90 dias", 
     "Este mês", "Mês passado", "Este ano"],
    index=5  # Padrão: Últimos 90 dias
)

# Calcular datas baseado na seleção
hoje = datetime.now().date()

if periodo_rapido == "Hoje":
    data_inicio = hoje
    data_fim = hoje
elif periodo_rapido == "Ontem":
    data_inicio = hoje - timedelta(days=1)
    data_fim = hoje - timedelta(days=1)
elif periodo_rapido == "Últimos 7 dias":
    data_inicio = hoje - timedelta(days=7)
    data_fim = hoje
elif periodo_rapido == "Últimos 30 dias":
    data_inicio = hoje - timedelta(days=30)
    data_fim = hoje
elif periodo_rapido == "Últimos 90 dias":
    data_inicio = hoje - timedelta(days=90)
    data_fim = hoje
elif periodo_rapido == "Este mês":
    data_inicio = hoje.replace(day=1)
    data_fim = hoje
elif periodo_rapido == "Mês passado":
    primeiro_dia_mes_atual = hoje.replace(day=1)
    ultimo_dia_mes_passado = primeiro_dia_mes_atual - timedelta(days=1)
    data_inicio = ultimo_dia_mes_passado.replace(day=1)
    data_fim = ultimo_dia_mes_passado
elif periodo_rapido == "Este ano":
    data_inicio = hoje.replace(month=1, day=1)
    data_fim = hoje
else:  # Personalizado
    col1, col2 = st.sidebar.columns(2)
    with col1:
        data_inicio = st.date_input(
            "Data Inicial:",
            value=hoje - timedelta(days=90),
            max_value=hoje,
            key="data_inicio_custom"
        )
    with col2:
        data_fim = st.date_input(
            "Data Final:",
            value=hoje,
            max_value=hoje,
            key="data_fim_custom"
        )

dias_periodo = (data_fim - data_inicio).days
st.sidebar.info(f"**{dias_periodo + 1} dias** de {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}")

st.sidebar.divider()

# Carregar lista de fundos e serviços do BigQuery para os filtros
@st.cache_data(ttl=3600)
def carregar_opcoes_filtros():
    """Carrega lista de fundos e serviços disponíveis"""
    try:
        client = get_bigquery_client()
        
        # Buscar fundos - ajustando nome da coluna
        query_fundos = """
        SELECT DISTINCT name as fund_name
        FROM `kanastra-live.hub.funds` 
        WHERE name IS NOT NULL 
        ORDER BY name
        """
        fundos_df = client.query(query_fundos).to_dataframe()
        fundos = ["Todos"] + fundos_df['fund_name'].tolist()
        
        # Serviços fixos (podem estar na query)
        servicos = ["Todos", "Administração", "Gestão", "Custódia", "Custódia Kanastra"]
        
        return fundos, servicos
    except Exception as e:
        st.sidebar.warning(f"⚠️ Não foi possível carregar filtros: {str(e)[:100]}")
        return ["Todos"], ["Todos"]

# Carregar opções
fundos_disponiveis, servicos_disponiveis = carregar_opcoes_filtros()

# Filtro de Fundo (múltiplos)
st.sidebar.subheader("🏢 Fundos")
fundos_selecionados = st.sidebar.multiselect(
    "Selecione um ou mais fundos:",
    fundos_disponiveis[1:],  # Remove "Todos"
    default=[],
    key="filtro_fundos",
    help="Deixe vazio para mostrar todos os fundos"
)

# Filtro de Serviço
st.sidebar.subheader("⚙️ Serviço")
servico_selecionado = st.sidebar.selectbox(
    "Selecione o serviço:",
    servicos_disponiveis,
    key="filtro_servico"
)

st.sidebar.divider()

# Inicializar session_state para executar automaticamente na primeira vez
if 'auto_executed' not in st.session_state:
    st.session_state['auto_executed'] = False
    st.session_state['execute_query'] = True

# Função para carregar a query SQL
@st.cache_data
def load_sql_query():
    """Carrega a query SQL do arquivo"""
    sql_file = "Calculadora 5.0.sql"
    
    if not os.path.exists(sql_file):
        st.error(f"❌ Arquivo SQL não encontrado: {sql_file}")
        return None
    
    with open(sql_file, 'r', encoding='utf-8') as f:
        query = f.read()
    
    return query

# Função para executar query no BigQuery
@st.cache_data(ttl=600)  # Cache por 10 minutos
def executar_query_bigquery(_client, query, data_inicio_str, data_fim_str):
    """Executa a query SQL no BigQuery"""
    
    try:
        # Substituir parâmetros de data na query (se necessário)
        # Ajuste conforme sua query usar as datas
        query_parametrizada = query.replace('CURRENT_DATE()', f"'{data_fim_str}'")
        
        # Se sua query tiver parâmetros específicos, adicione aqui
        # Exemplo: query = query.replace('@data_inicio', f"'{data_inicio_str}'")
        
        # Executar query
        query_job = _client.query(query_parametrizada)
        
        # Converter para DataFrame
        df = query_job.to_dataframe()
        
        # Informações sobre a query
        bytes_processed = query_job.total_bytes_processed / 1024 / 1024  # MB
        
        return df, bytes_processed, None
        
    except Exception as e:
        return None, 0, str(e)

# Botão para executar
if st.sidebar.button("🚀 Executar Query SQL", type="primary", width='stretch'):
    st.session_state['execute_query'] = True

# Executar query automaticamente ou quando solicitado
if st.session_state.get('execute_query', False) or not st.session_state['auto_executed']:
    
    # Marcar como executado
    st.session_state['auto_executed'] = True
    st.session_state['execute_query'] = False
    
    # Carregar query SQL
    with st.spinner('📄 Carregando query SQL...'):
        sql_query = load_sql_query()
    
    if sql_query:
        st.sidebar.success("✅ Query SQL carregada!")
        
        # Conectar ao BigQuery
        with st.spinner('🔌 Conectando ao BigQuery...'):
            try:
                client = get_bigquery_client()
                st.sidebar.success("✅ Conectado ao BigQuery!")
            except Exception as e:
                st.sidebar.error(f"❌ Erro ao conectar: {e}")
                st.stop()
        
        # Executar query
        inicio_execucao = datetime.now()
        with st.spinner('⚡ Executando query SQL no BigQuery...'):
            data_inicio_str = data_inicio.strftime('%Y-%m-%d')
            data_fim_str = data_fim.strftime('%Y-%m-%d')
            
            df, bytes_mb, error = executar_query_bigquery(
                client, 
                sql_query, 
                data_inicio_str, 
                data_fim_str
            )
        fim_execucao = datetime.now()
        tempo_execucao = (fim_execucao - inicio_execucao).total_seconds()
        
        if error:
            st.error(f"❌ Erro ao executar query: {error}")
            with st.expander("🔍 Ver detalhes do erro"):
                st.code(error)
            st.stop()
        
        if df is not None and not df.empty:
            # Salvar DataFrame ORIGINAL (sem filtros) e timestamp
            st.session_state['df_original'] = df.copy()
            st.session_state['df'] = df.copy()  # Manter compatibilidade
            st.session_state['bytes_mb'] = bytes_mb
            st.session_state['ultima_atualizacao'] = fim_execucao
            st.session_state['tempo_execucao'] = tempo_execucao
            
            st.sidebar.success(f"✅ Query executada com sucesso!")
            st.sidebar.info(f"📊 {len(df):,} registros carregados")
            st.sidebar.info(f"� {bytes_mb:.2f} MB processados")
            st.sidebar.info(f"⏱️ Tempo: {tempo_execucao:.2f}s")
            st.sidebar.success(f"🕐 Atualizado: {fim_execucao.strftime('%d/%m/%Y %H:%M:%S')}")
        else:
            st.warning("⚠️ Query retornou 0 registros")

# Mostrar resultados se existirem
if 'df' in st.session_state:
    df_original = st.session_state.get('df_original', st.session_state['df'])
    bytes_mb = st.session_state.get('bytes_mb', 0)
    
    # Aplicar filtros em tempo real
    df_filtrado = df_original.copy()
    
    # Filtro de Fundos (múltiplos)
    if fundos_selecionados and 'fund_name' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['fund_name'].isin(fundos_selecionados)]
    
    # Filtro de Serviço
    if servico_selecionado != "Todos":
        col_service = 'Service' if 'Service' in df_filtrado.columns else 'servico' if 'servico' in df_filtrado.columns else None
        if col_service:
            df_filtrado = df_filtrado[df_filtrado[col_service] == servico_selecionado]
    
    # Filtro de Data (aplicar no DataFrame já carregado)
    if 'date_ref' in df_filtrado.columns:
        # Converter se necessário
        if not pd.api.types.is_datetime64_any_dtype(df_filtrado['date_ref']):
            df_filtrado['date_ref'] = pd.to_datetime(df_filtrado['date_ref'])
        
        df_filtrado = df_filtrado[
            (df_filtrado['date_ref'].dt.date >= data_inicio) & 
            (df_filtrado['date_ref'].dt.date <= data_fim)
        ]
    
    # SISTEMA DE INVALIDAÇÃO DE CACHE INTELIGENTE
    @st.cache_data(ttl=60)  # Cache de 1 minuto para timestamp (precisa ser rápido para detectar aprovações)
    def obter_timestamp_ultima_modificacao():
        """Obtém timestamp da última modificação em alterações pendentes e ajustes"""
        try:
            client = get_bigquery_client()
            query = """
            SELECT MAX(timestamp_mod) as ultima_modificacao
            FROM (
                SELECT MAX(data_aplicacao) as timestamp_mod 
                FROM `kanastra-live.finance.descontos`
                UNION ALL
                SELECT MAX(data_solicitacao) as timestamp_mod 
                FROM `kanastra-live.finance.alteracoes_pendentes`
                UNION ALL
                SELECT MAX(data_aprovacao) as timestamp_mod
                FROM `kanastra-live.finance.historico_alteracoes`
            )
            """
            result = client.query(query).to_dataframe()
            if not result.empty and pd.notnull(result.iloc[0]['ultima_modificacao']):
                return result.iloc[0]['ultima_modificacao']
            return datetime.now()
        except Exception as e:
            return datetime.now()
    
    # VERIFICAR ALTERAÇÕES PENDENTES DE APROVAÇÃO
    # SEM CACHE - sempre consulta valores atuais (query rápida, crítica para bloqueio de exportação)
    def verificar_alteracoes_pendentes():
        """Verifica se existem alterações pendentes de aprovação
        
        Esta função NÃO usa cache para garantir que bloqueios de exportação sejam sempre precisos.
        """
        try:
            client = get_bigquery_client()
            query = """
            SELECT 
                COUNT(*) as total_pendente,
                COUNT(DISTINCT solicitacao_id) as solicitacoes_pendentes
            FROM `kanastra-live.finance.alteracoes_pendentes`
            WHERE status = 'PENDENTE'
            """
            result = client.query(query).to_dataframe()
            
            if not result.empty:
                total = int(result.iloc[0]['total_pendente'])
                solicitacoes = int(result.iloc[0]['solicitacoes_pendentes'])
                return total, solicitacoes
            
            return 0, 0
        except Exception as e:
            return 0, 0
    
    # APLICAR WAIVERS E DESCONTOS APROVADOS do BigQuery
    @st.cache_data(ttl=3600)  # Cache de 1 hora (invalidado por timestamp)
    def carregar_ajustes_ativos(data_inicio_dt, data_fim_dt, cache_key):
        """Carrega waivers e descontos aprovados que se aplicam ao período
        
        Args:
            cache_key: Timestamp usado para invalidar cache quando há modificações
        """
        try:
            client = get_bigquery_client()
            query = f"""
            SELECT 
                fund_id,
                fund_name,
                categoria,
                tipo_desconto,
                valor_desconto,
                percentual_desconto,
                forma_aplicacao,
                servico,
                data_inicio,
                data_fim,
                observacao,
                data_aplicacao
            FROM `kanastra-live.finance.descontos`
            WHERE data_inicio <= DATE('{data_fim_dt}')
              AND (data_fim IS NULL OR data_fim >= DATE('{data_inicio_dt}'))
            ORDER BY categoria, data_inicio
            """
            df = client.query(query).to_dataframe()
            # Armazenar timestamp da carga
            st.session_state.ultima_carga_ajustes = datetime.now()
            return df
        except Exception as e:
            st.warning(f"⚠️ Não foi possível carregar ajustes (waivers/descontos): {e}")
            return pd.DataFrame()
    
    # Obter timestamp de última modificação (atualiza a cada 1 min, mas força recarga se houver mudanças)
    timestamp_modificacao = obter_timestamp_ultima_modificacao()
    
    # Verificar se deve forçar recarga de ajustes (mantém funcionalidade de botão manual)
    force_reload_ajustes = st.session_state.get('force_reload_ajustes', False)
    if force_reload_ajustes:
        carregar_ajustes_ativos.clear()
        obter_timestamp_ultima_modificacao.clear()
        st.session_state.force_reload_ajustes = False
    
    # Carregar dados usando timestamp como cache key (invalida automaticamente quando há alterações)
    ajustes_ativos = carregar_ajustes_ativos(data_inicio, data_fim, timestamp_modificacao)
    
    # Verificar alterações pendentes - SEM CACHE para garantir precisão no bloqueio
    total_pendente, solicitacoes_pendentes = verificar_alteracoes_pendentes()
    
    # AVISOS DE ALTERAÇÕES PENDENTES E AJUSTES ATIVOS
    st.divider()
    
    # Mostrar timestamp da última verificação
    if isinstance(timestamp_modificacao, pd.Timestamp):
        ultima_verificacao = timestamp_modificacao.to_pydatetime()
    else:
        ultima_verificacao = timestamp_modificacao
    
    tempo_decorrido = (datetime.now() - ultima_verificacao).total_seconds()
    if tempo_decorrido < 60:
        tempo_texto = f"{int(tempo_decorrido)}s atrás"
    elif tempo_decorrido < 3600:
        tempo_texto = f"{int(tempo_decorrido // 60)}min atrás"
    else:
        tempo_texto = f"{int(tempo_decorrido // 3600)}h atrás"
    
    st.caption(f"🔄 Última verificação de alterações: {tempo_texto}")
    
    col_aviso1, col_aviso2 = st.columns(2)
    
    with col_aviso1:
        if total_pendente > 0:
            st.error(
                f"⚠️ **ATENÇÃO: {solicitacoes_pendentes} solicitação(ões) pendente(s) de aprovação** "
                f"({total_pendente} alteração(ões) no total)\n\n"
                f"❌ **Exportação bloqueada até aprovação**"
            )
        else:
            st.success("✅ **Nenhuma alteração pendente de aprovação**")
    
    with col_aviso2:
        if not ajustes_ativos.empty:
            ultima_carga = st.session_state.get('ultima_carga_ajustes', None)
            tempo_info = ""
            if ultima_carga:
                tempo_desde_carga = (datetime.now() - ultima_carga).total_seconds()
                if tempo_desde_carga < 60:
                    tempo_info = f" (carregados há {int(tempo_desde_carga)}s)"
                else:
                    minutos = int(tempo_desde_carga // 60)
                    tempo_info = f" (carregados há {minutos}min)"
            
            st.info(f"📊 **{len(ajustes_ativos)} ajustes ativos** aplicados{tempo_info}")
        else:
            st.info("ℹ️ **Nenhum ajuste (waiver/desconto) ativo no período**")
    
    # PAINEL DE DETALHAMENTO DOS AJUSTES ATIVOS
    if not ajustes_ativos.empty:
        with st.expander("📋 **Detalhes dos Ajustes Aplicados**", expanded=False):
            # Agrupar por categoria
            for categoria in ajustes_ativos['categoria'].unique():
                df_cat = ajustes_ativos[ajustes_ativos['categoria'] == categoria]
                
                # Traduzir categoria
                categoria_nome = {
                    'waiver': '💰 Waivers',
                    'desconto_juridico': '⚖️ Descontos Jurídicos',
                    'desconto_comercial': '🤝 Descontos Comerciais'
                }.get(categoria, categoria)
                
                st.markdown(f"### {categoria_nome} ({len(df_cat)})", unsafe_allow_html=True)
                
                for idx, ajuste in df_cat.iterrows():
                    # Identificar fundo
                    fundo = ajuste.get('fund_name') or f"ID {ajuste.get('fund_id')}"
                    
                    # Montar descrição do ajuste
                    if ajuste['tipo_desconto'] == 'Percentual':
                        descricao_valor = f"{ajuste['percentual_desconto']:.2f}% de desconto"
                    else:
                        descricao_valor = f"R$ {ajuste['valor_desconto']:,.2f}"
                    
                    # Período
                    periodo = f"{ajuste['data_inicio'].strftime('%d/%m/%Y')} até "
                    if pd.notnull(ajuste.get('data_fim')):
                        periodo += ajuste['data_fim'].strftime('%d/%m/%Y')
                    else:
                        periodo += "vigência indefinida"
                    
                    # Serviço específico ou todos
                    servico_info = f" - Serviço: {ajuste['servico']}" if ajuste.get('servico') else " - Todos os serviços"
                    
                    # Forma de aplicação
                    forma = ajuste['forma_aplicacao']
                    
                    st.markdown(
                        f"**{fundo}** | {descricao_valor} ({forma}){servico_info}  \n"
                        f"📅 {periodo}  \n"
                        f"_{ajuste.get('observacao', 'Sem observação')}_",
                        unsafe_allow_html=True
                    )
                    st.markdown("---")
    
    if not ajustes_ativos.empty:
        # Encontrar coluna de acumulado
        col_acumulado = None
        for col in df_filtrado.columns:
            if 'acumulado' in col.lower():
                col_acumulado = col
                break
        
        if col_acumulado:
            ajustes_aplicados = []
            for _, ajuste in ajustes_ativos.iterrows():
                # Compatibilidade: waivers usam fund_name, descontos usam fund_id
                if 'fund_name' in ajuste and pd.notnull(ajuste.get('fund_name')):
                    fund_identifier = ajuste['fund_name']
                    mask_campo = 'fund_name'
                else:
                    fund_identifier = ajuste.get('fund_id')
                    mask_campo = 'fund_id'
                
                valor = ajuste.get('valor', 0)
                tipo_desconto = ajuste.get('tipo_desconto', 'Fixo')
                percentual = ajuste.get('percentual_desconto', 0)
                forma_aplicacao = ajuste.get('forma_aplicacao', 'Provisionado')
                categoria = ajuste.get('categoria', 'waiver')
                servico = ajuste.get('servico')
                
                if valor > 0 or percentual > 0:
                    # Filtrar registros do fundo
                    mask_fundo = (df_filtrado[mask_campo] == fund_identifier)
                    
                    # Filtrar por período se disponível
                    if 'date_ref' in df_filtrado.columns:
                        mask_fundo = mask_fundo & (
                            (df_filtrado['date_ref'].dt.date >= ajuste['data_inicio']) &
                            (df_filtrado['date_ref'].dt.date <= ajuste['data_fim'])
                        )
                    
                    # Filtrar por serviço se especificado
                    if servico and ('Service' in df_filtrado.columns or 'servico' in df_filtrado.columns):
                        col_servico = 'Service' if 'Service' in df_filtrado.columns else 'servico'
                        mask_fundo = mask_fundo & (df_filtrado[col_servico] == servico)
                    
                    if mask_fundo.sum() > 0:
                        # Calcular valor do ajuste baseado no tipo
                        if tipo_desconto == 'Percentual':
                            # Para percentual, calcular desconto sobre cada taxa
                            valores_ajuste = df_filtrado.loc[mask_fundo, col_acumulado] * (percentual / 100)
                            valor_total_aplicado = valores_ajuste.sum()
                        else:
                            # Para Fixo, usar valor direto
                            valor_total_aplicado = valor
                        
                        # Aplicar baseado na forma
                        if forma_aplicacao == "Provisionado":
                            # Provisionado: distribuir proporcionalmente
                            qtd_registros = mask_fundo.sum()
                            
                            if tipo_desconto == 'Percentual':
                                # Percentual: aplicar desconto em cada registro individualmente
                                df_filtrado.loc[mask_fundo, col_acumulado] = (
                                    df_filtrado.loc[mask_fundo, col_acumulado] * (1 - percentual/100)
                                )
                            else:
                                # Fixo: distribuir valor igualmente
                                valor_por_registro = valor / qtd_registros if qtd_registros > 0 else 0
                                df_filtrado.loc[mask_fundo, col_acumulado] = (
                                    df_filtrado.loc[mask_fundo, col_acumulado] - valor_por_registro
                                )
                            
                            ajustes_aplicados.append(
                                f"{fund_identifier} ({categoria}): R$ {valor_total_aplicado:,.2f} {tipo_desconto} Provisionado"
                            )
                        else:
                            # Não Provisionado: aplicar no último registro
                            idx_ultimo = df_filtrado[mask_fundo].index.max()
                            if pd.notnull(idx_ultimo):
                                if tipo_desconto == 'Percentual':
                                    # Percentual: aplicar desconto no último registro
                                    df_filtrado.at[idx_ultimo, col_acumulado] = (
                                        df_filtrado.at[idx_ultimo, col_acumulado] * (1 - percentual/100)
                                    )
                                else:
                                    # Fixo: subtrair valor total do último registro
                                    df_filtrado.at[idx_ultimo, col_acumulado] = (
                                        df_filtrado.at[idx_ultimo, col_acumulado] - valor
                                    )
                                
                                ajustes_aplicados.append(
                                    f"{fund_identifier} ({categoria}): R$ {valor_total_aplicado:,.2f} {tipo_desconto} Não Provisionado"
                                )
            
            if ajustes_aplicados:
                st.success(f"✅ **Ajustes Aplicados ({len(ajustes_aplicados)}):** {' | '.join(ajustes_aplicados)}")

    
    # Usar o DataFrame filtrado
    df = df_filtrado
    
    # Mostrar info de última atualização
    ultima_atualizacao = st.session_state.get('ultima_atualizacao', None)
    tempo_execucao = st.session_state.get('tempo_execucao', 0)
    
    if ultima_atualizacao:
        tempo_decorrido = (datetime.now() - ultima_atualizacao).total_seconds()
        minutos = int(tempo_decorrido // 60)
        segundos = int(tempo_decorrido % 60)
        
        if minutos > 0:
            tempo_txt = f"{minutos}min {segundos}s atrás"
        else:
            tempo_txt = f"{segundos}s atrás"
        
        st.info(f"📅 **Última atualização:** {ultima_atualizacao.strftime('%d/%m/%Y às %H:%M:%S')} ({tempo_txt}) | ⏱️ Tempo de execução: {tempo_execucao:.2f}s")
    
    # Estatísticas no topo
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        registros_filtrados = len(df)
        registros_total = len(df_original)
        diferenca = registros_filtrados - registros_total
        st.metric("📝 Registros Filtrados", f"{registros_filtrados:,}", delta=f"{diferenca:,}", delta_color="off")
    
    with col2:
        if 'fund_id' in df.columns:
            st.metric("🏢 Fundos Únicos", df['fund_id'].nunique())
        else:
            st.metric("🏢 Fundos Únicos", "N/A")
    
    with col3:
        if 'Service' in df.columns or 'servico' in df.columns:
            col_service = 'Service' if 'Service' in df.columns else 'servico'
            st.metric("⚙️ Serviços", df[col_service].nunique())
        else:
            st.metric("⚙️ Serviços", "N/A")
    
    with col4:
        st.metric("💾 Dados Processados", f"{bytes_mb:.2f} MB")
    
    st.divider()
    
    # Busca adicional na tabela
    with st.expander("� Busca Adicional na Tabela", expanded=False):
        busca = st.text_input("Buscar texto em qualquer campo:")
        if busca:
            mask = df.astype(str).apply(lambda x: x.str.contains(busca, case=False, na=False)).any(axis=1)
            df = df[mask]
            st.info(f"🔍 Encontrados **{len(df)}** registros com '{busca}'")
    
    # Preparar DataFrame para exibição (antes dos botões para permitir exportação)
    colunas_desejadas = {
        'date_ref': 'Dia',
        'fund_name': 'Nome do fundo',
        'cnpj': 'CNPJ',
        'fund_id': 'ID do fundo',
        'fund_type': 'Código Sinqia',
        'Service': 'Serviço',
        'pl_total_diario': 'PL',
        'acumulado': 'Provisão Calculadora',
        'provisao_carteira': 'Provisão Sinqia',
        'diferenca': 'Diferença'
    }
    
    # Verificar quais colunas existem no DataFrame
    colunas_existentes = [col for col in colunas_desejadas.keys() if col in df.columns]
    
    # Selecionar apenas as colunas existentes
    df_exibir = df[colunas_existentes].copy()
    
    # Aplicar módulo (valor absoluto) na coluna diferenca se existir
    if 'diferenca' in df_exibir.columns:
        df_exibir['diferenca'] = df_exibir['diferenca'].abs()
    
    # Renomear as colunas
    df_exibir.rename(columns={col: colunas_desejadas[col] for col in colunas_existentes}, inplace=True)
    
    # Botões de ação e exportação
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    
    # VERIFICAÇÃO DUPLA: Re-verificar pendentes imediatamente antes de exportar
    total_pendente_atual, solicitacoes_pendentes_atual = verificar_alteracoes_pendentes()
    
    with col1:
        # Verificar se há ajustes aplicados
        if not ajustes_ativos.empty:
            st.success(f"✅ **{len(df):,}** registros (com ajustes aplicados)")
        else:
            st.info(f"📊 Exibindo **{len(df):,}** registros")
    
    with col2:
        if st.button("🔄 Cache Geral", width='stretch'):
            st.cache_data.clear()
            st.success("✅ Cache limpo!")
            st.rerun()
    
    with col3:
        if st.button("🔃 Recarregar Ajustes", width='stretch', help="Força recarga de waivers e descontos"):
            st.session_state.force_reload_ajustes = True
            st.rerun()
    
    with col4:
        # VALIDAÇÃO DE SEGURANÇA: Bloquear exportação se há alterações pendentes
        if total_pendente_atual > 0:
            st.button(
                label="📥 Exportar CSV",
                width='stretch',
                type="primary",
                disabled=True,
                help=f"⚠️ Exportação bloqueada: {solicitacoes_pendentes_atual} solicitação(ões) pendente(s) de aprovação"
            )
        else:
            # Gerar CSV apenas se não houver pendências (camada extra de segurança)
            download_filtrado = df_exibir.to_csv(index=False).encode('utf-8')
            
            # Nome do arquivo indica se tem ajustes
            sufixo = '_com_ajustes' if not ajustes_ativos.empty else ''
            
            st.download_button(
                label="📥 Exportar CSV",
                data=download_filtrado,
                file_name=f'calculadora_taxas{sufixo}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
                mime='text/csv',
                type="primary",
                width='stretch',
                help="✅ Exportar dados com ajustes aplicados"
            )
    
    # Exibir tabela
    st.subheader("📋 Resultados da Query SQL")
    
    # Configurar formatação das colunas
    column_config = {
        'Dia': st.column_config.DateColumn('Dia', format="DD/MM/YYYY"),
        'Nome do fundo': st.column_config.TextColumn('Nome do fundo', width="large"),
        'CNPJ': st.column_config.TextColumn('CNPJ', width="medium"),
        'ID do fundo': st.column_config.NumberColumn('ID do fundo', format="%d"),
        'Código Sinqia': st.column_config.TextColumn('Código Sinqia', width="small"),
        'Serviço': st.column_config.TextColumn('Serviço', width="medium"),
        'PL': st.column_config.NumberColumn('PL', format="R$ %.2f"),
        'Provisão Calculadora': st.column_config.NumberColumn('Provisão Calculadora', format="R$ %.2f"),
        'Provisão Sinqia': st.column_config.NumberColumn('Provisão Sinqia', format="R$ %.2f"),
        'Diferença': st.column_config.NumberColumn('Diferença', format="R$ %.2f")
    }
    
    # Mostrar tabela
    st.dataframe(
        df_exibir,
        width='stretch',
        height=600,
        hide_index=True,
        column_config=column_config
    )
    
    # GRÁFICOS INTERATIVOS
    st.divider()
    st.subheader("📊 Análise de Diferenças")
    
    # Preparar dados para os gráficos (usar DataFrame original com nomes originais)
    if 'diferenca' in df.columns and 'fund_name' in df.columns and 'Service' in df.columns:
        
        # Filtrar dados onde provisão Sinqia não está vazia/nula
        df_graficos = df.copy()
        if 'provisao_carteira' in df_graficos.columns:
            df_graficos = df_graficos[df_graficos['provisao_carteira'].notna() & (df_graficos['provisao_carteira'] != 0)]
        
        col_grafico1, col_grafico2 = st.columns(2)
        
        with col_grafico1:
            st.markdown("### Diferença máxima por fundo")
            
            # Agrupar por fundo e pegar a diferença máxima (em módulo)
            df_fundo = df_graficos.groupby('fund_name')['diferenca'].apply(lambda x: x.abs().max()).reset_index()
            df_fundo.columns = ['Nome do fundo', 'Valor Diferença (R$)']
            df_fundo = df_fundo.sort_values('Valor Diferença (R$)', ascending=False).head(5)
            
            # Criar gráfico de barras com Plotly
            import plotly.express as px
            
            # Cores da identidade visual Kanastra
            kanastra_colors = ['#193c32', '#1e5546', '#14735a', '#2daa82']
            
            fig1 = px.bar(
                df_fundo,
                x='Nome do fundo',
                y='Valor Diferença (R$)',
                text='Valor Diferença (R$)',
                color='Valor Diferença (R$)',
                color_continuous_scale=[[0, '#2daa82'], [0.5, '#14735a'], [1, '#193c32']]
            )
            
            fig1.update_traces(
                texttemplate='%{text:,.2f}',
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>Diferença Máxima: R$ %{y:,.2f}<extra></extra>'
            )
            
            fig1.update_layout(
                xaxis_title="Nome do fundo",
                yaxis_title="Valor Diferença (R$)",
                showlegend=False,
                height=600,
                xaxis_tickangle=-45
            )
            
            st.plotly_chart(fig1, width='stretch')
        
        with col_grafico2:
            st.markdown("### Diferença máxima por tipo de serviço")
            
            # Agrupar por serviço e somar diferenças (em módulo)
            df_servico = df_graficos.groupby('Service')['diferenca'].apply(lambda x: x.abs().sum()).reset_index()
            df_servico.columns = ['Serviço', 'Valor Diferença (R$)']
            df_servico = df_servico.sort_values('Valor Diferença (R$)', ascending=False)
            
            fig2 = px.bar(
                df_servico,
                x='Serviço',
                y='Valor Diferença (R$)',
                text='Valor Diferença (R$)',
                color='Valor Diferença (R$)',
                color_continuous_scale=[[0, '#2daa82'], [0.5, '#14735a'], [1, '#193c32']]
            )
            
            fig2.update_traces(
                texttemplate='%{text:,.2f}',
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>Diferença: R$ %{y:,.2f}<extra></extra>'
            )
            
            fig2.update_layout(
                xaxis_title="Serviço",
                yaxis_title="Valor Diferença (R$)",
                showlegend=False,
                height=600
            )
            
            st.plotly_chart(fig2, width='stretch')
    
    # Informações adicionais
    with st.expander("ℹ️ Informações da Query"):
        st.write("**Colunas retornadas:**")
        st.write(", ".join(df.columns.tolist()))
        st.write(f"\n**Tipos de dados:**")
        st.write(df.dtypes)

else:
    # Tela inicial - sem dados
    st.info("👈 Configure os **filtros** na barra lateral e clique em **🚀 Executar Query SQL**")
    
    st.markdown("""
    ### 📖 Como usar:
    
    1. **Configure os filtros** na barra lateral:
       - 📅 **Data Inicial e Final** - defina o período desejado
       - 🏢 **Fundo** - selecione um fundo específico ou "Todos"
       - ⚙️ **Serviço** - escolha Administração, Gestão, Custódia, etc.
    
    2. Clique em **🚀 Executar Query SQL**
    
    3. Aguarde a execução (pode levar alguns minutos)
    
    4. Os resultados aparecerão automaticamente já filtrados
    
    5. Use a **busca adicional** se necessário
    
    6. **Exporte para CSV** quando pronto
    
    ### ✨ Vantagens:
    
    - ✅ **100% preciso** - usa a query SQL original do BigQuery
    - ✅ **Filtros antes da execução** - retorna apenas dados relevantes
    - ✅ **Cache inteligente** (10 min) - não reexecuta desnecessariamente
    - ✅ **Super rápido** - filtra milhões de registros em segundos
    - ✅ **Sempre atualizado** - dados direto do BigQuery
    """)

# Rodapé
st.divider()
st.markdown("""
<div style='text-align: center; padding: 30px; background-color: #f3f2f3; border-radius: 10px; margin-top: 40px;'>
    <img src='https://www.kanastra.design/symbol-green.svg' style='height: 40px; margin-bottom: 15px;' alt='Kanastra'>
    <p style='color: #193c32; font-weight: 500; margin: 10px 0;'>
        © 2025 Kanastra - Calculadora de Taxas 5.0
    </p>
    <p style='color: #14735a; font-size: 0.9em; margin: 5px 0;'>
        Desenvolvido com Python 🐍 + Streamlit ⚡ + Google BigQuery ☁️
    </p>
    <p style='color: #2daa82; font-size: 0.85em; margin-top: 10px;'>
        <a href='https://www.kanastra.design/' target='_blank' style='color: #2daa82; text-decoration: none;'>
            Identidade Visual Kanastra →
        </a>
    </p>
</div>
""", unsafe_allow_html=True)
