"""
Dashboard Streamlit - Gestão de Taxas com BigQuery
Planilha sempre visível + Formulários personalizados
"""
import streamlit as st
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import uuid
import json

# Configuração da página
st.set_page_config(
    page_title="Gestão de Taxas - Kanastra",
    page_icon="https://www.kanastra.design/symbol.svg",
    layout="wide",
    initial_sidebar_state="collapsed"
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
    .stSelectbox label, .stMultiSelect label, .stTextInput label, .stNumberInput label, .stDateInput label {
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
    
    /* Text/Number inputs */
    .stTextInput > div > div > input, .stNumberInput > div > div > input {
        border-radius: 10px !important;
        border: 2px solid #e9ecef !important;
        transition: all 0.3s ease !important;
    }
    
    .stTextInput > div > div > input:focus, .stNumberInput > div > div > input:focus {
        border-color: #2daa82 !important;
        box-shadow: 0 0 0 3px rgba(45, 170, 130, 0.1) !important;
    }
    
    /* Date inputs */
    .stDateInput > div > div > input {
        border-radius: 10px !important;
        border: 2px solid #e9ecef !important;
    }
    
    /* Checkboxes */
    .stCheckbox {
        background: white;
        padding: 0.75rem;
        border-radius: 10px;
        border: 1px solid #e9ecef;
        transition: all 0.3s ease;
    }
    
    .stCheckbox:hover {
        border-color: #2daa82;
        box-shadow: 0 2px 8px rgba(45, 170, 130, 0.1);
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
    
    /* Tabs modernas */
    .stTabs [data-baseweb="tab-list"] {
        gap: 1rem;
        background: white;
        padding: 0.75rem;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
    
    .stTabs [data-baseweb="tab"] {
        color: #193c32;
        font-weight: 600;
        padding: 0.75rem 1.5rem;
        border-radius: 8px;
        transition: all 0.3s ease;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background-color: #f8f9fa;
    }
    
    .stTabs [aria-selected="true"] {
        color: white;
        background: linear-gradient(135deg, #14735a 0%, #2daa82 100%);
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(20, 115, 90, 0.3);
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
        <h1 style="margin: 0;">Dashboard - Gestão de Taxas</h1>
        <p style="margin: 0; color: #14735a; font-weight: 500;">Kanastra Finance | Sistema de gerenciamento de taxas mínimas e variáveis</p>
    </div>
</div>
""", unsafe_allow_html=True)

# Configurações de usuários
USUARIOS = {
    # Aprovadores - podem aprovar/rejeitar alterações
    "EricIsamo": {
        "senha": "kanastra2025", 
        "nome": "Eric Isamo",
        "perfil": "aprovador",
        "email": "eric@kanastra.com"
    },
    "ThiagoGarcia": {
        "senha": "kanastra2025", 
        "nome": "Thiago Garcia",
        "perfil": "aprovador",
        "email": "thiago@kanastra.com"
    },
    
    # Editores - podem adicionar/editar, mas precisam de aprovação
    "GustavoPrometti": {
        "senha": "editor2025", 
        "nome": "Gustavo Prometti",
        "perfil": "editor",
        "email": "gustavo.prometti@kanastra.com.br"
    },
    "FinanceUser": {
        "senha": "editor2025", 
        "nome": "Usuário Finance",
        "perfil": "editor",
        "email": "finance@kanastra.com"
    }
}

# Manter compatibilidade com código antigo
APROVADORES = {k: v for k, v in USUARIOS.items() if v.get("perfil") == "aprovador"}

# Inicializar session_state
if 'dados_originais' not in st.session_state:
    st.session_state.dados_originais = None
if 'dados_editados' not in st.session_state:
    st.session_state.dados_editados = None
if 'alteracoes_pendentes' not in st.session_state:
    st.session_state.alteracoes_pendentes = []
if 'usuario_logado' not in st.session_state:
    st.session_state.usuario_logado = None
if 'perfil_usuario' not in st.session_state:
    st.session_state.perfil_usuario = None
if 'usuario_aprovador' not in st.session_state:  # Manter compatibilidade
    st.session_state.usuario_aprovador = None
if 'tabela_selecionada' not in st.session_state:
    st.session_state.tabela_selecionada = None

# Função para criar cliente BigQuery
@st.cache_resource
def get_bigquery_client():
    try:
        if "gcp_service_account" in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(credentials=credentials, project='kanastra-live')
    except:
        pass
    try:
        return bigquery.Client(project='kanastra-live')
    except Exception as e:
        st.error(f"❌ Erro ao criar cliente BigQuery: {e}")
        return None

# Função para carregar dados
@st.cache_data(ttl=300)
def carregar_dados_bigquery(tabela):
    client = get_bigquery_client()
    if client is None:
        return None
    
    try:
        if tabela == "fee_minimo":
            query = """
            SELECT 
                empresa,
                `fund id` as fund_id,
                cliente,
                servico,
                faixa,
                fee_min
            FROM `kanastra-live.finance.fee_minimo`
            ORDER BY `fund id`
            """
        else:
            query = """
            SELECT 
                empresa,
                `fund id` as fund_id,
                cliente,
                servico,
                faixa,
                fee_variavel
            FROM `kanastra-live.finance.fee_variavel`
            ORDER BY `fund id`, faixa
            """
        
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"❌ Erro ao carregar dados: {e}")
        return None

@st.cache_data(ttl=3600)
def carregar_fundos_completos():
    """Carrega lista de fundos com ID, nome, CNPJ e cliente para criação de taxas"""
    try:
        client = get_bigquery_client()
        if client is None:
            return pd.DataFrame()
        
        query = """
        SELECT 
            id as fund_id,
            name as fund_name,
            government_id as cnpj,
            name as client
        FROM `kanastra-live.hub.funds` 
        WHERE name IS NOT NULL 
        ORDER BY name
        """
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"❌ Erro ao carregar fundos completos: {e}")
        return pd.DataFrame()

# Funções para persistência de alterações pendentes
def salvar_alteracao_pendente(tipo_alteracao, tabela, dados, usuario="usuario_kanastra", solicitacao_id=None, tipo_categoria=None, origem=None):
    """
    Salva uma alteração pendente no BigQuery
    
    Args:
        tipo_alteracao: INSERT, UPDATE ou DELETE (tipo de operação)
        tabela: Nome da tabela (fee_minimo, fee_variavel, waiver, desconto)
        dados: Dict com os dados da alteração
        usuario: Nome do usuário que criou
        solicitacao_id: UUID para agrupar linhas relacionadas
        tipo_categoria: Categoria (taxa_minima, taxa_variavel, waiver, desconto)
        origem: Para descontos - 'juridico' ou 'comercial'
    """
    client = get_bigquery_client()
    if client is None:
        st.error("❌ Erro ao conectar com BigQuery")
        return False, None
    
    try:
        alteracao_id = str(uuid.uuid4())
        timestamp_now = datetime.now().isoformat()
        
        # Se não foi passado um solicitacao_id, criar um novo (para agrupar linhas relacionadas)
        if solicitacao_id is None:
            solicitacao_id = str(uuid.uuid4())
        
        # Determinar tipo_categoria se não especificado
        if not tipo_categoria:
            if tabela == 'fee_minimo':
                tipo_categoria = 'taxa_minima'
            elif tabela == 'fee_variavel':
                tipo_categoria = 'taxa_variavel'
            elif tabela == 'waiver':
                tipo_categoria = 'waiver'
            else:
                tipo_categoria = 'desconto'
        
        # Converter dados para JSON string
        dados_json = json.dumps(dados, ensure_ascii=False)
        
        # Query com suporte a tipo_alteracao_categoria e origem
        if origem:  # Para descontos
            query = f"""
            INSERT INTO `kanastra-live.finance.alteracoes_pendentes` 
            (id, usuario, timestamp, tipo_alteracao, tipo_alteracao_categoria, origem, tabela, dados, status, solicitacao_id)
            VALUES (
                '{alteracao_id}',
                '{usuario}',
                TIMESTAMP('{timestamp_now}'),
                '{tipo_alteracao}',
                '{tipo_categoria}',
                '{origem}',
                '{tabela}',
                JSON '{dados_json}',
                'PENDENTE',
                '{solicitacao_id}'
            )
            """
        else:  # Para taxas e waivers
            query = f"""
            INSERT INTO `kanastra-live.finance.alteracoes_pendentes` 
            (id, usuario, timestamp, tipo_alteracao, tipo_alteracao_categoria, tabela, dados, status, solicitacao_id)
            VALUES (
                '{alteracao_id}',
                '{usuario}',
                TIMESTAMP('{timestamp_now}'),
                '{tipo_alteracao}',
                '{tipo_categoria}',
                '{tabela}',
                JSON '{dados_json}',
                'PENDENTE',
                '{solicitacao_id}'
            )
            """
        
        client.query(query).result()
        return True, solicitacao_id
    except Exception as e:
        st.error(f"❌ Erro ao salvar alteração: {e}")
        return False, None

def carregar_alteracoes_pendentes():
    """Carrega todas as alterações pendentes do BigQuery agrupadas por solicitacao_id"""
    client = get_bigquery_client()
    if client is None:
        return []
    
    try:
        # Primeiro, tentar adicionar a coluna solicitacao_id se não existir
        try:
            alter_query = """
            ALTER TABLE `kanastra-live.finance.alteracoes_pendentes`
            ADD COLUMN IF NOT EXISTS solicitacao_id STRING
            """
            client.query(alter_query).result()
        except:
            pass  # Coluna já existe ou erro ao adicionar
        
        # Verificar se a coluna existe antes de consultar
        check_query = """
        SELECT column_name 
        FROM `kanastra-live.finance.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'alteracoes_pendentes'
        """
        columns_df = client.query(check_query).to_dataframe()
        has_solicitacao_id = 'solicitacao_id' in columns_df['column_name'].values
        
        # Montar query baseado na existência da coluna
        if has_solicitacao_id:
            query = """
            SELECT 
                id,
                usuario,
                timestamp,
                tipo_alteracao,
                tabela,
                dados,
                status,
                solicitacao_id
            FROM `kanastra-live.finance.alteracoes_pendentes`
            WHERE status = 'PENDENTE'
            ORDER BY timestamp ASC, solicitacao_id
            """
        else:
            # Fallback: usar id como solicitacao_id se a coluna não existir
            query = """
            SELECT 
                id,
                usuario,
                timestamp,
                tipo_alteracao,
                tabela,
                dados,
                status,
                id as solicitacao_id
            FROM `kanastra-live.finance.alteracoes_pendentes`
            WHERE status = 'PENDENTE'
            ORDER BY timestamp ASC
            """
        
        df = client.query(query).to_dataframe()
        
        # Agrupar alterações por solicitacao_id
        solicitacoes = {}
        for _, row in df.iterrows():
            solicitacao_id = row.get('solicitacao_id', row['id'])  # Fallback para id se não tiver solicitacao_id
            
            alteracao = {
                'id': row['id'],
                'usuario': row['usuario'],
                'timestamp': row['timestamp'],
                'tipo_alteracao': row['tipo_alteracao'],
                'tabela': row['tabela'],
                'dados': json.loads(row['dados']),
                'status': row['status'],
                'solicitacao_id': solicitacao_id
            }
            
            if solicitacao_id not in solicitacoes:
                solicitacoes[solicitacao_id] = []
            solicitacoes[solicitacao_id].append(alteracao)
        
        # Retornar lista de solicitações agrupadas
        return list(solicitacoes.values())
    except Exception as e:
        st.error(f"❌ Erro ao carregar alterações pendentes: {e}")
        return []

def carregar_historico_alteracoes(limit=100):
    """Carrega histórico de alterações já aprovadas (waivers e descontos da tabela descontos)"""
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()
    
    try:
        query = f"""
        SELECT 
            data_aplicacao as data_aprovacao,
            usuario as aprovador_por,
            categoria,
            fund_id,
            fund_name,
            tipo_desconto,
            valor_desconto,
            percentual_desconto,
            forma_aplicacao,
            servico,
            origem,
            data_inicio,
            data_fim,
            observacao
        FROM `kanastra-live.finance.descontos`
        ORDER BY data_aplicacao DESC
        LIMIT {limit}
        """
        
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.warning(f"⚠️ Erro ao carregar histórico: {e}")
        return pd.DataFrame()

def atualizar_status_alteracao(alteracao_id, novo_status, aprovador=None):
    """Atualiza o status de uma alteração (APROVADO/REJEITADO) e registra quem aprovou"""
    client = get_bigquery_client()
    if client is None:
        return False
    
    try:
        # Tentar adicionar a coluna aprovador_por se não existir (ignora erro se já existir)
        try:
            alter_query = """
            ALTER TABLE `kanastra-live.finance.alteracoes_pendentes`
            ADD COLUMN IF NOT EXISTS aprovador_por STRING
            """
            client.query(alter_query).result()
        except:
            pass  # Coluna já existe
        
        # Atualizar status e aprovador
        if aprovador:
            query = f"""
            UPDATE `kanastra-live.finance.alteracoes_pendentes`
            SET status = '{novo_status}',
                aprovador_por = '{aprovador}'
            WHERE id = '{alteracao_id}'
            """
        else:
            query = f"""
            UPDATE `kanastra-live.finance.alteracoes_pendentes`
            SET status = '{novo_status}'
            WHERE id = '{alteracao_id}'
            """
        
        client.query(query).result()
        return True
    except Exception as e:
        st.error(f"❌ Erro ao atualizar status: {e}")
        return False

# FUNÇÃO REMOVIDA: salvar_historico_alteracao
# A tabela finance.historico_alteracoes não existe mais
# Usamos finance.descontos como fonte única de histórico

# =======================
# VERIFICAÇÃO DE LOGIN - BLOQUEIO TOTAL
# =======================

# Verificar se usuário está logado ANTES de mostrar qualquer coisa
if not st.session_state.usuario_logado:
    st.markdown("---")
    st.subheader("🔐 Login do Sistema")
    st.info("💡 **Editores** podem adicionar/editar taxas | **Aprovadores** podem aprovar alterações")
    
    col_login1, col_login2, col_login3 = st.columns([1, 1, 2])
    
    with col_login1:
        usuario = st.text_input("Usuário", key="usuario", placeholder="Digite seu usuário")
    
    with col_login2:
        senha = st.text_input("Senha", type="password", key="senha")
    
    with col_login3:
        if st.button("🔓 Entrar", width='stretch', type="primary"):
            if usuario in USUARIOS and USUARIOS[usuario]["senha"] == senha:
                # Login bem-sucedido
                st.session_state.usuario_logado = usuario
                st.session_state.perfil_usuario = USUARIOS[usuario]["perfil"]
                
                # Manter compatibilidade com código antigo
                if USUARIOS[usuario]["perfil"] == "aprovador":
                    st.session_state.usuario_aprovador = usuario
                
                st.success(f"✅ Login realizado como **{USUARIOS[usuario]['nome']}** ({USUARIOS[usuario]['perfil'].upper()})")
                st.rerun()
            else:
                st.error("❌ Credenciais incorretas!")
    
    st.markdown("---")
    st.info("🔒 **Faça login para acessar o dashboard de gestão de taxas**")
    
    # Mostrar contador de alterações pendentes mesmo sem login
    alteracoes_nao_logado = carregar_alteracoes_pendentes()
    if alteracoes_nao_logado:
        st.warning(f"⏳ {len(alteracoes_nao_logado)} alteração(ões) aguardando aprovação. Faça login para revisar.")
    
    st.stop()  # PARAR AQUI - NÃO MOSTRAR MAIS NADA

# =======================
# USUÁRIO LOGADO - MOSTRAR INFORMAÇÕES
# =======================

perfil = st.session_state.perfil_usuario
nome = USUARIOS[st.session_state.usuario_logado]['nome']

# Ícones por perfil
icone_perfil = "👑" if perfil == "aprovador" else "✏️"
cor_perfil = "green" if perfil == "aprovador" else "blue"

col_user1, col_user2 = st.columns([3, 1])

with col_user1:
    st.markdown(f"""
    <div style='background-color: #{cor_perfil}22; padding: 15px; border-radius: 8px; border-left: 4px solid #{cor_perfil};'>
        <p style='margin: 0; font-size: 16px;'>
            {icone_perfil} <strong>{nome}</strong> ({st.session_state.usuario_logado})
        </p>
        <p style='margin: 5px 0 0 0; font-size: 14px; color: #666;'>
            Perfil: <strong>{perfil.upper()}</strong> | Email: {USUARIOS[st.session_state.usuario_logado]['email']}
        </p>
    </div>
    """, unsafe_allow_html=True)

with col_user2:
    if st.button("🚪 Sair", width='stretch', type="secondary"):
        st.session_state.usuario_logado = None
        st.session_state.perfil_usuario = None
        st.session_state.usuario_aprovador = None
        st.rerun()

st.markdown("---")

# =======================
# SIDEBAR - INFORMAÇÕES E STATUS
# =======================

with st.sidebar:
    st.image("https://www.kanastra.design/wordmark-green.svg", width=150)
    st.markdown("---")
    
    # Informações do usuário logado
    if st.session_state.usuario_logado:
        perfil_emoji = "👑" if perfil == "aprovador" else "✏️"
        perfil_nome = "Aprovador" if perfil == "aprovador" else "Editor"
        
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%);
            padding: 1rem;
            border-radius: 12px;
            border-left: 4px solid #2196f3;
            margin-bottom: 1rem;
        ">
            <div style="color: #1565c0; font-weight: 600; margin-bottom: 0.5rem;">
                {perfil_emoji} {perfil_nome}
            </div>
            <div style="color: #0d47a1; font-size: 0.9rem;">
                {st.session_state.usuario_logado}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Quick stats
    st.markdown("### 📊 Status Rápido")
    
    # Verificar alterações pendentes
    solicitacoes_pendentes_sidebar = carregar_alteracoes_pendentes()
    if perfil == "editor":
        minhas_solicitacoes = [s for s in solicitacoes_pendentes_sidebar if s[0].get('usuario') == st.session_state.usuario_logado]
        total_minhas = len(minhas_solicitacoes)
        if total_minhas > 0:
            st.warning(f"⏳ {total_minhas} suas solicitações pendentes")
        else:
            st.success("✅ Nenhuma solicitação sua pendente")
    else:
        total_todas = len(solicitacoes_pendentes_sidebar)
        if total_todas > 0:
            st.warning(f"⏳ {total_todas} solicitações para revisar")
        else:
            st.success("✅ Nenhuma solicitação pendente")
    
    st.markdown("---")
    
    # Informações úteis
    st.markdown("### ℹ️ Funções")
    st.markdown("""
    **📋 Taxas**
    - Taxa Mínima
    - Taxa Variável
    
    **💰 Waivers**
    - Fixo
    - Percentual
    
    **🎯 Descontos**
    - Jurídico
    - Comercial
    """)

# =======================
# NAVEGAÇÃO POR ABAS
# =======================

# Navegação com Tabs no topo (estilo moderno)
st.markdown("---")

# Criar tabs para navegação
tab1, tab2, tab3 = st.tabs([
    "📋 Criação/Alteração de Taxas - Regulamento",
    "💰 Waivers",
    "🎯 Descontos"
])

# =======================
# TAB 1: CRIAÇÃO/ALTERAÇÃO DE TAXAS - REGULAMENTO
# =======================

with tab1:
    
    st.header("📋 Criação/Alteração de Taxas - Regulamento")
    st.markdown("---")
    
    # =======================
    # SEÇÃO 1: SELEÇÃO E CARREGAMENTO
    # =======================

    st.subheader("📋 Selecione e Carregue os Dados")

    opcoes_tabela = {
        "Taxa Mínima": "fee_minimo",
        "Taxa Variável": "fee_variavel"
    }

    col1, col2 = st.columns([2, 1])

    with col1:
        tabela_display = st.selectbox(
            "Selecione o Tipo de Taxa:",
            list(opcoes_tabela.keys()),
            key="select_tabela"
        )
        tabela = opcoes_tabela[tabela_display]

    with col2:
        if st.button("📊 Carregar Dados", width='stretch', type="primary"):
            # Limpar cache antes de carregar novos dados
            carregar_dados_bigquery.clear()
            
            with st.spinner("Carregando..."):
                df = carregar_dados_bigquery(tabela)
                if df is not None and not df.empty:
                    st.session_state.dados_originais = df.copy()
                    st.session_state.dados_editados = df.copy()
                    st.session_state.tabela_selecionada = tabela
                    st.success(f"✅ {len(df)} registros carregados!")
                    

                elif df is not None:
                    st.warning("⚠️ Tabela vazia")
                else:
                    st.error("❌ Erro ao carregar")

    st.markdown("---")

    # =======================
    # SEÇÃO 2: ESCOLHA DA AÇÃO E FORMULÁRIOS
    # =======================

    if st.session_state.dados_editados is not None:
        # VALIDAÇÃO CRÍTICA: Verificar se a tabela selecionada corresponde aos dados carregados
        if st.session_state.tabela_selecionada != tabela:
            st.error("❌ **ATENÇÃO: Incompatibilidade detectada!**")
            st.warning(f"⚠️ Você selecionou **'{tabela_display}'** mas os dados carregados são de **'{[k for k, v in opcoes_tabela.items() if v == st.session_state.tabela_selecionada][0]}'**")
            st.info("👉 **SOLUÇÃO:** Clique no botão '📊 Carregar Dados' acima para carregar os dados corretos.")
            
            # Botão para forçar recarga
            if st.button("🔄 Recarregar Dados Corretos", type="primary"):
                carregar_dados_bigquery.clear()
                df = carregar_dados_bigquery(tabela)
                if df is not None and not df.empty:
                    st.session_state.dados_originais = df.copy()
                    st.session_state.dados_editados = df.copy()
                    st.session_state.tabela_selecionada = tabela
                    st.success(f"✅ {len(df)} registros de {tabela_display} carregados!")
                    st.rerun()
            st.stop()  # NÃO MOSTRAR MAIS NADA ATÉ CORRIGIR
        
        st.subheader("🔧 Escolha a Ação")
        
        acao = st.radio(
            "O que deseja fazer?",
            ["Criar Nova Taxa", "Editar Taxa Existente"],
            key="radio_acao",
            horizontal=True
        )
        
        st.markdown("---")
    
        # =======================
        # SEÇÃO 3: FORMULÁRIOS ESPECÍFICOS (4 DIFERENTES)
        # =======================
        
        # FORMULÁRIO 1: Taxa Mínima + Criar
        if st.session_state.tabela_selecionada == "fee_minimo" and acao == "Criar Nova Taxa":
            st.subheader("➕ Criar Nova Taxa Mínima")
            
            with st.form("form_criar_taxa_minima"):
                st.markdown("### 📝 Preencha os dados da nova taxa")
                st.info("ℹ️ A taxa mínima será aplicada independente do PL. Serão criadas automaticamente 2 linhas (faixa 0 e faixa máxima).")
                
                # Carregar fundos do BigQuery
                df_fundos = carregar_fundos_completos()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Criar lista de opções com nome do cliente (fund_name)
                    opcoes_clientes = [f"{row['client']} (ID: {row['fund_id']})" for _, row in df_fundos.iterrows()]
                    cliente_selecionado = st.selectbox(
                        "Cliente",
                        options=opcoes_clientes,
                        help="Selecione o cliente (nome do fundo)"
                    )
                    # Extrair fund_id da seleção
                    idx_selecionado = opcoes_clientes.index(cliente_selecionado)
                    fund_id = int(df_fundos.iloc[idx_selecionado]['fund_id'])
                    cnpj = df_fundos.iloc[idx_selecionado]['cnpj']
                    cliente = df_fundos.iloc[idx_selecionado]['client']
                
                with col2:
                    servico = st.selectbox(
                        "Serviço",
                        ["Administração", "Gestão", "Custódia", "Agente Monitoramento", "Performance"]
                    )
                
                fee_min = st.number_input("Fee Mínimo (R$)", min_value=0.0, step=100.0, format="%.2f")
                
                submitted = st.form_submit_button("➕ Criar Taxa Mínima", width='stretch', type="primary")
                
                if submitted:
                    # Criar DUAS linhas: faixa 0 e faixa máxima (1000000000000000)
                    taxa_faixa_0 = {
                        "empresa": "a",
                        "fund_id": fund_id,
                        "cliente": cliente,
                        "servico": servico,
                        "faixa": 0.0,
                        "fee_min": fee_min
                    }
                
                    taxa_faixa_max = {
                        "empresa": "a",
                        "fund_id": fund_id,
                        "cliente": cliente,
                        "servico": servico,
                        "faixa": 1000000000000000.0,
                        "fee_min": fee_min
                    }
                
                    # Salvar no BigQuery (com usuário logado)
                    usuario_atual = st.session_state.get('usuario_logado', 'usuario_kanastra')
                    solicitacao_id = str(uuid.uuid4())  # Mesmo ID para agrupar as 2 linhas
                    sucesso_1, _ = salvar_alteracao_pendente("INSERT", "fee_minimo", taxa_faixa_0, usuario_atual, solicitacao_id)
                    sucesso_2, _ = salvar_alteracao_pendente("INSERT", "fee_minimo", taxa_faixa_max, usuario_atual, solicitacao_id)
                    
                    if sucesso_1 and sucesso_2:
                        st.success(f"✅ Taxa mínima criada! Cliente: {cliente} - {servico} - 2 linhas adicionadas (faixa 0 e máxima)")
                        st.info("⏳ Aguardando aprovação de um aprovador")
                        st.rerun()
                    else:
                        st.error("❌ Erro ao salvar taxa mínima")
    
    
        # FORMULÁRIO 2: Taxa Mínima + Editar
        elif st.session_state.tabela_selecionada == "fee_minimo" and acao == "Editar Taxa Existente":
            st.subheader("✏️ Editar Taxa Mínima Existente")
        
            with st.form("form_editar_taxa_minima"):
                st.markdown("### 📝 Selecione o fundo e serviço para editar o fee mínimo")
            
                col1, col2, col3 = st.columns(3)
            
                with col1:
                    # Listar todos os clientes disponíveis
                    df = st.session_state.dados_editados
                    clientes_disponiveis = sorted(df['cliente'].unique())
                    cliente_edit = st.selectbox(
                        "Selecione o Cliente",
                        options=clientes_disponiveis
                    )
            
                with col2:
                    servico_edit = st.selectbox(
                        "Selecione o Serviço",
                        ["Administração", "Gestão", "Custódia", "Agente Monitoramento", "Performance"]
                    )
            
                with col3:
                    novo_fee_min = st.number_input("Novo Fee Mínimo (R$)", min_value=0.0, step=100.0, format="%.2f")
            
                submitted_edit = st.form_submit_button("💾 Salvar Novo Valor", width='stretch', type="primary")
            
                if submitted_edit:
                    # Buscar o registro pelo cliente e serviço
                    df = st.session_state.dados_editados
                    registro = df[(df['cliente'] == cliente_edit) & (df['servico'] == servico_edit)]
                
                    if not registro.empty:
                        reg_data = registro.iloc[0]
                    
                        taxa_editada = {
                            "empresa": "a",
                            "fund_id": int(reg_data['fund_id']),  # Mantém o valor original
                            "cliente": reg_data['cliente'],  # Mantém o valor original
                            "servico": servico_edit,
                            "faixa": float(reg_data['faixa']),  # Mantém o valor original
                            "fee_min": novo_fee_min,  # Apenas este valor é editado
                            "original_lower": float(reg_data['faixa'])  # Chave para UPDATE
                        }
                    
                        # Salvar no BigQuery (com usuário logado)
                        usuario_atual = st.session_state.get('usuario_logado', 'usuario_kanastra')
                        sucesso, _ = salvar_alteracao_pendente("UPDATE", "fee_minimo", taxa_editada, usuario_atual)
                        if sucesso:
                            st.success(f"✅ Fee mínimo atualizado! Cliente: {cliente_edit} - {servico_edit} - Novo valor: R$ {novo_fee_min:,.2f}")
                            st.info("⏳ Aguardando aprovação de um aprovador")
                            st.rerun()
                        else:
                            st.error("❌ Erro ao salvar alteração")
                    else:
                        st.error(f"❌ Registro não encontrado para Cliente {cliente_edit} e Serviço {servico_edit}")

        # FORMULÁRIO 3: Taxa Variável + Criar
        elif st.session_state.tabela_selecionada == "fee_variavel" and acao == "Criar Nova Taxa":
            st.subheader("➕ Criar Nova Taxa Variável")
        
            # Inicializar estado para múltiplas faixas
            if 'faixas_variavel' not in st.session_state:
                st.session_state.faixas_variavel = []
        
            with st.form("form_criar_taxa_variavel"):
                st.markdown("### 📝 Informações básicas")
                
                # Carregar fundos do BigQuery
                df_fundos_var = carregar_fundos_completos()
            
                col1, col2 = st.columns(2)
            
                with col1:
                    # Criar lista de opções com nome do cliente (fund_name)
                    opcoes_clientes_var = [f"{row['client']} (ID: {row['fund_id']})" for _, row in df_fundos_var.iterrows()]
                    cliente_selecionado_var = st.selectbox(
                        "Cliente",
                        options=opcoes_clientes_var,
                        key="var_cliente_select",
                        help="Selecione o cliente (nome do fundo)"
                    )
                    # Extrair fund_id da seleção
                    idx_selecionado_var = opcoes_clientes_var.index(cliente_selecionado_var)
                    fund_id_var = int(df_fundos_var.iloc[idx_selecionado_var]['fund_id'])
                    cnpj_var = df_fundos_var.iloc[idx_selecionado_var]['cnpj']
                    cliente_var = df_fundos_var.iloc[idx_selecionado_var]['client']
            
                with col2:
                    servico_var = st.selectbox(
                        "Serviço",
                        ["Administração", "Gestão", "Performance", "Custódia"],
                        key="var_service"
                    )
            
                st.markdown("---")
                st.markdown("### 📊 Faixas de PL e Taxas")
                st.info("ℹ️ Será criada 1 linha no BigQuery para cada faixa. Ex: 3 faixas = 3 linhas no banco de dados")
            
                # Número de faixas
                num_faixas = st.number_input("Quantas faixas deseja criar?", min_value=1, max_value=10, value=2, step=1)
            
                faixas_data = []
            
                for i in range(num_faixas):
                    st.markdown(f"**Faixa {i+1}:**")
                    col_a, col_b = st.columns(2)
                
                    with col_a:
                        faixa_pl = st.number_input(
                            f"PL Mínimo (R$)", 
                            min_value=0.0, 
                            step=1000000.0, 
                            format="%.0f",
                            key=f"faixa_{i}"
                        )
                
                    with col_b:
                        fee_pct = st.number_input(
                            f"Taxa Variável (%)", 
                            min_value=0.0, 
                            max_value=100.0, 
                            step=0.0001, 
                            format="%.4f",
                            key=f"fee_var_{i}"
                        )
                
                    faixas_data.append({
                        "faixa": faixa_pl,
                        "fee_variavel": fee_pct
                    })
                
                submitted_var = st.form_submit_button("➕ Criar Taxas Variáveis", width='stretch', type="primary")
            
                if submitted_var:
                    # Criar uma linha para cada faixa com mesmo solicitacao_id
                    usuario_atual = st.session_state.get('usuario_logado', 'usuario_kanastra')
                    solicitacao_id = str(uuid.uuid4())  # Mesmo ID para agrupar todas as faixas
                    sucesso = True
                
                    for faixa in faixas_data:
                        nova_taxa = {
                            "empresa": "a",
                            "fund_id": fund_id_var,
                            "cliente": cliente_var,
                            "servico": servico_var,
                            "faixa": faixa["faixa"],
                            "fee_variavel": faixa["fee_variavel"]
                        }
                    
                        resultado, _ = salvar_alteracao_pendente("INSERT", "fee_variavel", nova_taxa, usuario_atual, solicitacao_id)
                        if not resultado:
                            sucesso = False
                            break
                
                    if sucesso:
                        st.success(f"✅ {len(faixas_data)} faixa(s) de taxa variável criada(s)! Cliente: {cliente_var} - {servico_var}")
                        st.info("⏳ Aguardando aprovação de um aprovador")
                        st.rerun()
                    else:
                        st.error("❌ Erro ao salvar uma ou mais linhas")
    
        # FORMULÁRIO 4: Taxa Variável + Editar
        elif st.session_state.tabela_selecionada == "fee_variavel" and acao == "Editar Taxa Existente":
            st.subheader("✏️ Editar Taxa Variável Existente")
        
            with st.form("form_editar_taxa_variavel"):
                st.markdown("### 📝 Selecione o cliente e serviço para editar todas as faixas")
            
                col1, col2 = st.columns(2)
            
                with col1:
                    # Listar todos os clientes disponíveis
                    df = st.session_state.dados_editados
                    clientes_disponiveis_var = sorted(df['cliente'].unique())
                    cliente_edit_var = st.selectbox(
                        "Selecione o Cliente",
                        options=clientes_disponiveis_var,
                        key="edit_var_cliente"
                    )
            
                with col2:
                    servico_edit_var = st.selectbox(
                        "Selecione o Serviço",
                        ["Administração", "Gestão", "Performance", "Custódia"],
                        key="edit_var_service"
                    )
            
                submitted_buscar = st.form_submit_button("🔍 Carregar Faixas para Edição", width='stretch', type="primary")
            
                if submitted_buscar:
                    df = st.session_state.dados_editados
                
                    # Buscar todas as faixas deste cliente+serviço
                    registros = df[(df['cliente'] == cliente_edit_var) & (df['servico'] == servico_edit_var)]
                
                    if not registros.empty:
                        # Ordenar por faixa
                        registros = registros.sort_values('faixa')
                        st.session_state.faixas_var_para_editar = registros.to_dict('records')
                        st.success(f"✅ {len(registros)} faixas encontradas! Atualize os valores abaixo.")
                        st.rerun()
                    else:
                        st.error(f"❌ Nenhuma faixa encontrada para {cliente_edit_var} - {servico_edit_var}")
        
            # Se há faixas carregadas, mostrar formulário de edição
            if 'faixas_var_para_editar' in st.session_state and st.session_state.faixas_var_para_editar:
                st.markdown("---")
            
                with st.form("form_atualizar_faixas_variavel"):
                    st.markdown("### 📊 Edite as faixas abaixo")
                    st.info(f"ℹ️ Total de {len(st.session_state.faixas_var_para_editar)} linha(s) para editar")
                
                    faixas_editadas = []
                
                    for idx, faixa in enumerate(st.session_state.faixas_var_para_editar):
                        st.markdown(f"**Linha {idx + 1}:**")
                        col_a, col_b = st.columns(2)
                    
                        with col_a:
                            faixa_edit = st.number_input(
                                f"Faixa (PL Mínimo R$)",
                                value=float(faixa['faixa']),
                                min_value=0.0,
                                step=1000000.0,
                                format="%.0f",
                                key=f"edit_faixa_{idx}"
                            )
                    
                        with col_b:
                            fee_edit = st.number_input(
                                f"Taxa Variável (%)",
                                value=float(faixa['fee_variavel']),
                                min_value=0.0,
                                max_value=100.0,
                                step=0.0001,
                                format="%.4f",
                                key=f"edit_fee_var_{idx}"
                            )
                    
                        faixas_editadas.append({
                            "empresa": faixa['empresa'],
                            "fund_id": int(faixa['fund_id']),
                            "cliente": faixa['cliente'],
                            "servico": faixa['servico'],
                            "faixa": faixa_edit,
                            "fee_variavel": fee_edit,
                            "original_faixa": float(faixa['faixa'])  # Para identificar qual linha atualizar
                        })
                
                    st.markdown("---")
                
                    col_btn1, col_btn2 = st.columns(2)
                
                    with col_btn1:
                        submitted_update = st.form_submit_button("💾 Salvar Todas as Alterações", width='stretch', type="primary")
                
                    with col_btn2:
                        cancelar_update = st.form_submit_button("❌ Cancelar", width='stretch')
                
                    if submitted_update:
                        # Salvar todas as faixas editadas no BigQuery com mesmo solicitacao_id
                        sucesso = True
                        usuario_atual = st.session_state.get('usuario_logado', 'usuario_kanastra')
                        solicitacao_id = str(uuid.uuid4())  # Mesmo ID para agrupar todas as edições
                    
                        for faixa_edit in faixas_editadas:
                            resultado, _ = salvar_alteracao_pendente("UPDATE", "fee_variavel", faixa_edit, usuario_atual, solicitacao_id)
                            if not resultado:
                                sucesso = False
                                break
                    
                        if sucesso:
                            st.success(f"✅ {len(faixas_editadas)} faixa(s) atualizada(s)! Cliente: {faixas_editadas[0]['cliente']}")
                            st.info("⏳ Aguardando aprovação de um aprovador")
                            del st.session_state.faixas_var_para_editar
                            st.rerun()
                        else:
                            st.error("❌ Erro ao salvar uma ou mais alterações")
                
                    if cancelar_update:
                        del st.session_state.faixas_var_para_editar
                        st.rerun()

    
        # =======================
        # SEÇÃO 4: VISUALIZAÇÃO DA PLANILHA
        # =======================
    
        st.markdown("---")
        st.subheader(f"📊 Dados da {tabela_display}")
    
        # Filtros
        col_filtro1, col_filtro2, col_filtro3 = st.columns([2, 2, 1])
    
        with col_filtro1:
            # Filtro por cliente
            clientes_unicos = ["Todos"] + sorted(st.session_state.dados_editados['cliente'].unique().tolist())
            cliente_filtro = st.selectbox("🔍 Filtrar por Cliente", clientes_unicos, key="filtro_cliente")
    
        with col_filtro2:
            # Filtro por serviço
            if 'servico' in st.session_state.dados_editados.columns:
                servicos_unicos = ["Todos"] + sorted(st.session_state.dados_editados['servico'].unique().tolist())
                servico_filtro = st.selectbox("🔍 Filtrar por Serviço", servicos_unicos, key="filtro_servico")
            else:
                servico_filtro = "Todos"
    
        with col_filtro3:
            # Botão para limpar filtros
            if st.button("🔄 Limpar Filtros", width='stretch'):
                st.rerun()
    
        # Aplicar filtros
        df_filtrado = st.session_state.dados_editados.copy()
    
        if cliente_filtro != "Todos":
            df_filtrado = df_filtrado[df_filtrado['cliente'] == cliente_filtro]
    
        if servico_filtro != "Todos":
            df_filtrado = df_filtrado[df_filtrado['servico'] == servico_filtro]
    
        st.info(f"**{len(df_filtrado)}** de **{len(st.session_state.dados_editados)}** registros exibidos")
    
        # Planilha sempre visível com filtros aplicados
        st.dataframe(
            df_filtrado,
            width='stretch',
            height=400
        )

    # =======================

    st.markdown("---")

# =======================
# ABA 2: WAIVERS
# =======================
# TAB 2: WAIVERS
# =======================

with tab2:
    
    st.header("💰 Gestão de Waivers")
    st.markdown("---")
    
    # Carregar lista de fundos do BigQuery
    @st.cache_data(ttl=3600)
    def carregar_fundos_disponiveis():
        """Carrega lista de fundos disponíveis - apenas nomes"""
        try:
            client = get_bigquery_client()
            query = """
            SELECT DISTINCT name as fund_name
            FROM `kanastra-live.hub.funds` 
            WHERE name IS NOT NULL 
            ORDER BY name
            """
            df = client.query(query).to_dataframe()
            return df['fund_name'].tolist()
        except Exception as e:
            st.error(f"❌ Erro ao carregar fundos: {e}")
            return []
    
    # Seção: Criar Novo Waiver
    st.subheader("➕ Criar Novo Waiver Progressivo")
    
    st.info("💡 **Waivers Progressivos**: Configure múltiplas fases com percentuais diferentes. Ex: Meses 1-2 = 100% waiver (não cobra), Mês 3-4 = 50% waiver (cobra metade), Mês 5+ = 0% (cobra full)")
    
    # Carregar fundos FORA do formulário
    fundos_disponiveis = carregar_fundos_disponiveis()
    
    # Serviços disponíveis
    SERVICOS_DISPONIVEIS = ["Administração", "Gestão", "Custódia", "Agente Monitoramento", "Performance"]
    
    # Seleção de fundos e serviços FORA do formulário
    col_select1, col_select2 = st.columns(2)
    
    with col_select1:
        fundos_selecionados = st.multiselect(
            "🏢 Selecione os fundos:",
            fundos_disponiveis,
            help="Escolha um ou mais fundos",
            key="fundos_waiver_select"
        )
    
    with col_select2:
        servicos_selecionados = st.multiselect(
            "🔧 Selecione os serviços:",
            SERVICOS_DISPONIVEIS,
            help="Deixe vazio para aplicar em TODOS os serviços",
            key="servicos_waiver_select"
        )
    
    if not servicos_selecionados:
        st.caption("ℹ️ Waiver será aplicado em **TODOS** os serviços")
    else:
        st.caption(f"✅ Waiver será aplicado apenas em: **{', '.join(servicos_selecionados)}**")
    
    if not fundos_selecionados:
        st.info("👆 Selecione pelo menos um fundo para configurar o waiver")
    else:
        # Inicializar número de fases no session_state
        if 'num_fases_waiver' not in st.session_state:
            st.session_state.num_fases_waiver = 1
        
        # Controles para adicionar/remover fases FORA do formulário
        col_fase1, col_fase2, col_fase3 = st.columns([2, 2, 4])
        
        with col_fase1:
            if st.button("➕ Adicionar Fase", width='stretch'):
                st.session_state.num_fases_waiver += 1
                st.rerun()
        
        with col_fase2:
            if st.button("➖ Remover Fase", width='stretch', disabled=st.session_state.num_fases_waiver <= 1):
                if st.session_state.num_fases_waiver > 1:
                    st.session_state.num_fases_waiver -= 1
                    st.rerun()
        
        with col_fase3:
            st.info(f"📊 **{st.session_state.num_fases_waiver} fase(s)** configurada(s)")
        
        # Mostrar formulário
        with st.form("form_criar_waiver"):
            st.markdown("### 📝 Configure as fases do waiver")
            st.info(f"✅ {len(fundos_selecionados)} fundo(s) × {st.session_state.num_fases_waiver} fase(s) = **{len(fundos_selecionados) * st.session_state.num_fases_waiver * (len(servicos_selecionados) if servicos_selecionados else 1)} waiver(s)** serão criados")
            
            st.markdown("---")
            
            # Configurar cada fase
            fases_config = []
            
            for fase_idx in range(st.session_state.num_fases_waiver):
                st.markdown(f"### 📋 Fase {fase_idx + 1}")
                
                col_periodo1, col_periodo2 = st.columns(2)
                
                with col_periodo1:
                    data_inicio_fase = st.date_input(
                        f"📅 Data Início:",
                        value=datetime.now().date(),
                        key=f"data_inicio_fase_{fase_idx}"
                    )
                
                with col_periodo2:
                    data_fim_fase = st.date_input(
                        f"📅 Data Fim:",
                        value=datetime.now().date(),
                        key=f"data_fim_fase_{fase_idx}"
                    )
                
                col_tipo1, col_tipo2, col_tipo3 = st.columns([3, 3, 2])
                
                with col_tipo1:
                    tipo_valor_waiver = st.radio(
                        f"💰 Tipo de Waiver:",
                        ["Percentual (%)", "Valor Fixo (R$)"],
                        horizontal=True,
                        key=f"tipo_valor_fase_{fase_idx}",
                        help="• Percentual: desconto sobre a taxa calculada\n• Valor Fixo: valor em reais"
                    )
                    
                    if tipo_valor_waiver == "Percentual (%)":
                        percentual_waiver = st.number_input(
                            f"📊 Percentual de Waiver (%):",
                            min_value=0.0,
                            max_value=100.0,
                            value=100.0 if fase_idx == 0 else 50.0,
                            step=5.0,
                            format="%.1f",
                            key=f"percentual_fase_{fase_idx}",
                            help="Ex: 100% = não cobra nada, 50% = cobra metade, 0% = cobra full"
                        )
                        valor_fixo_waiver = None
                        tipo_desconto_fase = "Percentual"
                    else:
                        valor_fixo_waiver = st.number_input(
                            f"💵 Valor Fixo (R$):",
                            min_value=0.0,
                            value=0.0,
                            step=100.0,
                            format="%.2f",
                            key=f"valor_fixo_fase_{fase_idx}",
                            help="Valor em reais que será descontado"
                        )
                        percentual_waiver = None
                        tipo_desconto_fase = "Fixo"
                
                with col_tipo2:
                    forma_aplicacao = st.selectbox(
                        f"📊 Forma de Aplicação:",
                        ["Provisionado", "Nao_Provisionado"],
                        key=f"forma_aplicacao_fase_{fase_idx}",
                        format_func=lambda x: "🔄 Provisionado (Distribuído)" if x == "Provisionado" else "📍 Não Provisionado (Último)",
                        help="• Provisionado: distribui por todos os registros\n• Não Provisionado: aplica no último registro"
                    )
                
                with col_tipo3:
                    dias_fase = (data_fim_fase - data_inicio_fase).days + 1
                    st.metric("📆 Dias", dias_fase)
                    
                    if tipo_valor_waiver == "Percentual (%)":
                        st.metric("📊 Desconto", f"{percentual_waiver}%")
                    else:
                        st.metric("💰 Valor", f"R$ {valor_fixo_waiver:,.2f}")
                
                fases_config.append({
                    "data_inicio": data_inicio_fase,
                    "data_fim": data_fim_fase,
                    "tipo_desconto": tipo_desconto_fase,
                    "percentual_waiver": percentual_waiver,
                    "valor_fixo_waiver": valor_fixo_waiver,
                    "forma_aplicacao": forma_aplicacao
                })
                
                st.divider()
            
            # Observação geral
            observacao_waiver = st.text_area(
                "📝 Observação (opcional):",
                placeholder="Ex: Waiver progressivo - redução gradual em 3 fases...",
                key="obs_waiver_fases"
            )
            
            submitted_waiver = st.form_submit_button("➕ Criar Waivers Progressivos", width='stretch', type="primary")
            
            if submitted_waiver:
                # Validações
                erros = []
                
                for idx, fase in enumerate(fases_config, 1):
                    if fase['tipo_desconto'] == "Percentual" and (fase['percentual_waiver'] is None or fase['percentual_waiver'] < 0):
                        erros.append(f"❌ Fase {idx}: Percentual inválido")
                    
                    if fase['tipo_desconto'] == "Fixo" and (fase['valor_fixo_waiver'] is None or fase['valor_fixo_waiver'] <= 0):
                        erros.append(f"❌ Fase {idx}: Valor fixo deve ser maior que zero")
                    
                    if fase['data_fim'] < fase['data_inicio']:
                        erros.append(f"❌ Fase {idx}: Data fim anterior à data início")
                
                # Verificar sobreposição de períodos
                for i, fase1 in enumerate(fases_config):
                    for j, fase2 in enumerate(fases_config):
                        if i < j:
                            # Verifica se há sobreposição
                            if not (fase1['data_fim'] < fase2['data_inicio'] or fase2['data_fim'] < fase1['data_inicio']):
                                erros.append(f"⚠️ Atenção: Fase {i+1} e Fase {j+1} têm períodos sobrepostos")
                
                if erros:
                    for erro in erros:
                        st.warning(erro) if "Atenção" in erro else st.error(erro)
                else:
                    # Criar waivers para cada combinação: fundo × fase × serviço
                    usuario_atual = st.session_state.get('usuario_logado', 'usuario_kanastra')
                    solicitacao_id = str(uuid.uuid4())  # Mesmo ID para agrupar todos
                    sucesso = True
                    total_waivers = 0
                    
                    servicos_para_criar = servicos_selecionados if servicos_selecionados else [None]
                    
                    for fundo in fundos_selecionados:
                        for servico in servicos_para_criar:
                            for idx, fase in enumerate(fases_config, 1):
                                # Calcular valor_desconto baseado no tipo
                                if fase['tipo_desconto'] == "Percentual":
                                    valor_desconto = 0.0  # Será calculado na aplicação
                                    percentual_desconto = fase['percentual_waiver']
                                else:
                                    valor_desconto = fase['valor_fixo_waiver']
                                    percentual_desconto = None
                                
                                dados_waiver = {
                                    "fund_name": fundo,
                                    "valor_waiver": valor_desconto,
                                    "tipo_waiver": fase['forma_aplicacao'],
                                    "data_inicio": fase['data_inicio'].strftime('%Y-%m-%d'),
                                    "data_fim": fase['data_fim'].strftime('%Y-%m-%d'),
                                    "servico": servico,
                                    "tipo_desconto": fase['tipo_desconto'],
                                    "percentual_desconto": percentual_desconto,
                                    "observacao": f"{observacao_waiver or 'Waiver progressivo'} - Fase {idx}/{len(fases_config)}"
                                }
                                
                                resultado, _ = salvar_alteracao_pendente("INSERT", "waiver", dados_waiver, usuario_atual, solicitacao_id)
                                if resultado:
                                    total_waivers += 1
                                else:
                                    sucesso = False
                                    break
                            
                            if not sucesso:
                                break
                        
                        if not sucesso:
                            break
                    
                    if sucesso:
                        st.success(f"✅ {total_waivers} waiver(s) criado(s) em {len(fases_config)} fase(s) e enviados para aprovação!")
                        st.info("⏳ Aguardando aprovação de um aprovador")
                        # Resetar número de fases
                        st.session_state.num_fases_waiver = 1
                        st.rerun()
                    else:
                        st.error("❌ Erro ao salvar um ou mais waivers")
    
    st.markdown("---")
    
    # Seção: Histórico de Waivers
    st.subheader("📊 Histórico de Waivers Aprovados")
    
    @st.cache_data(ttl=300)
    def carregar_historico_waivers():
        """Carrega histórico de waivers do BigQuery (tabela finance.descontos)"""
        try:
            client = get_bigquery_client()
            query = """
            SELECT 
                id,
                data_aplicacao,
                usuario,
                fund_name,
                valor_desconto as valor_waiver,
                tipo_desconto,
                percentual_desconto,
                forma_aplicacao,
                data_inicio,
                data_fim,
                servico,
                observacao
            FROM `kanastra-live.finance.descontos`
            WHERE categoria = 'waiver'
            ORDER BY data_aplicacao DESC
            LIMIT 100
            """
            df = client.query(query).to_dataframe()
            return df
        except Exception as e:
            st.warning(f"⚠️ Erro ao carregar waivers: {e}")
            return pd.DataFrame()
    
    df_waivers = carregar_historico_waivers()
    
    if not df_waivers.empty:
        # Filtros
        col_filtro1, col_filtro2 = st.columns(2)
        
        with col_filtro1:
            fundos_filtro = st.multiselect(
                "Filtrar por Fundo:",
                options=sorted(df_waivers['fund_name'].unique()),
                key="filtro_fundos_waiver"
            )
        
        with col_filtro2:
            tipo_filtro = st.selectbox(
                "Filtrar por Forma de Aplicação:",
                ["Todos", "Provisionado", "Nao_Provisionado"],
                key="filtro_tipo_waiver",
                format_func=lambda x: "Todos" if x == "Todos" else ("Provisionado (Distribuído)" if x == "Provisionado" else "Não Provisionado (Último)")
            )
        
        # Aplicar filtros
        df_filtrado = df_waivers.copy()
        
        if fundos_filtro:
            df_filtrado = df_filtrado[df_filtrado['fund_name'].isin(fundos_filtro)]
        
        if tipo_filtro != "Todos":
            df_filtrado = df_filtrado[df_filtrado['forma_aplicacao'] == tipo_filtro]
        
        st.info(f"📊 Exibindo **{len(df_filtrado)}** de **{len(df_waivers)}** waivers")
        
        # Exibir tabela
        st.dataframe(
            df_filtrado,
            width='stretch',
            height=400,
            column_config={
                "id": None,  # Ocultar ID
                "data_aplicacao": st.column_config.DatetimeColumn("Data Aplicação", format="DD/MM/YYYY HH:mm"),
                "usuario": "Usuário",
                "fund_name": "Fundo",
                "servico": "Serviço",
                "tipo_desconto": st.column_config.TextColumn("Tipo", help="Fixo ou Percentual"),
                "valor_waiver": st.column_config.NumberColumn("Valor Base", format="R$ %.2f"),
                "percentual_desconto": st.column_config.NumberColumn("Percentual", format="%.1f%%"),
                "forma_aplicacao": st.column_config.TextColumn("Forma Aplicação", help="Provisionado = Distribuído, Nao_Provisionado = Último registro"),
                "data_inicio": st.column_config.DateColumn("📅 Início Vigência", format="DD/MM/YYYY"),
                "data_fim": st.column_config.DateColumn("📅 Fim Vigência", format="DD/MM/YYYY"),
                "observacao": "Observação"
            }
        )
    else:
        st.info("📝 Nenhum waiver aprovado ainda.")

# =======================
# ABA 3: DESCONTOS
# =======================
# TAB 3: DESCONTOS
# =======================

with tab3:
    
    st.header("🎯 Gestão de Descontos")
    st.markdown("---")
    
    # Serviços disponíveis
    SERVICOS_DISPONIVEIS = ["Administração", "Gestão", "Custódia", "Agente Monitoramento", "Performance"]
    
    # Seção: Criar Novo Desconto
    st.subheader("➕ Criar Novo Desconto")
    
    # Carregar fundos completos (ID + Nome + CNPJ)
    fundos_completos = carregar_fundos_completos()
    
    if fundos_completos.empty:
        st.warning("⚠️ Nenhum fundo disponível no sistema")
    else:
        # Criar opções para o selectbox
        opcoes_fundos = [f"{row['fund_id']} - {row['fund_name']} ({row['cnpj']})" 
                        for _, row in fundos_completos.iterrows()]
        
        # Seleção de fundo FORA do formulário
        fundo_selecionado = st.selectbox(
            "🏢 Selecione o fundo:",
            [""] + opcoes_fundos,
            key="fundo_desconto_select",
            help="Escolha o fundo que receberá o desconto"
        )
        
        if not fundo_selecionado:
            st.info("👆 Selecione um fundo para configurar o desconto")
        else:
            # Extrair fund_id da seleção
            fund_id_selecionado = int(fundo_selecionado.split(" - ")[0])
            fund_name_selecionado = fundo_selecionado.split(" - ")[1].split(" (")[0]
            
            # Mostrar formulário
            with st.form("form_criar_desconto"):
                st.markdown("### 📝 Configure o desconto")
                st.info(f"✅ Fundo selecionado: **{fund_name_selecionado}** (ID: {fund_id_selecionado})")
                
                st.markdown("---")
                
                # Origem do desconto
                col_origem1, col_origem2 = st.columns(2)
                
                with col_origem1:
                    origem_desconto = st.selectbox(
                        "📋 Origem do Desconto:",
                        ["comercial", "juridico"],
                        format_func=lambda x: "🤝 Comercial (Acordo Comercial)" if x == "comercial" else "⚖️ Jurídico (Ordem Judicial)",
                        help="• Comercial: Negociações e acordos comerciais\n• Jurídico: Ordens judiciais e decisões obrigatórias"
                    )
                
                with col_origem2:
                    documento_referencia = st.text_input(
                        "📄 Documento de Referência:",
                        placeholder="Nº do processo, contrato, etc.",
                        help="Número do processo judicial, contrato ou documento que originou o desconto"
                    )
                
                st.markdown("---")
                
                # Tipo de desconto (3 opções)
                tipo_desconto_opcao = st.radio(
                    "💰 Tipo de Desconto:",
                    ["Valor Fixo (R$)", "Percentual (%)", "Total (Zera taxa)"],
                    horizontal=True,
                    help="• Valor Fixo: desconto de valor específico em R$\n• Percentual: desconto parcial (% sobre a taxa)\n• Total: zera completamente a taxa (100% de desconto)"
                )
                
                # Determinar tipo_desconto e valores
                if "Valor Fixo" in tipo_desconto_opcao:
                    tipo_desconto = "Fixo"
                elif "Percentual" in tipo_desconto_opcao:
                    tipo_desconto = "Percentual"
                else:  # Total
                    tipo_desconto = "Percentual"
                
                col_valor1, col_valor2 = st.columns(2)
                
                with col_valor1:
                    if tipo_desconto == "Fixo":
                        valor_desconto = st.number_input(
                            "💵 Valor do Desconto (R$):",
                            min_value=0.0,
                            value=0.0,
                            step=100.0,
                            format="%.2f",
                            help="Valor fixo em reais que será deduzido da taxa"
                        )
                        percentual_desconto = None
                    elif "Percentual" in tipo_desconto_opcao:
                        percentual_desconto = st.number_input(
                            "📊 Percentual de Desconto (%):",
                            min_value=0.0,
                            max_value=100.0,
                            value=0.0,
                            step=1.0,
                            format="%.2f",
                            help="Percentual que será aplicado sobre a taxa calculada"
                        )
                        valor_desconto = 0.0
                    else:  # Total - zera a taxa
                        percentual_desconto = 100.0
                        valor_desconto = 0.0
                        st.info("💯 **Desconto Total**: A taxa será zerada completamente (100% de desconto)")
                
                with col_valor2:
                    forma_aplicacao = st.selectbox(
                        "📊 Forma de Aplicação:",
                        ["Provisionado", "Nao_Provisionado"],
                        format_func=lambda x: "🔄 Provisionado (Distribuído)" if x == "Provisionado" else "📍 Não Provisionado (Último Registro)",
                        help="• Provisionado: distribui o desconto proporcionalmente por todos os registros do período\n• Não Provisionado: aplica o desconto total no último registro do período"
                    )
                
                st.markdown("---")
                
                # Serviços (múltipla seleção)
                st.markdown("### 🔧 Serviços")
                st.caption("Selecione os serviços nos quais o desconto será aplicado. Se nenhum for selecionado, o desconto será aplicado em TODOS os serviços.")
                
                servicos_selecionados = st.multiselect(
                    "Selecione os serviços:",
                    SERVICOS_DISPONIVEIS,
                    help="Deixe vazio para aplicar em todos os serviços do fundo"
                )
                
                if not servicos_selecionados:
                    st.info("ℹ️ Desconto será aplicado em **TODOS** os serviços do fundo")
                else:
                    st.success(f"✅ Desconto será aplicado apenas em: **{', '.join(servicos_selecionados)}**")
                
                st.markdown("---")
                
                # Período de vigência
                st.markdown("### 📅 Período de Aplicação")
                col_data1, col_data2, col_data3 = st.columns([3, 3, 2])
                
                with col_data1:
                    data_inicio_desconto = st.date_input(
                        "Data Início:",
                        value=datetime.now().date(),
                        key="data_inicio_desconto"
                    )
                
                with col_data2:
                    vigencia_indefinida_desc = st.checkbox(
                        "⏰ Vigência indefinida",
                        value=False,
                        help="Marque se o desconto não tem data de término"
                    )
                    
                    if not vigencia_indefinida_desc:
                        data_fim_desconto = st.date_input(
                            "Data Fim:",
                            value=datetime.now().date(),
                            key="data_fim_desconto"
                        )
                    else:
                        data_fim_desconto = None
                        st.info("⏰ Desconto sem data de término")
                
                with col_data3:
                    if data_fim_desconto:
                        dias = (data_fim_desconto - data_inicio_desconto).days + 1
                        st.metric("📆 Dias", dias)
                    else:
                        st.metric("📆 Dias", "Indefinido")
                
                # Observação
                observacao_desconto = st.text_area(
                    "📝 Observação:",
                    placeholder="Digite informações adicionais sobre este desconto (motivo, justificativa, contexto)...",
                    key="obs_desconto",
                    height=100
                )
                
                st.markdown("---")
                
                # Resumo antes de enviar
                st.markdown("### 📋 Resumo do Desconto")
                col_res1, col_res2, col_res3 = st.columns(3)
                
                with col_res1:
                    st.metric("🏢 Fundo", fund_name_selecionado)
                    st.caption(f"ID: {fund_id_selecionado}")
                
                with col_res2:
                    if tipo_desconto == "Fixo":
                        st.metric("💰 Valor", f"R$ {valor_desconto:,.2f}")
                        st.caption("Desconto Total")
                    else:
                        st.metric("📊 Desconto", f"{percentual_desconto}%")
                        st.caption("Desconto Parcial")
                
                with col_res3:
                    origem_label = "🤝 Comercial" if origem_desconto == "comercial" else "⚖️ Jurídico"
                    st.metric("📋 Origem", origem_label)
                    st.caption(forma_aplicacao.replace("_", " "))
                
                submitted_desconto = st.form_submit_button(
                    "➕ Criar Desconto", 
                    width='stretch', 
                    type="primary"
                )
                
                if submitted_desconto:
                    # Validações
                    erros = []
                    
                    if tipo_desconto == "Fixo" and valor_desconto <= 0:
                        erros.append("❌ Valor do desconto deve ser maior que zero")
                    
                    if tipo_desconto == "Percentual" and percentual_desconto <= 0:
                        erros.append("❌ Percentual de desconto deve ser maior que zero")
                    
                    if not documento_referencia:
                        erros.append("❌ Documento de referência é obrigatório")
                    
                    if data_fim_desconto and data_fim_desconto < data_inicio_desconto:
                        erros.append("❌ Data fim não pode ser anterior à data início")
                    
                    if erros:
                        for erro in erros:
                            st.error(erro)
                    else:
                        # Criar solicitação de desconto para cada serviço selecionado
                        # Se nenhum serviço foi selecionado, cria UMA solicitação com servico=NULL
                        usuario_atual = st.session_state.get('usuario_logado', 'usuario_kanastra')
                        solicitacao_id = str(uuid.uuid4())
                        sucesso = True
                        
                        servicos_para_criar = servicos_selecionados if servicos_selecionados else [None]
                        
                        for servico in servicos_para_criar:
                            dados_desconto = {
                                "fund_id": fund_id_selecionado,
                                "fund_name": fund_name_selecionado,
                                "valor_desconto": valor_desconto if tipo_desconto == "Fixo" else 0.0,
                                "tipo_desconto": tipo_desconto,
                                "percentual_desconto": percentual_desconto if tipo_desconto == "Percentual" else None,
                                "forma_aplicacao": forma_aplicacao,
                                "data_inicio": data_inicio_desconto.strftime('%Y-%m-%d'),
                                "data_fim": data_fim_desconto.strftime('%Y-%m-%d') if data_fim_desconto else None,
                                "servico": servico,
                                "observacao": observacao_desconto or f"Desconto {origem_desconto} criado via Dashboard",
                                "documento_referencia": documento_referencia
                            }
                            
                            resultado, _ = salvar_alteracao_pendente(
                                "INSERT", 
                                "desconto", 
                                dados_desconto, 
                                usuario_atual, 
                                solicitacao_id,
                                tipo_categoria="desconto",
                                origem=origem_desconto
                            )
                            
                            if not resultado:
                                sucesso = False
                                break
                        
                        if sucesso:
                            qtd_servicos = len(servicos_para_criar)
                            if servicos_selecionados:
                                st.success(f"✅ Desconto criado para {qtd_servicos} serviço(s) e enviado para aprovação!")
                                st.info(f"📋 Serviços: {', '.join(servicos_selecionados)}")
                            else:
                                st.success(f"✅ Desconto criado para TODOS os serviços e enviado para aprovação!")
                            
                            st.info("⏳ Aguardando aprovação de um aprovador")
                            st.rerun()
                        else:
                            st.error("❌ Erro ao salvar desconto")
    
    st.markdown("---")
    
    # Seção: Histórico de Descontos (futuro)
    st.subheader("📊 Histórico de Descontos")
    st.info("🚧 Em breve: visualização de descontos aprovados e ativos")

st.markdown("---")

# =======================
# PAINEL DE APROVAÇÃO - COMUM A TODAS AS ABAS
# =======================

# PAINEL DE APROVAÇÃO (apenas para aprovadores)
if perfil == "aprovador":
    st.subheader("👑 Painel de Aprovação")
else:
    st.subheader("📊 Suas Alterações Pendentes")

# Carregar alterações pendentes do BigQuery (agrupadas por solicitacao_id)
solicitacoes_pendentes = carregar_alteracoes_pendentes()

# Filtrar solicitações conforme perfil
if perfil == "editor":
    # Editores veem apenas suas próprias solicitações
    solicitacoes_filtradas = [s for s in solicitacoes_pendentes if s[0].get('usuario') == st.session_state.usuario_logado]
else:
    # Aprovadores veem todas as solicitações
    solicitacoes_filtradas = solicitacoes_pendentes

if solicitacoes_filtradas:
    st.markdown("---")
    
    total_solicitacoes = len(solicitacoes_filtradas)
    total_linhas = sum(len(s) for s in solicitacoes_filtradas)
    
    if perfil == "aprovador":
        st.subheader(f"⏳ Solicitações Pendentes: {total_solicitacoes} ({total_linhas} linhas)")
    else:
        st.subheader(f"⏳ Suas Solicitações Pendentes: {total_solicitacoes} ({total_linhas} linhas)")
    
    # Processar cada solicitação (grupo de alterações)
    for idx, solicitacao in enumerate(solicitacoes_filtradas):
        # Primeira linha da solicitação tem os dados gerais
        primeira_linha = solicitacao[0]
        usuario_alteracao = primeira_linha.get('usuario', 'N/A')
        timestamp = primeira_linha['timestamp']
        tipo_alteracao = primeira_linha['tipo_alteracao']
        tabela = primeira_linha['tabela']
        
        # Cor de fundo diferente se for solicitação de outro usuário (para aprovadores)
        if perfil == "aprovador" and usuario_alteracao != st.session_state.usuario_logado:
            st.markdown(f"""
            <div style='background-color: #fffbea; padding: 15px; border-radius: 8px; border-left: 4px solid #f59e0b; margin-bottom: 15px;'>
                <strong>📦 Solicitação #{idx + 1}</strong> - <em>Por: {usuario_alteracao}</em> - <em>{len(solicitacao)} linha(s)</em>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"### 📦 Solicitação #{idx + 1} - {len(solicitacao)} linha(s)")
        
        # Exibir informações gerais da solicitação
        col_info1, col_info2, col_info3, col_info4 = st.columns(4)
        with col_info1:
            st.info(f"**Tipo:** {tipo_alteracao}")
        with col_info2:
            st.info(f"**Data/Hora:** {timestamp.strftime('%d/%m/%Y %H:%M')}")
        with col_info3:
            st.info(f"**Tabela:** {tabela}")
        with col_info4:
            st.info(f"**Linhas:** {len(solicitacao)}")
        
        # Mostrar todas as linhas da solicitação como tabela expandida
        with st.expander(f"📋 Ver {len(solicitacao)} linha(s) desta solicitação", expanded=True):
            # Criar DataFrame com todas as linhas
            dados_todas_linhas = [alteracao['dados'] for alteracao in solicitacao]
            df_solicitacao = pd.DataFrame(dados_todas_linhas)
            st.dataframe(df_solicitacao, width='stretch', hide_index=True)
        
        # Botões de aprovação/rejeição EM BLOCO (APENAS PARA APROVADORES)
        if perfil == "aprovador":
            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                if st.button(f"✅ Aprovar Solicitação Completa", key=f"aprovar_solicitacao_{idx}", width='stretch', type="primary"):
                    # Executar todas as alterações da solicitação
                    try:
                        client = get_bigquery_client()
                        erros = []
                        queries_executadas = []
                        
                        # Processar cada linha da solicitação
                        for alteracao in solicitacao:
                            tabela_alt = alteracao['tabela']
                            dados = alteracao['dados']
                            tipo_alt = alteracao['tipo_alteracao']
                            
                            try:
                                # WAIVER - Insere em finance.descontos com categoria='waiver'
                                if tabela_alt == "waiver":
                                    waiver_id = str(uuid.uuid4())
                                    data_aplicacao = datetime.now().isoformat()
                                    usuario_criador = alteracao.get('usuario', 'usuario_kanastra')
                                    
                                    # tipo_waiver vem como 'Provisionado' ou 'Nao_Provisionado'
                                    forma_aplicacao = dados['tipo_waiver']
                                    
                                    # Verificar se tem tipo_desconto (waivers novos) ou usar padrão (waivers antigos)
                                    tipo_desconto = dados.get('tipo_desconto', 'Fixo')
                                    percentual_desconto = dados.get('percentual_desconto')
                                    valor_desconto = dados.get('valor_waiver', 0.0)
                                    servico = dados.get('servico')
                                    
                                    sql = f"""
                                    INSERT INTO `kanastra-live.finance.descontos` 
                                    (id, data_aplicacao, usuario, fund_id, fund_name, categoria,
                                     valor_desconto, tipo_desconto, percentual_desconto, forma_aplicacao, origem,
                                     data_inicio, data_fim, servico, observacao, documento_referencia)
                                    VALUES (
                                        '{waiver_id}',
                                        TIMESTAMP('{data_aplicacao}'),
                                        '{usuario_criador}',
                                        NULL,
                                        '{dados['fund_name']}',
                                        'waiver',
                                        {valor_desconto},
                                        '{tipo_desconto}',
                                        {percentual_desconto if percentual_desconto is not None else 'NULL'},
                                        '{forma_aplicacao}',
                                        NULL,
                                        DATE('{dados['data_inicio']}'),
                                        DATE('{dados['data_fim']}'),
                                        {f"'{servico}'" if servico else 'NULL'},
                                        '{dados.get('observacao', 'Aprovado via Dashboard')}',
                                        NULL
                                    )
                                    """
                                
                                # DESCONTO - Inserir na tabela descontos com categoria baseada na origem
                                elif tabela_alt == "desconto":
                                    desconto_id = str(uuid.uuid4())
                                    data_aplicacao = datetime.now().isoformat()
                                    usuario_aprovador = st.session_state.usuario_logado
                                    
                                    # Obter origem da alteração (juridico ou comercial)
                                    origem_desconto = alteracao.get('origem', 'comercial')
                                    categoria_desconto = f'desconto_{origem_desconto}'  # 'desconto_juridico' ou 'desconto_comercial'
                                    
                                    # Forma de aplicação: Provisionado ou Nao_Provisionado
                                    forma_aplicacao = dados.get('forma_aplicacao', 'Nao_Provisionado')
                                    
                                    sql = f"""
                                    INSERT INTO `kanastra-live.finance.descontos` 
                                    (id, data_aplicacao, usuario, fund_id, fund_name, categoria,
                                     valor_desconto, tipo_desconto, percentual_desconto, forma_aplicacao, origem,
                                     data_inicio, data_fim, servico, observacao, documento_referencia)
                                    VALUES (
                                        '{desconto_id}',
                                        TIMESTAMP('{data_aplicacao}'),
                                        '{usuario_aprovador}',
                                        {dados.get('fund_id', 0)},
                                        '{dados.get('fund_name', '')}',
                                        '{categoria_desconto}',
                                        {dados.get('valor_desconto', 0)},
                                        '{dados.get('tipo_desconto', 'Fixo')}',
                                        {dados.get('percentual_desconto') if dados.get('percentual_desconto') else 'NULL'},
                                        '{forma_aplicacao}',
                                        '{origem_desconto}',
                                        DATE('{dados['data_inicio']}'),
                                        {"DATE('" + dados['data_fim'] + "')" if dados.get('data_fim') else 'NULL'},
                                        {f"'{dados['servico']}'" if dados.get('servico') else 'NULL'},
                                        '{dados.get('observacao', 'Aprovado via Dashboard')}',
                                        '{dados.get('documento_referencia', '')}'
                                    )
                                    """
                                
                                elif tipo_alt == "INSERT":
                                    # Gerar SQL INSERT para taxas
                                    colunas = [k for k in dados.keys()]
                                    valores = []
                                    for k in colunas:
                                        v = dados[k]
                                        if v is None:
                                            valores.append("NULL")
                                        elif isinstance(v, str):
                                            valores.append(f"'{v}'")
                                        elif isinstance(v, (int, float)):
                                            valores.append(str(v))
                                        else:
                                            valores.append(f"'{str(v)}'")
                                    
                                    # Mapear fund_id para `fund id` com backticks
                                    colunas_sql = [f"`fund id`" if c == "fund_id" else c for c in colunas]
                                    
                                    sql = f"""
                                    INSERT INTO `kanastra-live.finance.{tabela_alt}` 
                                    ({', '.join(colunas_sql)})
                                    VALUES ({', '.join(valores)})
                                    """
                                    
                                else:  # UPDATE para taxas
                                    # Gerar SQL UPDATE
                                    set_clause = []
                                    for k, v in dados.items():
                                        if k not in ['fund_id', 'cliente', 'servico', 'empresa', 'original_faixa', 'original_lower']:
                                            if v is None:
                                                set_clause.append(f"{k} = NULL")
                                            elif isinstance(v, str):
                                                set_clause.append(f"{k} = '{v}'")
                                            elif isinstance(v, (int, float)):
                                                set_clause.append(f"{k} = {v}")
                                            else:
                                                set_clause.append(f"{k} = '{str(v)}'")
                                    
                                    # WHERE clause baseado na tabela
                                    if tabela_alt == "fee_minimo":
                                        where = f"`fund id` = {dados['fund_id']} AND servico = '{dados['servico']}' AND faixa = {dados.get('original_lower', dados['faixa'])}"
                                    else:  # fee_variavel
                                        original_faixa = dados.get('original_faixa', dados.get('original_lower', dados['faixa']))
                                        where = f"`fund id` = {dados['fund_id']} AND servico = '{dados['servico']}' AND faixa = {original_faixa}"
                                    
                                    sql = f"""
                                    UPDATE `kanastra-live.finance.{tabela_alt}`
                                    SET {', '.join(set_clause)}
                                    WHERE {where}
                                    """
                                
                                # Executar query
                                queries_executadas.append(sql)
                                query_job = client.query(sql)
                                query_job.result()
                                
                            except Exception as e:
                                erros.append(f"Erro em uma das linhas: {str(e)}")
                        
                        # Se todas as queries foram executadas com sucesso
                        if not erros:
                            # Mostrar queries executadas
                            with st.expander("📜 Ver SQL executado"):
                                for q in queries_executadas:
                                    st.code(q, language="sql")
                            
                            # Limpar cache se for waiver
                            if tabela == "waiver":
                                st.cache_data.clear()
                            
                            # Atualizar status de TODAS as linhas da solicitação como APROVADO
                            aprovador = st.session_state.usuario_logado
                            sucesso_atualizacao = True
                            
                            for alteracao in solicitacao:
                                # Atualizar status
                                if not atualizar_status_alteracao(alteracao['id'], 'APROVADO', aprovador):
                                    sucesso_atualizacao = False
                            
                            if sucesso_atualizacao:
                                if tabela == "waiver":
                                    st.success(f"✅ Solicitação completa aprovada! {len(solicitacao)} waiver(s) registrado(s)!")
                                else:
                                    st.success(f"✅ Solicitação completa aprovada! {len(solicitacao)} linha(s) aplicada(s)!")
                                st.rerun()
                            else:
                                st.warning("⚠️ Alterações aplicadas mas houve erro ao atualizar status")
                        else:
                            st.error("❌ Erros ao processar solicitação:")
                            for erro in erros:
                                st.error(erro)
                        
                    except Exception as e:
                        st.error(f"❌ Erro geral ao processar solicitação: {str(e)}")
            
            with col_btn2:
                if st.button(f"❌ Rejeitar Solicitação Completa", key=f"rejeitar_solicitacao_{idx}", width='stretch'):
                    aprovador = st.session_state.usuario_logado
                    sucesso_rejeicao = True
                    for alteracao in solicitacao:
                        if not atualizar_status_alteracao(alteracao['id'], 'REJEITADO', aprovador):
                            sucesso_rejeicao = False
                    
                    if sucesso_rejeicao:
                        st.warning(f"⚠️ Solicitação completa rejeitada por {aprovador}! {len(solicitacao)} linha(s) descartada(s).")
                        st.rerun()
                    else:
                        st.error("❌ Erro ao rejeitar solicitação")
        else:
            # Editores apenas visualizam, não podem aprovar
            st.info("⏳ Aguardando aprovação de um aprovador")
        
        st.markdown("---")
else:
    # Mensagem quando não há solicitações
    if perfil == "aprovador":
        st.info("✅ Não há solicitações pendentes de aprovação no momento")
    else:
        st.info("📝 Você ainda não criou nenhuma solicitação pendente")

# HISTÓRICO DE ALTERAÇÕES APROVADAS (visível para aprovadores)
if perfil == "aprovador":
    st.markdown("---")
    st.subheader("📜 Histórico de Alterações Aprovadas (Waivers e Descontos)")
    
    # Carregar histórico
    df_historico = carregar_historico_alteracoes(limit=50)
    
    if not df_historico.empty:
        # Preparar dados para exibição
        df_exibir = df_historico.copy()
        
        # Processar dados para criar resumo
        dados_processados = []
        for _, row in df_exibir.iterrows():
            try:
                # Identificar fundo
                fundo = row.get('fund_name') or f"ID {row.get('fund_id', 'N/A')}"
                
                # Criar resumo baseado no tipo
                if row.get('tipo_desconto') == 'Percentual':
                    valor_info = f"{row.get('percentual_desconto', 0):.2f}%"
                else:
                    valor_info = f"R$ {row.get('valor_desconto', 0):,.2f}"
                
                forma = row.get('forma_aplicacao', 'N/A')
                servico = row.get('servico', 'Todos os serviços')
                periodo = f"{row['data_inicio'].strftime('%d/%m/%Y')} até "
                if pd.notnull(row.get('data_fim')):
                    periodo += row['data_fim'].strftime('%d/%m/%Y')
                else:
                    periodo += "vigência indefinida"
                
                resumo = f"Fundo: {fundo} | Valor: {valor_info} | {forma} | Serviço: {servico} | {periodo}"
                dados_processados.append(resumo)
            except Exception as e:
                dados_processados.append(f"Erro ao processar: {str(e)}")
        
        df_exibir['Detalhes'] = dados_processados
        
        # Traduzir categoria
        categoria_map = {
            'waiver': '💰 Waiver',
            'desconto_juridico': '⚖️ Desconto Jurídico',
            'desconto_comercial': '🤝 Desconto Comercial'
        }
        df_exibir['Tipo'] = df_exibir['categoria'].map(categoria_map).fillna(df_exibir['categoria'])
        
        # Selecionar e renomear colunas para exibição
        df_final = df_exibir[['data_aprovacao', 'aprovador_por', 'Tipo', 'Detalhes', 'observacao']].copy()
        df_final.columns = ['Data Aplicação', 'Usuário', 'Tipo', 'Detalhes', 'Observação']
        
        # Exibir tabela
        st.dataframe(
            df_final,
            width='stretch',
            height=400,
            hide_index=True,
            column_config={
                'Data Aplicação': st.column_config.DatetimeColumn(
                    'Data Aplicação',
                    format="DD/MM/YYYY HH:mm:ss"
                ),
                'Usuário': st.column_config.TextColumn('Usuário', width="small"),
                'Tipo': st.column_config.TextColumn('Tipo', width="medium"),
                'Detalhes': st.column_config.TextColumn('Detalhes', width="large"),
                'Observação': st.column_config.TextColumn('Observação', width="medium")
            }
        )
        
        st.caption(f"📊 Exibindo últimas 50 alterações aprovadas (waivers e descontos ativos)")
    else:
        st.info("ℹ️ Nenhuma alteração aprovada no histórico ainda")
