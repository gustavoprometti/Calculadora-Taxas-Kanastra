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

# CSS customizado com fontes e identidade visual Kanastra
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');
    
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
    
    /* Títulos com fonte Inter Display Medium */
    h1, h2, h3 {
        font-family: 'Inter', sans-serif;
        font-weight: 500;
        color: #193c32;
    }
    
    /* Botões com cores Kanastra */
    .stButton>button {
        background-color: #2daa82;
        color: white;
        font-weight: 500;
        border-radius: 8px;
        border: none;
        transition: all 0.3s;
    }
    
    .stButton>button:hover {
        background-color: #14735a;
        box-shadow: 0 4px 12px rgba(45, 170, 130, 0.3);
    }
    
    /* Metrics com destaque */
    [data-testid="stMetricValue"] {
        color: #193c32;
        font-weight: 600;
    }
    
    /* Sidebar com logo */
    [data-testid="stSidebar"] {
        background-color: #f3f2f3;
    }
    
    /* Divisores com cor Kanastra */
    hr {
        border-color: #2daa82;
    }
    
    /* DataFrames */
    .dataframe {
        border-radius: 8px !important;
    }
    
    /* Alertas */
    .stAlert {
        border-radius: 8px;
        border-left: 4px solid #2daa82;
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
            SELECT *
            FROM `kanastra-live.finance.fee_variavel`
            ORDER BY `fund id`
            """
        
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"❌ Erro ao carregar dados: {e}")
        return None

# Funções para persistência de alterações pendentes
def salvar_alteracao_pendente(tipo_alteracao, tabela, dados, usuario="usuario_kanastra"):
    """Salva uma alteração pendente no BigQuery"""
    client = get_bigquery_client()
    if client is None:
        st.error("❌ Erro ao conectar com BigQuery")
        return False
    
    try:
        alteracao_id = str(uuid.uuid4())
        timestamp_now = datetime.now().isoformat()
        
        # Converter dados para JSON string
        dados_json = json.dumps(dados, ensure_ascii=False)
        
        query = f"""
        INSERT INTO `kanastra-live.finance.alteracoes_pendentes` 
        (id, usuario, timestamp, tipo_alteracao, tabela, dados, status)
        VALUES (
            '{alteracao_id}',
            '{usuario}',
            TIMESTAMP('{timestamp_now}'),
            '{tipo_alteracao}',
            '{tabela}',
            JSON '{dados_json}',
            'PENDENTE'
        )
        """
        
        client.query(query).result()
        return True
    except Exception as e:
        st.error(f"❌ Erro ao salvar alteração: {e}")
        return False

def carregar_alteracoes_pendentes():
    """Carrega todas as alterações pendentes do BigQuery"""
    client = get_bigquery_client()
    if client is None:
        return []
    
    try:
        query = """
        SELECT 
            id,
            usuario,
            timestamp,
            tipo_alteracao,
            tabela,
            dados,
            status
        FROM `kanastra-live.finance.alteracoes_pendentes`
        WHERE status = 'PENDENTE'
        ORDER BY timestamp ASC
        """
        
        df = client.query(query).to_dataframe()
        
        # Converter para lista de dicionários
        alteracoes = []
        for _, row in df.iterrows():
            alteracao = {
                'id': row['id'],
                'usuario': row['usuario'],
                'timestamp': row['timestamp'],
                'tipo_alteracao': row['tipo_alteracao'],
                'tabela': row['tabela'],
                'dados': json.loads(row['dados']),
                'status': row['status']
            }
            alteracoes.append(alteracao)
        
        return alteracoes
    except Exception as e:
        st.error(f"❌ Erro ao carregar alterações pendentes: {e}")
        return []

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
        if st.button("🔓 Entrar", use_container_width=True, type="primary"):
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
    if st.button("🚪 Sair", use_container_width=True, type="secondary"):
        st.session_state.usuario_logado = None
        st.session_state.perfil_usuario = None
        st.session_state.usuario_aprovador = None
        st.rerun()

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
    if st.button("📊 Carregar Dados", use_container_width=True, type="primary"):
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
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                fund_id = st.number_input("Fund ID", min_value=1, step=1)
            
            with col2:
                cliente = st.text_input("Cliente")
            
            with col3:
                servico = st.selectbox(
                    "Serviço",
                    ["Administração", "Gestão", "Custódia", "Agente Monitoramento", "Performance"]
                )
            
            fee_min = st.number_input("Fee Mínimo (R$)", min_value=0.0, step=100.0, format="%.2f")
            
            submitted = st.form_submit_button("➕ Criar Taxa Mínima", use_container_width=True, type="primary")
            
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
                if salvar_alteracao_pendente("INSERT", "fee_minimo", taxa_faixa_0, usuario_atual):
                    if salvar_alteracao_pendente("INSERT", "fee_minimo", taxa_faixa_max, usuario_atual):
                        st.success(f"✅ Taxa mínima criada! Cliente: {cliente} - {servico} - 2 linhas adicionadas (faixa 0 e máxima)")
                        st.info("⏳ Aguardando aprovação de um aprovador")
                        st.rerun()
                    else:
                        st.error("❌ Erro ao salvar segunda linha")
                else:
                    st.error("❌ Erro ao salvar primeira linha")
    
    
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
            
            submitted_edit = st.form_submit_button("💾 Salvar Novo Valor", use_container_width=True, type="primary")
            
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
                    if salvar_alteracao_pendente("UPDATE", "fee_minimo", taxa_editada, usuario_atual):
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
        
        # Mapeamento de serviços em português para inglês (para o banco)
        servicos_map = {
            "Administração": "administration",
            "Gestão": "management",
            "Performance": "performance",
            "Custódia": "custody"
        }
        
        with st.form("form_criar_taxa_variavel"):
            st.markdown("### 📝 Informações básicas")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                fund_id_var = st.number_input("Fund ID", min_value=1, step=1, key="var_fund_id")
            
            with col2:
                cliente_var = st.text_input("Cliente", key="var_cliente")
            
            with col3:
                service_type_pt = st.selectbox(
                    "Tipo de Serviço",
                    ["Administração", "Gestão", "Performance", "Custódia"],
                    key="var_service"
                )
            
            st.markdown("---")
            st.markdown("### 📊 Faixas de PL e Taxas (Cascata)")
            st.info("ℹ️ Será criada 1 linha no BigQuery para cada faixa. Ex: 3 faixas = 3 linhas no banco de dados")
            
            # Número de faixas
            num_faixas = st.number_input("Quantas faixas deseja criar?", min_value=1, max_value=10, value=2, step=1)
            
            faixas_data = []
            
            for i in range(num_faixas):
                st.markdown(f"**Faixa {i+1}:**")
                col_a, col_b, col_c = st.columns(3)
                
                with col_a:
                    lower = st.number_input(
                        f"PL Inicial (R$)", 
                        min_value=0.0, 
                        step=1000000.0, 
                        format="%.0f",
                        key=f"lower_{i}"
                    )
                
                with col_b:
                    # Se for última faixa, sugerir valor máximo
                    default_upper = 1000000000000000.0 if i == num_faixas - 1 else 0.0
                    upper = st.number_input(
                        f"PL Final (R$)", 
                        min_value=0.0, 
                        step=1000000.0, 
                        format="%.0f",
                        value=default_upper,
                        key=f"upper_{i}",
                        help="Use 1000000000000000 para valor ilimitado"
                    )
                
                with col_c:
                    fee_pct = st.number_input(
                        f"Taxa (%)", 
                        min_value=0.0, 
                        max_value=100.0, 
                        step=0.0001, 
                        format="%.4f",
                        key=f"fee_{i}"
                    )
                
                faixas_data.append({
                    "lower_bound": lower,
                    "upper_bound": upper,
                    "fee_percentage": fee_pct
                })
            
            submitted_var = st.form_submit_button("➕ Criar Taxas Variáveis", use_container_width=True, type="primary")
            
            if submitted_var:
                # Converter serviço para inglês
                service_type_en = servicos_map[service_type_pt]
                
                linhas_criadas = []
                
                # 1. Criar linha com limite superior máximo (primeira linha)
                ultima_faixa = faixas_data[-1]
                linha_max = {
                    "fund_id": fund_id_var,
                    "cliente": cliente_var,
                    "service_type": service_type_en,
                    "lower_bound": ultima_faixa["upper_bound"],  # 99999999999 ou similar
                    "upper_bound": None,
                    "fee_percentage": ultima_faixa["fee_percentage"],
                    "tipo_alteracao": "INSERT",
                    "timestamp": datetime.now()
                }
                linhas_criadas.append(linha_max)
                
                # 2. Criar linhas para cada faixa (início e fim de cada faixa)
                for idx, faixa in enumerate(faixas_data):
                    # Linha do início da faixa
                    linha_inicio = {
                        "fund_id": fund_id_var,
                        "cliente": cliente_var,
                        "service_type": service_type_en,
                        "lower_bound": faixa["lower_bound"],
                        "upper_bound": None,
                        "fee_percentage": faixa["fee_percentage"],
                        "tipo_alteracao": "INSERT",
                        "timestamp": datetime.now()
                    }
                    linhas_criadas.append(linha_inicio)
                    
                    # Linha do fim da faixa (apenas se não for a última faixa)
                    if idx < len(faixas_data) - 1:
                        linha_fim = {
                            "fund_id": fund_id_var,
                            "cliente": cliente_var,
                            "service_type": service_type_en,
                            "lower_bound": faixa["upper_bound"],
                            "upper_bound": None,
                            "fee_percentage": faixa["fee_percentage"],
                            "tipo_alteracao": "INSERT",
                            "timestamp": datetime.now()
                        }
                        linhas_criadas.append(linha_fim)
                
                # Salvar todas as linhas no BigQuery (com usuário logado)
                usuario_atual = st.session_state.get('usuario_logado', 'usuario_kanastra')
                sucesso = True
                for linha in linhas_criadas:
                    # Remover campos temporários
                    linha_limpa = {k: v for k, v in linha.items() if k not in ['tipo_alteracao', 'timestamp']}
                    if not salvar_alteracao_pendente("INSERT", "fee_variavel", linha_limpa, usuario_atual):
                        sucesso = False
                        break
                
                if sucesso:
                    st.success(f"✅ {len(linhas_criadas)} linha(s) de taxa variável criada(s)! Cliente: {cliente_var} - {service_type_pt}")
                    st.info(f"📊 {len(faixas_data)} faixas configuradas = {len(linhas_criadas)} linhas no BigQuery")
                    st.info("⏳ Aguardando aprovação de um aprovador")
                    st.rerun()
                else:
                    st.error("❌ Erro ao salvar uma ou mais linhas")
    
    # FORMULÁRIO 4: Taxa Variável + Editar
    elif st.session_state.tabela_selecionada == "fee_variavel" and acao == "Editar Taxa Existente":
        st.subheader("✏️ Editar Taxa Variável Existente")
        
        # Verificar se dados foram carregados
        if st.session_state.dados_editados is None:
            st.warning("⚠️ **Por favor, carregue os dados primeiro!**")
            st.info("👆 Use o botão '📊 Carregar Dados' acima para carregar a tabela Taxa Variável")
        else:
            # Mapeamento de serviços
            servicos_map = {
                "Administração": "administration",
                "Gestão": "management",
                "Performance": "performance",
                "Custódia": "custody"
                }
            servicos_map_reverse = {v: k for k, v in servicos_map.items()}
            
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
                    service_type_edit_pt = st.selectbox(
                        "Selecione o Serviço",
                        ["Administração", "Gestão", "Performance", "Custódia"],
                        key="edit_var_service"
                    )
                
                    submitted_buscar = st.form_submit_button("🔍 Carregar Faixas para Edição", use_container_width=True, type="primary")
                
                if submitted_buscar:
                    # Converter serviço para inglês
                    service_type_en = servicos_map[service_type_edit_pt]
                    
                    # Buscar todas as faixas deste cliente+serviço
                    df = st.session_state.dados_editados
                    registros = df[(df['cliente'] == cliente_edit_var) & (df['service_type'] == service_type_en)]
                    
                    if not registros.empty:
                        # Ordenar por lower_bound
                        registros = registros.sort_values('lower_bound')
                        st.session_state.faixas_var_para_editar = registros.to_dict('records')
                        st.success(f"✅ {len(registros)} faixas encontradas! Atualize os valores abaixo.")
                        st.rerun()
                    else:
                        st.error(f"❌ Nenhuma faixa encontrada para {cliente_edit_var} - {service_type_edit_pt}")
            
            # Se há faixas carregadas, mostrar formulário de edição
            if 'faixas_var_para_editar' in st.session_state and st.session_state.faixas_var_para_editar:
                st.markdown("---")
                
                with st.form("form_atualizar_faixas_variavel"):
                    st.markdown("### 📊 Edite as faixas abaixo")
                    st.info(f"ℹ️ Total de {len(st.session_state.faixas_var_para_editar)} linha(s) para editar")
                    
                    faixas_editadas = []
                    
                    for idx, faixa in enumerate(st.session_state.faixas_var_para_editar):
                        st.markdown(f"**Linha {idx + 1}:**")
                        col_a, col_b, col_c = st.columns(3)
                        
                        with col_a:
                            lower_edit = st.number_input(
                                f"PL Inicial (R$)",
                                value=float(faixa['lower_bound']),
                                min_value=0.0,
                                step=1000000.0,
                                format="%.0f",
                                key=f"edit_lower_{idx}"
                            )
                        
                        with col_b:
                            # upper_bound pode ser None para algumas linhas
                            upper_val = float(faixa.get('upper_bound', 0)) if faixa.get('upper_bound') is not None else 0.0
                            upper_edit = st.number_input(
                                f"PL Final (R$)",
                                value=upper_val,
                                min_value=0.0,
                                step=1000000.0,
                                format="%.0f",
                                key=f"edit_upper_{idx}",
                                help="Deixe 0 se não aplicável"
                            )
                        
                        with col_c:
                            fee_edit = st.number_input(
                                f"Taxa (%)",
                                value=float(faixa['fee_percentage']),
                                min_value=0.0,
                                max_value=100.0,
                                step=0.0001,
                                format="%.4f",
                                key=f"edit_fee_{idx}"
                            )
                        
                        faixas_editadas.append({
                            "fund_id": int(faixa['fund_id']),
                            "cliente": faixa['cliente'],
                            "service_type": faixa['service_type'],
                            "lower_bound": lower_edit,
                            "upper_bound": upper_edit if upper_edit > 0 else None,
                            "fee_percentage": fee_edit,
                            "original_lower": float(faixa['lower_bound'])  # Para identificar qual linha atualizar
                        })
                    
                    st.markdown("---")
                    
                    col_btn1, col_btn2 = st.columns(2)
                    
                    with col_btn1:
                        submitted_update = st.form_submit_button("💾 Salvar Todas as Alterações", use_container_width=True, type="primary")
                    
                    with col_btn2:
                        cancelar_update = st.form_submit_button("❌ Cancelar", use_container_width=True)
                    
                    if submitted_update:
                        # Salvar todas as faixas editadas no BigQuery
                        sucesso = True
                        for faixa_edit in faixas_editadas:
                            taxa_var_editada = {
                                "fund_id": faixa_edit["fund_id"],
                                "cliente": faixa_edit["cliente"],
                                "service_type": faixa_edit["service_type"],
                                "lower_bound": faixa_edit["lower_bound"],
                                "upper_bound": faixa_edit["upper_bound"],
                                "fee_percentage": faixa_edit["fee_percentage"],
                                "original_lower": faixa_edit["original_lower"]  # Para WHERE clause
                            }
                            
                            usuario_atual = st.session_state.get('usuario_logado', 'usuario_kanastra')
                            if not salvar_alteracao_pendente("UPDATE", "fee_variavel", taxa_var_editada, usuario_atual):
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
        # Filtro por serviço (adapta ao tipo de tabela)
        if st.session_state.tabela_selecionada == "fee_minimo" and 'servico' in st.session_state.dados_editados.columns:
            servicos_unicos = ["Todos"] + sorted(st.session_state.dados_editados['servico'].unique().tolist())
            servico_filtro = st.selectbox("🔍 Filtrar por Serviço", servicos_unicos, key="filtro_servico")
        elif st.session_state.tabela_selecionada == "fee_variavel" and 'service_type' in st.session_state.dados_editados.columns:
            servicos_unicos = ["Todos"] + sorted(st.session_state.dados_editados['service_type'].unique().tolist())
            servico_filtro = st.selectbox("🔍 Filtrar por Service Type", servicos_unicos, key="filtro_service_type")
        else:
            servico_filtro = "Todos"
    
    with col_filtro3:
        # Botão para limpar filtros
        if st.button("🔄 Limpar Filtros", use_container_width=True):
            st.rerun()
    
    # Aplicar filtros
    df_filtrado = st.session_state.dados_editados.copy()
    
    if cliente_filtro != "Todos":
        df_filtrado = df_filtrado[df_filtrado['cliente'] == cliente_filtro]
    
    if servico_filtro != "Todos":
        if st.session_state.tabela_selecionada == "fee_minimo":
            df_filtrado = df_filtrado[df_filtrado['servico'] == servico_filtro]
        else:
            df_filtrado = df_filtrado[df_filtrado['service_type'] == servico_filtro]
    
    st.info(f"**{len(df_filtrado)}** de **{len(st.session_state.dados_editados)}** registros exibidos")
    
    # Planilha sempre visível com filtros aplicados
    st.dataframe(
        df_filtrado,
        use_container_width=True,
        height=400
    )

# =======================
# SEÇÃO 5: PAINEL DE APROVAÇÃO
# =======================

st.markdown("---")

# PAINEL DE APROVAÇÃO (apenas para aprovadores)
if perfil == "aprovador":
    st.subheader("👑 Painel de Aprovação")
else:
    st.subheader("📊 Suas Alterações Pendentes")

# Carregar alterações pendentes do BigQuery
alteracoes_pendentes = carregar_alteracoes_pendentes()

# Filtrar alterações conforme perfil
if perfil == "editor":
    # Editores veem apenas suas próprias alterações
    alteracoes_filtradas = [a for a in alteracoes_pendentes if a.get('usuario') == st.session_state.usuario_logado]
else:
    # Aprovadores veem todas as alterações
    alteracoes_filtradas = alteracoes_pendentes

if alteracoes_filtradas:
    st.markdown("---")
    
    if perfil == "aprovador":
        st.subheader(f"⏳ Todas as Alterações Pendentes ({len(alteracoes_filtradas)})")
    else:
        st.subheader(f"⏳ Suas Alterações Pendentes ({len(alteracoes_filtradas)})")
    
    # Processar cada alteração individualmente
    for idx, alteracao in enumerate(alteracoes_filtradas):
        usuario_alteracao = alteracao.get('usuario', 'N/A')
        
        # Cor de fundo diferente se for alteração de outro usuário (para aprovadores)
        if perfil == "aprovador" and usuario_alteracao != st.session_state.usuario_logado:
            st.markdown(f"""
            <div style='background-color: #fffbea; padding: 10px; border-radius: 8px; border-left: 4px solid #f59e0b; margin-bottom: 10px;'>
                <strong>📝 Alteração #{idx + 1}</strong> - <em>Por: {usuario_alteracao}</em>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"### 📝 Alteração #{idx + 1}")
        
        # Pegar dados do campo JSON
        dados = alteracao['dados']
        
        # Exibir informações
        col_info1, col_info2, col_info3, col_info4 = st.columns(4)
        with col_info1:
            st.info(f"**Tipo:** {alteracao['tipo_alteracao']}")
        with col_info2:
            st.info(f"**Hora:** {alteracao['timestamp'].strftime('%H:%M:%S')}")
        with col_info3:
            st.info(f"**Tabela:** {alteracao['tabela']}")
        with col_info4:
            st.info(f"**Por:** {usuario_alteracao}")
        
        # Mostrar dados da alteração como tabela
        df_alteracao = pd.DataFrame([dados])
        st.dataframe(df_alteracao, use_container_width=True, hide_index=True)
        
        # Botões de aprovação individual (APENAS PARA APROVADORES)
        if perfil == "aprovador":
            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                if st.button(f"✅ Aprovar #{idx + 1}", key=f"aprovar_{idx}", use_container_width=True, type="primary"):
                # Executar INSERT ou UPDATE no BigQuery
                    try:
                        client = get_bigquery_client()
                        tabela = alteracao['tabela']
                        dados = alteracao['dados']
                        
                        if alteracao['tipo_alteracao'] == "INSERT":
                            # Gerar SQL INSERT
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
                            INSERT INTO `kanastra-live.finance.{tabela}` 
                            ({', '.join(colunas_sql)})
                            VALUES ({', '.join(valores)})
                            """
                            
                        else:  # UPDATE
                            # Gerar SQL UPDATE
                            set_clause = []
                            for k, v in dados.items():
                                if k not in ['fund_id', 'cliente', 'service_type', 'servico', 'empresa', 'original_lower']:  # Não atualizar chaves
                                    if v is None:
                                        set_clause.append(f"{k} = NULL")
                                    elif isinstance(v, str):
                                        set_clause.append(f"{k} = '{v}'")
                                    elif isinstance(v, (int, float)):
                                        set_clause.append(f"{k} = {v}")
                                    else:
                                        set_clause.append(f"{k} = '{str(v)}'")
                            
                            # WHERE clause baseado na tabela
                            if tabela == "fee_minimo":
                                where = f"`fund id` = {dados['fund_id']} AND servico = '{dados['servico']}'"
                            else:  # fee_variavel
                                original_lower = dados.get('original_lower', dados['lower_bound'])
                                where = f"`fund id` = {dados['fund_id']} AND service_type = '{dados['service_type']}' AND lower_bound = {original_lower}"
                            
                            sql = f"""
                            UPDATE `kanastra-live.finance.{tabela}`
                            SET {', '.join(set_clause)}
                            WHERE {where}
                            """
                        
                        # Executar query
                        st.code(sql, language="sql")
                        query_job = client.query(sql)
                        query_job.result()
                        
                        # Atualizar status no BigQuery com aprovador
                        aprovador = st.session_state.usuario_aprovador
                        if atualizar_status_alteracao(alteracao['id'], 'APROVADO', aprovador):
                            st.success(f"✅ Alteração #{idx + 1} aprovada por {aprovador} e aplicada no BigQuery!")
                            st.rerun()
                        else:
                            st.error("❌ Erro ao atualizar status da alteração")
                        
                    except Exception as e:
                        st.error(f"❌ Erro ao aplicar alteração: {str(e)}")
            
            with col_btn2:
                if st.button(f"❌ Rejeitar #{idx + 1}", key=f"rejeitar_{idx}", use_container_width=True):
                    aprovador = st.session_state.usuario_aprovador
                    if atualizar_status_alteracao(alteracao['id'], 'REJEITADO', aprovador):
                        st.warning(f"⚠️ Alteração #{idx + 1} rejeitada por {aprovador}!")
                        st.rerun()
                    else:
                        st.error("❌ Erro ao rejeitar alteração")
        else:
            # Editores apenas visualizam, não podem aprovar
            st.info("⏳ Aguardando aprovação de um aprovador")
        
        st.markdown("---")
    else:
        # Mensagem quando não há alterações
        if perfil == "aprovador":
            st.info("✅ Não há alterações pendentes de aprovação no momento")
        else:
            st.info("📝 Você ainda não criou nenhuma alteração pendente")

# Sidebar
st.sidebar.header("ℹ️ Como Usar")
st.sidebar.markdown("""
### 📋 Passo a Passo:
1. **Faça login** com suas credenciais
2. **Selecione** a tabela desejada
3. **Carregue** os dados
4. **Visualize** a planilha completa
5. **Crie ou edite** taxas usando os formulários
6. **Aguarde aprovação** de um aprovador

### 👥 Perfis de Usuário:

**✏️ Editor** (Gustavo, Finance User)
- Pode adicionar novas taxas
- Pode editar taxas existentes
- Alterações ficam pendentes de aprovação
- Visualiza apenas suas próprias alterações

**👑 Aprovador** (Eric, Thiago)
- Todas as permissões de Editor
- Pode aprovar/rejeitar alterações
- Visualiza todas as alterações pendentes
- Pode aplicar mudanças ao BigQuery
""")
