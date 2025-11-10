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

# CSS customizado com fontes e identidade visual Kanastra
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500&display=swap');
    
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

# Função para salvar histórico de waiver no BigQuery
def salvar_waiver_bigquery(fund_name, valor_waiver, tipo_waiver, data_inicio, data_fim, usuario="usuario_kanastra"):
    """Salva registro de waiver no BigQuery"""
    client = get_bigquery_client()
    
    try:
        waiver_id = str(uuid.uuid4())
        data_aplicacao = datetime.now().isoformat()
        
        query = f"""
        INSERT INTO `kanastra-live.finance.historico_waivers` 
        (id, data_aplicacao, usuario, fund_name, valor_waiver, tipo_waiver, data_inicio, data_fim, observacao)
        VALUES (
            '{waiver_id}',
            TIMESTAMP('{data_aplicacao}'),
            '{usuario}',
            '{fund_name}',
            {valor_waiver},
            '{tipo_waiver}',
            DATE('{data_inicio}'),
            DATE('{data_fim}'),
            'Aplicado via Dashboard'
        )
        """
        
        client.query(query).result()
        return True
    except Exception as e:
        st.error(f"❌ Erro ao salvar waiver no BigQuery: {e}")
        return False

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

# Seção de Waiver
st.sidebar.subheader("💰 Provisionar Waiver")

# Seleção de fundos para waiver
fundos_waiver = st.sidebar.multiselect(
    "Selecione os fundos para Waiver:",
    fundos_disponiveis[1:],  # Remove "Todos"
    help="Escolha um ou mais fundos para aplicar waiver"
)

# Inputs de valor e tipo para cada fundo
valores_waiver = {}
tipos_waiver = {}
for fundo in fundos_waiver:
    st.sidebar.write(f"**{fundo}:**")
    
    col1, col2 = st.sidebar.columns([2, 1])
    
    with col1:
        valores_waiver[fundo] = st.number_input(
            f"Valor (R$):",
            min_value=0.0,
            value=0.0,
            step=100.0,
            format="%.2f",
            key=f"valor_waiver_{fundo}",
            label_visibility="collapsed"
        )
    
    with col2:
        tipos_waiver[fundo] = st.selectbox(
            f"Tipo:",
            ["Provisionado", "Não Provisionado"],
            key=f"tipo_waiver_{fundo}",
            label_visibility="collapsed"
        )
    
    st.sidebar.caption(f"💰 R$ {valores_waiver[fundo]:,.2f} - {tipos_waiver[fundo]}")
    st.sidebar.divider()

# Botões de ação
col_btn1, col_btn2 = st.sidebar.columns(2)

with col_btn1:
    # Botão para aplicar waiver
    aplicar_waiver = st.button(
        "💾 Aplicar",
        type="secondary",
        use_container_width=True,
        disabled=(not fundos_waiver or all(v <= 0 for v in valores_waiver.values())),
        help="Aplicar o waiver aos fundos selecionados"
    )

with col_btn2:
    # Botão para remover waiver
    remover_waiver = st.button(
        "🗑️ Remover",
        type="secondary",
        use_container_width=True,
        help="Remover todos os waivers aplicados"
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
if st.sidebar.button("🚀 Executar Query SQL", type="primary", use_container_width=True):
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

# Aplicar Waiver se solicitado
if aplicar_waiver and fundos_waiver and any(v > 0 for v in valores_waiver.values()):
    st.session_state['waiver_aplicado'] = {
        'fundos': fundos_waiver,
        'valores': valores_waiver,
        'tipos': tipos_waiver
    }
    st.sidebar.success("✅ Waiver será aplicado!")
    st.rerun()

# Remover Waiver se solicitado
if remover_waiver:
    if 'waiver_aplicado' in st.session_state:
        del st.session_state['waiver_aplicado']
        st.sidebar.success("✅ Waiver removido!")
        st.rerun()
    else:
        st.sidebar.warning("⚠️ Nenhum waiver ativo para remover")

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
    
    # APLICAR WAIVER se configurado
    waiver_info = st.session_state.get('waiver_aplicado', None)
    
    if waiver_info and waiver_info['fundos']:
        # Encontrar coluna de acumulado
        col_acumulado = None
        for col in df_filtrado.columns:
            if 'acumulado' in col.lower():
                col_acumulado = col
                break
        if col_acumulado:
            for fundo in waiver_info['fundos']:
                valor = waiver_info['valores'].get(fundo, 0)
                tipo_fundo = waiver_info['tipos'].get(fundo, "Provisionado")
                
                if valor > 0:
                    mask_fundo = (df_filtrado['fund_name'] == fundo)
                    
                    if tipo_fundo == "Provisionado":
                        # Provisionado: distribuir proporcionalmente por todos os registros do fundo
                        qtd_registros = mask_fundo.sum()
                        valor_por_registro = valor / qtd_registros if qtd_registros > 0 else 0
                        
                        # Aplicar ao acumulado
                        df_filtrado.loc[mask_fundo, col_acumulado] = (
                            df_filtrado.loc[mask_fundo, col_acumulado] - valor_por_registro
                        )
                        st.info(f"💰 **{fundo}** - Provisionado: R$ {valor_por_registro:,.2f}/registro × {qtd_registros} registros = R$ {valor:,.2f}")
                    else:
                        # Não Provisionado: aplicar tudo no último registro do fundo
                        idx_ultimo = df_filtrado[mask_fundo].index.max()
                        if pd.notnull(idx_ultimo):
                            # Aplicar ao acumulado
                            df_filtrado.at[idx_ultimo, col_acumulado] = (
                                df_filtrado.at[idx_ultimo, col_acumulado] - valor
                            )
                            st.info(f"💰 **{fundo}** - Não Provisionado: R$ {valor:,.2f} no último registro")
                        else:
                            st.warning(f"⚠️ Nenhum registro encontrado para o fundo {fundo}")
        else:
            st.warning("⚠️ Coluna 'acumulado' não encontrada nos dados")
    
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
    
    # Alerta se waiver está ativo
    if waiver_info and waiver_info.get('valores') and any(v > 0 for v in waiver_info['valores'].values()):
        waivers_txt = []
        for fundo in waiver_info['fundos']:
            if waiver_info['valores'][fundo] > 0:
                tipo = waiver_info['tipos'].get(fundo, "Provisionado")
                waivers_txt.append(f"{fundo}: R$ {waiver_info['valores'][fundo]:,.2f} ({tipo})")
        st.success(f"✅ **Waiver Ativo:** {' | '.join(waivers_txt)}")
    
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
    
    # Botões de ação
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.info(f"📊 Exibindo **{len(df):,}** registros")
    
    with col2:
        if st.button("🔄 Limpar Cache", use_container_width=True):
            st.cache_data.clear()
            st.success("✅ Cache limpo!")
            st.rerun()
    
    with col3:
        download_csv = df.to_csv(index=False).encode('utf-8')
        
        # Botão de exportar CSV
        csv_button_clicked = st.download_button(
            label="📥 Exportar CSV",
            data=download_csv,
            file_name=f'calculadora_taxas_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
            mime='text/csv',
            type="primary",
            use_container_width=True
        )
        
        # Se há waiver aplicado e botão foi clicado, salvar no BigQuery
        if csv_button_clicked and 'waiver_aplicado' in st.session_state:
            waiver_data = st.session_state['waiver_aplicado']
            fundos = waiver_data['fundos']
            valores = waiver_data['valores']
            tipos = waiver_data['tipos']
            
            sucesso_total = True
            for fundo in fundos:
                valor = valores.get(fundo, 0)
                tipo = tipos.get(fundo, "Provisionado")
                
                if valor > 0:
                    if not salvar_waiver_bigquery(fundo, valor, tipo, data_inicio, data_fim):
                        sucesso_total = False
            
            if sucesso_total:
                st.success("✅ CSV exportado e histórico de waiver salvo no BigQuery!")
            else:
                st.warning("⚠️ CSV exportado, mas houve erro ao salvar histórico de waiver no BigQuery")
    
    # Exibir tabela
    st.subheader("📋 Resultados da Query SQL")
    
    # Selecionar e renomear apenas as colunas desejadas
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
        use_container_width=True,
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
            
            st.plotly_chart(fig1, use_container_width=True)
        
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
            
            st.plotly_chart(fig2, use_container_width=True)
    
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
