import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# ==========================================
# 1. Configuração da Página e Tema
# ==========================================
st.set_page_config(page_title="NYC Taxi | Executive Board", page_icon="🚕", layout="wide", initial_sidebar_state="expanded")

# Customizando o visual com CSS para deixar os cards de KPI mais elegantes
st.markdown("""
    <style>
    div[data-testid="metric-container"] {
        background-color: #1E1E1E;
        border: 1px solid #333;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    </style>
""", unsafe_allow_html=True)

# Caminhos dos arquivos Gold (Mart e Anomalias)
PARQUET_PATH = 'include/data/gold/mart_taxi_metrics.parquet'


# ==========================================
# 2. Barra Lateral (Sidebar) e Filtros
# ==========================================

with st.sidebar:
    st.title("🚕 NYC Taxi Analytics")
    st.markdown("Dashboard executivo construído com **DuckDB + Streamlit**.")
    st.divider()
    
    st.subheader("📅 Filtros Temporais")
    min_date = pd.to_datetime('2015-01-01')
    max_date = pd.to_datetime('2016-12-31')
    
    date_range = st.date_input(
        "Selecione o Período",
        value=(pd.to_datetime('2016-01-01'), pd.to_datetime('2016-03-31')),
        min_value=min_date,
        max_value=max_date
    )

    st.divider()
    st.markdown("📊 **Camada:** Gold (Mart)")
    st.markdown("🔄 **Atualização:** Diária")
    st.markdown("👨‍💻 **Engenharia:** Arthur Machado")

# ==========================================
# 3. Lógica do Filtro Dinâmico SQL
# ==========================================
if len(date_range) == 2:
    start_date, end_date = date_range
    filter_query = f"WHERE data_corrida BETWEEN '{start_date}' AND '{end_date}'"
else:
    filter_query = ""

# ==========================================
# 4. Funções Auxiliares (Formatação)
# ==========================================
def format_number(num):
    if pd.isna(num): return "0"
    if num >= 1_000_000: return f"{num / 1_000_000:.2f}M"
    elif num >= 1_000: return f"{num / 1_000:.2f}K"
    else: return f"{num:,.0f}"

# ==========================================
# 5. Visão Geral: KPIs Principais
# ==========================================
st.title("🚖 Dashboard: Visão Geral")
st.markdown("Acompanhamento de performance, faturamento e comportamento da frota de táxis amarelos de Nova York.")

@st.cache_data
def get_kpis(query_filter):
    query = f"""
        SELECT 
            SUM(total_corridas) AS total_viagens,
            SUM(receita_total) AS faturamento_total,
            AVG(ticket_medio) AS ticket_medio_geral,
            AVG(gorjeta_media) AS gorjeta_media_geral
        FROM '{PARQUET_PATH}'
        {query_filter}
    """
    return duckdb.sql(query).df()

df_kpi = get_kpis(filter_query)

if pd.isna(df_kpi['total_viagens'][0]):
    st.warning("⚠️ Nenhuma corrida encontrada para o período selecionado. Ajuste os filtros na barra lateral.")
    st.stop()

col1, col2, col3, col4 = st.columns(4)
col1.metric("🚕 Total de Corridas", format_number(df_kpi['total_viagens'][0]))
col2.metric("💰 Faturamento Total", f"${format_number(df_kpi['faturamento_total'][0])}")
col3.metric("📈 Ticket Médio", f"${df_kpi['ticket_medio_geral'][0]:.2f}")
col4.metric("💵 Gorjeta Média", f"${df_kpi['gorjeta_media_geral'][0]:.2f}")

st.divider()

# ==========================================
# 6. Deep Dive: As 5 Views do Desafio
# ==========================================
st.subheader("🔍 Exploração por Dimensões de Negócio")
tab_hora, tab_fornecedor, tab_pagamento, tab_semana = st.tabs([
    "🕒 Operacional por Hora", 
    "🏢 Market Share", 
    "💳 Hábitos de Pagamento", 
    "📅 Sazonalidade Semanal"
])

chart_theme = "plotly_dark" 

# --- ABA 1: OPERACIONAL POR HORA ---
with tab_hora:
    query_hora = f"""
        SELECT 
            hora_dia, 
            SUM(total_corridas) AS volume, 
            AVG(ticket_medio) AS preco,
            (SUM(distancia_total_milhas) / NULLIF(SUM(tempo_total_minutos), 0)) * 60 AS velocidade_media_mph
        FROM '{PARQUET_PATH}' {filter_query} 
        GROUP BY hora_dia 
        ORDER BY hora_dia
    """
    df_hora = duckdb.sql(query_hora).df()
    
    col1, col2 = st.columns(2)
    with col1:
        fig_vol = px.area(df_hora, x='hora_dia', y='volume', title="Evolução do Volume ao Longo do Dia", 
                          color_discrete_sequence=['#FFC107'], template=chart_theme)
        st.plotly_chart(fig_vol, use_container_width=True)
    with col2:
        fig_preco = px.line(df_hora, x='hora_dia', y='preco', title="Variação do Ticket Médio por Hora", 
                            color_discrete_sequence=['#2E7D32'], markers=True, template=chart_theme)
        st.plotly_chart(fig_preco, use_container_width=True)
        
    st.divider()
    fig_vel = px.bar(df_hora, x='hora_dia', y='velocidade_media_mph', 
                     title="Velocidade Média da Frota por Hora (MPH)",
                     labels={'velocidade_media_mph':'Velocidade (MPH)', 'hora_dia': 'Hora do Dia'},
                     color='velocidade_media_mph', color_continuous_scale='Blues', template=chart_theme)
    fig_vel.update_layout(xaxis=dict(tickmode='linear', dtick=1))
    st.plotly_chart(fig_vel, use_container_width=True)

# --- ABA 2: FORNECEDOR (MARKET SHARE) ---
with tab_fornecedor:
    query_vendor = f"""
        SELECT 
            CASE WHEN vendor_id = 1 THEN 'Creative Mobile' ELSE 'VeriFone Inc' END AS fornecedor,
            SUM(total_corridas) AS volume, SUM(receita_total) AS receita
        FROM '{PARQUET_PATH}' {filter_query} GROUP BY vendor_id
    """
    df_vendor = duckdb.sql(query_vendor).df()
    
    col1, col2 = st.columns(2)
    with col1:
        fig_share = px.pie(df_vendor, values='volume', names='fornecedor', title="Market Share por Volume de Corridas",
                           hole=0.4, color_discrete_sequence=['#1E88E5', '#D32F2F'], template=chart_theme)
        fig_share.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_share, use_container_width=True)
    with col2:
        fig_rev = px.bar(df_vendor, x='fornecedor', y='receita', title="Receita Total Gerada",
                         labels={'fornecedor':'Fornecedor', 'receita':'Receita (USD)'},
                         color='fornecedor', color_discrete_sequence=['#1E88E5', '#D32F2F'], template=chart_theme)
        st.plotly_chart(fig_rev, use_container_width=True)

# --- ABA 3: PAGAMENTO (INVERTIDA) ---
with tab_pagamento:
    query_pagamento = f"""
        SELECT 
            CASE WHEN payment_type = 1 THEN 'Cartão de Crédito' 
                 WHEN payment_type = 2 THEN 'Dinheiro'
                 WHEN payment_type = 3 THEN 'Sem Cobrança' 
                 WHEN payment_type = 4 THEN 'Disputa' 
                 ELSE 'Desconhecido' END AS tipo,
            SUM(total_corridas) AS uso, 
            AVG(ticket_medio) AS ticket_medio
        FROM '{PARQUET_PATH}' {filter_query} 
        GROUP BY payment_type 
        ORDER BY uso DESC
    """
    df_pagamento = duckdb.sql(query_pagamento).df()
    
    st.info("💡 **Insight de Negócio:** No sistema da TLC, gorjetas pagas em dinheiro físico não são registradas pelo taxímetro. Por isso, a comparação foca na participação de mercado e no Ticket Médio.")
    
    col1, col2 = st.columns(2)
    with col1:
        fig_ticket = px.bar(df_pagamento, x='tipo', y='ticket_medio', 
                             title="Ticket Médio por Método de Pagamento ($)",
                             labels={'ticket_medio':'Valor Médio ($)', 'tipo':'Método'},
                             color='tipo',
                             color_discrete_sequence=['#8E24AA', '#D81B60', '#1E88E5', '#43A047'], 
                             template=chart_theme)
        fig_ticket.update_layout(showlegend=False)
        st.plotly_chart(fig_ticket, use_container_width=True)

    with col2:
        fig_uso = px.pie(df_pagamento, values='uso', names='tipo', 
                         title="Distribuição dos Métodos de Pagamento", hole=0.4,
                         color_discrete_sequence=['#8E24AA', '#D81B60', '#1E88E5', '#43A047'], 
                         template=chart_theme)
        fig_uso.update_traces(textposition='inside', textinfo='percent+label')
        fig_uso.update_layout(showlegend=False) 
        st.plotly_chart(fig_uso, use_container_width=True)

# --- ABA 4: DIA DA SEMANA ---
with tab_semana:
    query_semana = f"""
        SELECT dia_semana_num,
            CASE WHEN dia_semana_num = 0 THEN 'Domingo' WHEN dia_semana_num = 1 THEN 'Segunda'
                 WHEN dia_semana_num = 2 THEN 'Terça' WHEN dia_semana_num = 3 THEN 'Quarta'
                 WHEN dia_semana_num = 4 THEN 'Quinta' WHEN dia_semana_num = 5 THEN 'Sexta' WHEN dia_semana_num = 6 THEN 'Sábado' END AS dia,
            SUM(total_corridas) AS volume
        FROM '{PARQUET_PATH}' {filter_query} GROUP BY dia_semana_num ORDER BY dia_semana_num
    """
    df_semana = duckdb.sql(query_semana).df()
    
    fig_semana = px.line(df_semana, x='dia', y='volume', title="Comportamento Semanal de Corridas",
                         labels={'dia':'Dia da Semana', 'volume':'Total de Corridas'},
                         markers=True, color_discrete_sequence=['#E53935'], template=chart_theme)
    fig_semana.update_traces(line=dict(width=3), marker=dict(size=8))
    st.plotly_chart(fig_semana, use_container_width=True)

