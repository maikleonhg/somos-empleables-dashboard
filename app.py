import streamlit as st
from PIL import Image
import pandas as pd
import traceback

# 1. Configuration (Must be first)
st.set_page_config(
    page_title="Dashboard - Somos Empleables",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. Imports (Components & Services)
from services.etl import load_combined_data, load_pipeline
from services.transforms import group_daily_metrics, group_weekly_metrics, group_channel_conversion
from services.metrics import calculate_kpis
from components.filters import render_filters
from components.kpi import render_kpi
from components.charts import (
    plot_funnel, plot_sankey, plot_gender_dist, 
    plot_status_conversion, plot_daily_conversion, plot_contact_method,
    plot_weekly_evolution, plot_channel_conversion
)
from components.cards import load_style, render_error, render_chart_card

# 3. Load Styles
try:
    load_style("assets/style.css")
except FileNotFoundError:
    st.warning("Archivo CSS no encontrado. Asegúrate de que 'assets/style.css' exista.")

# --- CUSTOM ASSETS (Material Icons) ---
st.write('<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">', unsafe_allow_html=True)

# 4. Header & Logo
try:
    image = Image.open('assets/logo.jpeg')
    st.sidebar.image(image)
except Exception:
    pass

st.title('Dashboard - Somos Empleables')

# 5. Main Execution Block
try:
    # 5.1 Load Data
    traffic = load_combined_data().reset_index()
    pipeline = load_pipeline()
    
    # Ensure date formats
    traffic['Fecha'] = pd.to_datetime(traffic['Fecha'])
    if 'fecha agenda' in pipeline.columns:
        pipeline['fecha agenda'] = pd.to_datetime(pipeline['fecha agenda'])

    # 5.2 Filter Data
    traffic_f, pipeline_f = render_filters(traffic, pipeline)
    
    # 5.3 Transform Data
    daily_conv, daily_agendas, daily_hired = group_daily_metrics(traffic_f, pipeline_f)
    if not traffic_f.empty and not pipeline_f.empty:
        weekly_df = group_weekly_metrics(traffic_f, pipeline_f)
        channel_df = group_channel_conversion(pipeline_f)
    else:
        weekly_df = pd.DataFrame()
        channel_df = pd.DataFrame()
    
    # 5.4 Calculate KPIs
    kpis = calculate_kpis(pipeline_f, daily_conv)
    
    # --- SECTION 1: KPI CARDS ---
    st.header("1. Resumen General")
    k1, k2, k3 = st.columns(3)
    
    with k1:
        render_kpi(
            daily_conv, 
            'Conversaciones Activas', 'Conversaciones', 'conversaciones', 
            grafica="area", 
            total_override=kpis['total_conv_val']
        )
    with k2:
        render_kpi(
            daily_agendas, 
            'Agendados', 'Agendados', 'agendados', 
            grafica="area", 
            total_override=kpis['total_agendados_val'], 
            delta_override=kpis['rate_conv_agendados'], 
            delta_label="Tasa Conv."
        )
    with k3:
        if not daily_hired.empty:
            render_kpi(
                daily_hired, 
                'Contratados', 'Contratados', 'contratados', 
                grafica="bar", 
                total_override=kpis['total_contratados_val'], 
                delta_override=kpis['rate_cierre_contratados'], 
                delta_label="Tasa Cierre"
            )
        else:
             with st.container():
                st.metric(label="Contratados", value="0")

    # --- SECTION 2: EVOLUCIÓN Y CANALES (NUEVO) ---
    st.header("Metricas Operativas")
    c_new1, c_new2 = st.columns(2)
    
    with c_new1:
        st.subheader("Evolución Semanal (Conversaciones → Cierres)")
        if not weekly_df.empty:
            st.plotly_chart(plot_weekly_evolution(weekly_df), width="stretch", key="weekly_evo")
        else:
            st.info("Sin datos suficientes")

    with c_new2:
        st.subheader("Eficiencia por Canal (Cierre)")
        if not channel_df.empty:
            st.plotly_chart(plot_channel_conversion(channel_df), width="stretch", key="channel_eff")

    # --- SECTION 3: METRICS BREAKDOWN (OLD) ---
    st.header("Desglose")
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Distribución por Género")
        st.plotly_chart(plot_gender_dist(pipeline_f), width="stretch", key="gender_dist")
        
    with c2:
        st.subheader("Tasa de Cierre por Estado Laboral")
        st.plotly_chart(plot_status_conversion(pipeline_f), width="stretch", key="status_conv")

    # Keeping the Contact Method Aggregates
    st.subheader("Volumen por Canal: Agendados vs Retirados")
    st.plotly_chart(plot_contact_method(pipeline_f), width="stretch", key="contact_vol")
    
    st.header("Embudo de Conversión")
    funnel_data = dict(
        number=[kpis['total_conv_val'], kpis['total_agendados_val'], kpis['total_contratados_val']],
        stage=["Conversaciones", "Agendados", "Contratados"]
    )
    st.subheader("Embudo de Conversión Macro")
    st.plotly_chart(plot_funnel(funnel_data), width="stretch", key="funnel_macro")

    # --- SECTION 4: DETAILED DATA ---
    st.header("Data Detallada")
    with st.expander("Ver Data Tráfico (Diario)"):
        st.dataframe(traffic)
    
    with st.expander("Ver Pipeline Completo (Por Cliente)"):
        st.dataframe(pipeline_f)

    # --- SECTION 5: EXTRA ANALYTICS (MOVED DOWN) ---
    st.markdown("---")
    st.subheader("Análisis Profundo de Flujos")
    
    ec1, ec2 = st.columns(2)
    with ec1:
        st.subheader("Flujo de Clientes (Sankey)")
        st.plotly_chart(plot_sankey(pipeline_f), width="stretch", key="sankey_flow")
    with ec2:
         st.subheader("Tasa de Conversión Diaria (%)")
         st.plotly_chart(plot_daily_conversion(traffic_f), width="stretch", key="daily_conv_rate")

except Exception as e:
    render_error("Error general en el dashboard", e)
    # st.text(traceback.format_exc())
