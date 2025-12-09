import streamlit as st
import pandas as pd

def render_filters(traffic, pipeline):
    """
    Renders the sidebar filters and returns the filtered dataframes.
    Arguments renamed to generic 'traffic' and 'pipeline'.
    """
    st.sidebar.header("Filtros")
    
    # 1. Time Filter
    st.sidebar.subheader("Periodo")
    # time_filter = st.sidebar.radio("Seleccionar Periodo", ["Todo", "Esta Semana", "Este Mes", "Personalizado"])
    time_filter = st.sidebar.radio("Seleccionar Periodo", ["Todo", "Personalizado"]) # Temporarily hidden Week/Month
    
    start_date = traffic['Fecha'].min()
    end_date = traffic['Fecha'].max()
    
    if time_filter == "Esta Semana":
        today = pd.Timestamp.now().normalize()
        start_week = today - pd.Timedelta(days=today.dayofweek)
        start_date = start_week
        end_date = today
    elif time_filter == "Este Mes":
        today = pd.Timestamp.now().normalize()
        start_date = today.replace(day=1)
        end_date = today
    elif time_filter == "Personalizado":
        c1, c2 = st.sidebar.columns(2)
        start_date = c1.date_input("Inicio", start_date)
        end_date = c2.date_input("Fin", end_date)
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
    # Filter DataFrames by Date - using Timestamp comparison to include full end_date
    start_ts = pd.to_datetime(start_date).normalize()
    # Set end_ts to the very end of the end_date (23:59:59.999999)
    end_ts = pd.to_datetime(end_date).normalize() + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    
    # Filter Traffic
    traffic_f = traffic[(traffic['Fecha'] >= start_ts) & (traffic['Fecha'] <= end_ts)]
    # Sanear columna fecha agenda (si no lo hiciste en ETL)
    pipeline['fecha agenda'] = pd.to_datetime(pipeline['fecha agenda'], errors='coerce')

    # Filtrado seguro: Si tiene fecha agenda -> filtra por fecha. Si NO la tiene (NaT) -> se mantiene.
    pipeline_f = pipeline[
        pipeline['fecha agenda'].isna() |
        ((pipeline['fecha agenda'] >= start_ts) & (pipeline['fecha agenda'] <= end_ts))
    ]
    
    # 2. Employment Status Filter
    st.sidebar.subheader("Perfil")
    # IMPORTANT: Los valores del sidebar deben venir del pipeline_f filtrado por fecha
    all_statuses = pipeline_f['estado'].dropna().unique().tolist()
    sel_statuses = st.sidebar.multiselect("Estado Laboral", all_statuses, default=all_statuses)
    
    # 3. Gender Filter
    all_genders = pipeline_f['Genero'].dropna().unique().tolist()
    sel_genders = st.sidebar.multiselect("Género", all_genders, default=all_genders)
    
    # 4. Contract Status Filter
    all_contracts = pipeline_f['contrata programa'].dropna().unique().tolist()
    sel_contracts = st.sidebar.multiselect("Contratado", all_contracts, default=all_contracts)
    
    # Apply Attribute Filters
    pipeline_f = pipeline_f[
        (pipeline_f['estado'].isin(sel_statuses)) &
        (pipeline_f['Genero'].isin(sel_genders)) &
        (pipeline_f['contrata programa'].isin(sel_contracts))
    ]
    
    return traffic_f, pipeline_f
