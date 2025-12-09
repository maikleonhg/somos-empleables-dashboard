import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import pandas as pd

def aplicarBackgroundChart(fig, color="#ffffff"):
    """
    Aplica un color de fondo a un gráfico de Plotly para que coincida con el tema.
    """
    return fig.update_layout({
        "plot_bgcolor": color,
        "paper_bgcolor": color,
        "font": {"color": "#000000"}
    })

def plot_funnel(data):
    fig = px.funnel(data, x='number', y='stage')
    return aplicarBackgroundChart(fig)

def plot_sankey(df_pipeline_filtered):
    # Prepare Sankey Data
    sources = df_pipeline_filtered['medio contacto'].unique().tolist()
    professions = df_pipeline_filtered['profesión/formación'].unique().tolist()
    statuses = ['Contratado', 'No Contratado']
    reasons = df_pipeline_filtered[df_pipeline_filtered['contrata programa'] == 'No']['Motivo por el que no continua'].unique().tolist()
    reasons = [r for r in reasons if r != '-']
    
    all_labels = sources + professions + statuses + reasons
    label_map = {label: i for i, label in enumerate(all_labels)}
    
    sources_idx = []
    targets_idx = []
    values = []
    
    # 1. Source -> Profession
    flow1 = df_pipeline_filtered.groupby(['medio contacto', 'profesión/formación']).size().reset_index(name='count')
    for _, row in flow1.iterrows():
        sources_idx.append(label_map[row['medio contacto']])
        targets_idx.append(label_map[row['profesión/formación']])
        values.append(row['count'])
        
    # 2. Profession -> Status
    df = df_pipeline_filtered.copy()
    df['status_label'] = df['contrata programa'].map({'Sí': 'Contratado', 'No': 'No Contratado'})
    
    flow2 = df.groupby(['profesión/formación', 'status_label']).size().reset_index(name='count')
    for _, row in flow2.iterrows():
        sources_idx.append(label_map[row['profesión/formación']])
        targets_idx.append(label_map[row['status_label']])
        values.append(row['count'])
        
    # 3. Status (No Contratado) -> Reason
    flow3 = df[df['status_label'] == 'No Contratado'].groupby(['status_label', 'Motivo por el que no continua']).size().reset_index(name='count')
    for _, row in flow3.iterrows():
        if row['Motivo por el que no continua'] in label_map:
            sources_idx.append(label_map[row['status_label']])
            targets_idx.append(label_map[row['Motivo por el que no continua']])
            values.append(row['count'])
            
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=all_labels,
            color="blue"
        ),
        link=dict(
            source=sources_idx,
            target=targets_idx,
            value=values
        ))])
    
    return aplicarBackgroundChart(fig)

def plot_gender_dist(df):
    gender_counts = df['Genero'].value_counts().reset_index()
    gender_counts.columns = ['Genero', 'Count']
    fig = px.pie(gender_counts, values='Count', names='Genero', hole=0.4)
    return aplicarBackgroundChart(fig)

def plot_status_conversion(df):
    status_conversion = df.groupby(['estado', 'contrata programa']).size().reset_index(name='Count')
    total_by_status = status_conversion.groupby('estado')['Count'].transform('sum')
    status_conversion['Percentage'] = (status_conversion['Count'] / total_by_status * 100)
    
    fig = px.bar(
        status_conversion, 
        x='estado', 
        y='Percentage', 
        color='contrata programa', 
        text=status_conversion['Percentage'].apply(lambda x: f'{x:.1f}%'),
        title="Distribución Contratados vs No Contratados"
    )
    return aplicarBackgroundChart(fig)

def plot_daily_conversion(df_trafico):
    fig = px.line(df_trafico, x='Fecha', y='Tasa Conversion', markers=True)
    return aplicarBackgroundChart(fig)

def plot_contact_method(df_pipeline_filtered):
    # 1. Agendados count per medium
    agendados_by_source = df_pipeline_filtered['medio contacto'].value_counts().reset_index()
    agendados_by_source.columns = ['Medio', 'Count']
    agendados_by_source['Type'] = 'Agendados'
    
    # 2. Retirados count per medium (where contrata == 'No')
    retirados_df = df_pipeline_filtered[df_pipeline_filtered['contrata programa'] == 'No']
    retirados_by_source = retirados_df['medio contacto'].value_counts().reset_index()
    retirados_by_source.columns = ['Medio', 'Count']
    retirados_by_source['Type'] = 'Retirados'
    
    # 3. Combine
    comparison_df = pd.concat([agendados_by_source, retirados_by_source])
    
    # 4. Plot
    fig = px.bar(comparison_df, x='Medio', y='Count', color='Type', barmode='group')
    return aplicarBackgroundChart(fig)

def plot_weekly_evolution(weekly_df):
    """
    Plots stacked or grouped bars for Conversations, Agendas, Hires over time.
    Structure: Week -> Value, Color=Metric
    """
    # Unpivot / Melt for plotting
    df_melt = weekly_df.melt(id_vars='Fecha', value_vars=['Conversaciones Activas', 'Agendados', 'Contratados'], var_name='Métrica', value_name='Cantidad')
    
    fig = px.bar(
        df_melt, 
        x='Fecha', 
        y='Cantidad', 
        color='Métrica', 
        barmode='group',
        title='Evolución Semanal de Operación',
        labels={'Fecha': 'Semana', 'Cantidad': 'Volumen'}
    )
    return aplicarBackgroundChart(fig)

def plot_channel_conversion(channel_summary):
    """
    Plots conversion rate by channel.
    """
    # Create text labels
    channel_summary['Label'] = channel_summary.apply(lambda x: f"{x['Tasa Cierre']:.1f}% ({x['Contratados']}/{x['Agendados']})", axis=1)
    
    fig = px.bar(
        channel_summary,
        x='medio contacto',
        y='Tasa Cierre',
        text='Label',
        title='Tasa de Cierre por Medio de Contacto (Agendados → Contratados)',
        labels={'medio contacto': 'Canal', 'Tasa Cierre': 'Tasa Cierre (%)'}
    )
    fig.update_traces(textposition='outside')
    return aplicarBackgroundChart(fig)
