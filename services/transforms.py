import pandas as pd

def transform_conversations(df):
    """
    Groups traffic data by date to sum active conversations.
    """
    return df.groupby('Fecha')['Conversaciones Activas'].sum().reset_index().sort_values('Fecha')

def transform_agendados(df):
    """
    Groups traffic data by date to sum agendados.
    """
    return df.groupby('Fecha')['Agendados'].sum().reset_index().sort_values('Fecha')

def transform_hired(df, const_date='Fecha Ingreso DT'):
    """
    Groups pipeline data by hire date to count hired candidates.
    """
    if const_date in df.columns:
        df_hired = df[df['contrata programa'] == 'Sí'].groupby(const_date).size().reset_index(name='Contratados')
        df_hired = df_hired.rename(columns={const_date: 'Fecha Ingreso'})
    else:
        # Fallback if column name differs or needs parsing
        df['Fecha Ingreso'] = pd.to_datetime(df['Fecha Ingreso'], dayfirst=True, errors='coerce')
        df_hired = df[df['contrata programa'] == 'Sí'].groupby('Fecha Ingreso').size().reset_index(name='Contratados')
    
    return df_hired.sort_values('Fecha Ingreso')

def group_daily_metrics(traffic_f, pipeline_f):
    """
    Aggregates daily metrics for key indicators.
    """
    daily_conv = transform_conversations(traffic_f)
    daily_agendas = transform_agendados(traffic_f)
    daily_hired = transform_hired(pipeline_f)
    
    return daily_conv, daily_agendas, daily_hired

def group_weekly_metrics(traffic_f, pipeline_f):
    """
    Aggregates metrics by week for evolution charts.
    """
    # 1. Weekly Conversations & Agendas from Traffic (Assuming Traffic has Agendados column populated in ETL)
    # If traffic_f only has 'Conversaciones Activas', we get Agendados from pipeline
    # In 'load_agendados' (etl.py), we merged into traffic. So traffic_f HAS 'Agendados'.
    
    # Resample requires datetime index or on
    t_df = traffic_f.copy()
    if 'Fecha' in t_df.columns:
        t_df = t_df.set_index('Fecha')
        
    weekly_traf = t_df.resample('W-MON')[['Conversaciones Activas', 'Agendados']].sum().reset_index()
    
    # 2. Weekly Closures from Pipeline
    # Closures based on 'Fecha Ingreso'
    p_df = pipeline_f.copy()
    # Ensure Fecha Ingreso is datetime (it might be object if mixed or not parsed yet, though transform_hired handles it)
    # Let's rely on transform_hired logic but adapting for resampling
    
    # We need a dataframe with Fecha Ingreso to resample
    # Let's use 'Fecha Ingreso DT' if available directly
    date_col = 'Fecha Ingreso DT' if 'Fecha Ingreso DT' in p_df.columns else 'Fecha Ingreso'
    # Ensure it's datetime
    p_df[date_col] = pd.to_datetime(p_df[date_col], dayfirst=True, errors='coerce')
    
    # Filter only hired
    hired_df = p_df[p_df['contrata programa'] == 'Sí'].copy()
    if not hired_df.empty:
        weekly_hired = hired_df.set_index(date_col).resample('W-MON').size().reset_index(name='Contratados')
        # Rename date col to match 'Fecha' for merge
        weekly_hired = weekly_hired.rename(columns={date_col: 'Fecha'})
    else:
        weekly_hired = pd.DataFrame(columns=['Fecha', 'Contratados'])
        
    # 3. Merge
    weekly_combined = pd.merge(weekly_traf, weekly_hired, on='Fecha', how='outer').fillna(0)
    weekly_combined = weekly_combined.sort_values('Fecha')
    
    return weekly_combined

def group_channel_conversion(pipeline_f):
    """
    Calculates conversion rates (Agendados -> Cierre) by Channel.
    Since we don't have source 'Conversaciones' by channel, we assume Pipeline entry = Agenda (or intended agenda).
    """
    # Group by Medio
    # Count Total (Agendas)
    # Count Won (Sí)
    
    summary = pipeline_f.groupby('medio contacto').agg(
        Agendados=('usuario', 'count'),
        Contratados=('contrata programa', lambda x: (x == 'Sí').sum())
    ).reset_index()
    
    summary['Tasa Cierre'] = (summary['Contratados'] / summary['Agendados'] * 100).fillna(0)
    summary = summary.sort_values('Tasa Cierre', ascending=False)
    
    return summary
