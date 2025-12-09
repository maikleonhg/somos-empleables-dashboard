import pandas as pd
import numpy as np
import os

# Define paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), 'Data')

def load_conversaciones():
    """
    Loads 'conversaciones_completo.csv' from Data folder.
    """
    file_path = os.path.join(DATA_DIR, 'conversaciones_completo.csv')
    if not os.path.exists(file_path):
        return pd.DataFrame(columns=['Fecha', 'Conversaciones Activas'])
        
    df = pd.read_csv(file_path)
    df['Fecha'] = pd.to_datetime(df['Fecha'])
    df = df.set_index('Fecha').sort_index()
    return df

def load_agendados():
    """
    Loads 'pipeline_completo.csv' and aggregates it to match the expected format for 'load_combined_data'.
    Returns DataFrame indexed by 'Fecha' with columns:
    - Agendados
    - Breakdown by status (Empleado, etc.)
    - Breakdown by medium
    """
    file_path = os.path.join(DATA_DIR, 'pipeline_completo.csv')
    if not os.path.exists(file_path):
        return pd.DataFrame() # Return empty if missing
        
    df = pd.read_csv(file_path)
    df['fecha agenda'] = pd.to_datetime(df['fecha agenda'])
    
    # 1. Total Agendados per day
    daily_counts = df.groupby('fecha agenda').size().reset_index(name='Agendados')
    daily_counts = daily_counts.set_index('fecha agenda')
    
    # 2. Breakdown by Estado
    if 'estado' in df.columns:
        status_counts = df.pivot_table(index='fecha agenda', columns='estado', aggfunc='size', fill_value=0)
    else:
        status_counts = pd.DataFrame()

    # 3. Breakdown by Medio Contacto
    if 'medio contacto' in df.columns:
        contact_counts = df.pivot_table(index='fecha agenda', columns='medio contacto', aggfunc='size', fill_value=0)
    else:
        contact_counts = pd.DataFrame()
        
    # Merge all
    df_agendados = daily_counts.join(status_counts, how='outer').join(contact_counts, how='outer').fillna(0)
    df_agendados.index.name = 'Fecha'
    
    return df_agendados

def load_combined_data():
    """
    Loads both sources and joins them.
    """
    df_conv = load_conversaciones()
    df_agenda = load_agendados()
    
    # Merge
    df_combined = df_conv.join(df_agenda, how='outer').fillna(0)
    
    # Calculate Conversion Rate
    # Avoid div by zero
    df_combined['Tasa Conversion'] = np.where(
        df_combined['Conversaciones Activas'] > 0,
        (df_combined['Agendados'] / df_combined['Conversaciones Activas']) * 100,
        0
    )
    
    return df_combined

def load_pipeline():
    """
    Loads the full detailed pipeline.
    """
    file_path = os.path.join(DATA_DIR, 'pipeline_completo.csv')
    if not os.path.exists(file_path):
        return pd.DataFrame()
        
    df = pd.read_csv(file_path)
    
    # Ensure types
    if 'fecha agenda' in df.columns:
        df['fecha agenda'] = pd.to_datetime(df['fecha agenda'])
    if 'Fecha Ingreso DT' in df.columns:
        df['Fecha Ingreso DT'] = pd.to_datetime(df['Fecha Ingreso DT'])
    
    # Standardize Text Columns to fix duplicates (e.g. REFERIDO vs Referido)
    text_cols = ['medio contacto', 'estado', 'Genero', 'contrata programa']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()
            # Special case for "Lead Magnet" or acronyms if needed, 
            # likely Title case is enough: "Lead Magnet", "Cta" -> "Cta" might want "CTA".
            # For "CTA", title() makes it "Cta". Let's handle generic cases first.
            
            # Fix specific acronyms if necessary
            df[col] = df[col].replace({'Cta': 'CTA', 'Sdr': 'SDR'}, regex=False)
        
    # If using 'usuario' as index for display
    if 'usuario' in df.columns:
         # Handle duplicates if same user appears multiple times? 
         # The ETL process used normalized name as merge key but kept original names.
         # For display, we can just use default index or set usuario if unique.
         pass
         
    return df
