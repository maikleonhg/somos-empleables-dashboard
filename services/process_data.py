import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

# Define paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_OCT_DIR = os.path.join(BASE_DIR, 'Data Oct')
DATA_OUT_DIR = os.path.join(BASE_DIR, 'Data')

if not os.path.exists(DATA_OUT_DIR):
    os.makedirs(DATA_OUT_DIR)

def normalize_profession_gender(prof):
    if not isinstance(prof, str):
        return prof, 'Desconocido'
    
    prof_lower = prof.lower()
    gender = 'Masculino'
    
    # Simple inference
    first_word = prof_lower.split(' ')[0]
    if first_word.endswith('a') and first_word not in ['analista', 'periodista', 'artista']: 
            gender = 'Femenino'
    
    # Normalize "Ingeniera" -> "Ingeniero"
    prof_norm = prof.replace('Ingeniera', 'Ingeniero').replace('ingeniera', 'ingeniero')
    
    return prof_norm, gender

def normalize_name(name):
    if not isinstance(name, str):
        return name
    replacements = (
        ("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"),
        ("Á", "A"), ("É", "E"), ("Í", "I"), ("Ó", "O"), ("Ú", "U")
    )
    name_norm = name.strip()
    for a, b in replacements:
        name_norm = name_norm.replace(a, b)
    return name_norm.lower()

def process_conversaciones():
    print("Processing Conversaciones...")
    data_points = []
    day_offsets = {'Lunes': 0, 'Martes': 1, 'Miercoles': 2, 'Jueves': 3, 'Viernes': 4}
    
    # --- Oct ---
    start_oct = datetime(2025, 9, 29)
    df_oct = pd.read_csv(os.path.join(DATA_OCT_DIR, 'Seguimiento clientes Octubre - conversaciones activas.csv'))
    
    for index, row in df_oct.iterrows():
        week_offset = index
        for day_col, day_offset in day_offsets.items():
            if day_col in df_oct.columns:
                val = row[day_col]
                current_date = start_oct + timedelta(weeks=week_offset, days=day_offset)
                if val != '-' and pd.notna(val):
                    try:
                        val_int = int(val)
                        data_points.append({'Fecha': current_date, 'Conversaciones Activas': val_int})
                    except ValueError:
                        pass

    # --- Nov ---
    # Assuming Week 1 of Nov file corresponds to week starting Oct 27 (to match seamless transition from Oct file)
    # If the user's data is strict calendar month, "Semana 1" of Nov might range Nov 1-2.
    # But usually these tracking sheets are weekly.
    # Let's align on Mon Oct 27 for Week 1 of Nov file.
    start_nov = datetime(2025, 10, 27) 
    df_nov = pd.read_csv(os.path.join(DATA_OCT_DIR, 'Seguimiento clientes Noviembre - conversaciones activas.csv'))
    
    for index, row in df_nov.iterrows():
        week_offset = index
        for day_col, day_offset in day_offsets.items():
            if day_col in df_nov.columns:
                val = row[day_col]
                current_date = start_nov + timedelta(weeks=week_offset, days=day_offset)
                if val != '-' and pd.notna(val):
                    try:
                        val_int = int(val)
                        data_points.append({'Fecha': current_date, 'Conversaciones Activas': val_int})
                    except ValueError:
                        pass
                        
    df = pd.DataFrame(data_points)
    if not df.empty:
        df = df.sort_values('Fecha').drop_duplicates(subset=['Fecha'])
    
    out_path = os.path.join(DATA_OUT_DIR, 'conversaciones_completo.csv')
    df.to_csv(out_path, index=False)
    print(f"Saved {out_path}")

def process_pipeline():
    print("Processing Pipeline...")
    # --- 1. Oct Data Preparation ---
    agenda_path = os.path.join(DATA_OCT_DIR, 'Seguimiento clientes Octubre - agenda.csv')
    df_oct_agenda = pd.read_csv(agenda_path)
    df_oct_agenda['fecha agenda'] = pd.to_datetime(df_oct_agenda['fecha agenda'], format='%m/%d/%Y')
    df_oct_agenda['merge_key'] = df_oct_agenda['usuario'].apply(normalize_name)
    
    contratados_path = os.path.join(DATA_OCT_DIR, 'Seguimiento clientes Octubre - contratados.csv')
    df_oct_contratados = pd.read_csv(contratados_path)
    df_oct_contratados['contrata programa'] = 'Sí'
    df_oct_contratados['merge_key'] = df_oct_contratados['usuario'].apply(normalize_name)
    
    retirados_path = os.path.join(DATA_OCT_DIR, 'Seguimiento clientes Octubre - leads retirados.csv')
    df_oct_retirados = pd.read_csv(retirados_path)
    df_oct_retirados['merge_key'] = df_oct_retirados['usuario'].apply(normalize_name)
    
    # Merge Oct
    df_oct_pipeline = df_oct_agenda.merge(
        df_oct_contratados[['merge_key', 'contrata programa', 'Fecha Ingreso']], 
        on='merge_key', how='left'
    ).merge(
        df_oct_retirados[['merge_key', 'Motivo por el que no continua']], 
        on='merge_key', how='left'
    )
    df_oct_pipeline = df_oct_pipeline.drop(columns=['merge_key'])
    
    # Fill Oct NaNs
    df_oct_pipeline['contrata programa'] = df_oct_pipeline['contrata programa'].fillna('No')
    df_oct_pipeline['Motivo por el que no continua'] = df_oct_pipeline['Motivo por el que no continua'].fillna('-')
    
    # --- 2. Nov Data Preparation ---
    nov_path = os.path.join(DATA_OCT_DIR, 'Seguimiento clientes Noviembre - agenda.csv')
    df_nov = pd.read_csv(nov_path)
    
    # Map Columns
    df_nov = df_nov.rename(columns={
        'fecha': 'fecha agenda',
        'estado lead': 'estado',
        'Medio Contacto': 'medio contacto'
    })
    
    # Standardize Dates
    df_nov['fecha agenda'] = pd.to_datetime(df_nov['fecha agenda'], format='%m/%d/%Y', errors='coerce')
    
    # Logic for Outcomes
    df_nov['contrata programa'] = df_nov['Acción Final'].apply(lambda x: 'Sí' if isinstance(x, str) and 'Contrata' in x else 'No')
    
    # Logic for Motivo
    # If Motivo Retiro is filled, use it.
    df_nov['Motivo por el que no continua'] = df_nov['Motivo Retiro'].fillna('-')
    
    # Logic for Fecha Ingreso
    # Absent in Nov file. Leave as NaN or use agenda date if hired?
    # Users usually track this. For MVP, we pass whatever we have.
    df_nov['Fecha Ingreso'] = None
    
    # Ensure columns match
    cols_of_interest = [
        'usuario', 'fecha agenda', 'estado', 'medio contacto', 
        'profesión/formación', 'contrata programa', 'Fecha Ingreso', 'Motivo por el que no continua'
    ]
    
    # Select only matching columns for concat, fill missing if needed
    for col in cols_of_interest:
        if col not in df_nov.columns:
            df_nov[col] = None
        if col not in df_oct_pipeline.columns:
            df_oct_pipeline[col] = None
            
    # Normalize Profession/Gender for both
    # (Actually simpler to do it on the combined dict but let's do it now)
    
    # --- 3. Combine ---
    df_full = pd.concat([df_oct_pipeline[cols_of_interest], df_nov[cols_of_interest]], ignore_index=True)
    
    # Normalize Gender/Prof on full dataset
    df_full[['profesión/formación', 'Genero']] = df_full['profesión/formación'].apply(
        lambda x: pd.Series(normalize_profession_gender(x))
    )
    
    # Normalize Dates
    df_full['fecha agenda'] = pd.to_datetime(df_full['fecha agenda'])
    
    # Fix Fecha Ingreso format (some are D-M-Y from Oct file CSV, some are None)
    # Oct file had strings probably? pd.read_csv might have inferred or kept string.
    # In etl.py was: pd.to_datetime(..., format='%d/%m/%Y')
    # So we should enforce that partial parsing.
    
    # We'll just save it as is; etl.py/app.py will handle loaded types if we keep logic or we pre-process here.
    # Let's pre-process "Dias Cierre" here to save computation?
    # The user asked for "df ya completos" (complete DFs).
    
    df_full['Fecha Ingreso DT'] = pd.to_datetime(df_full['Fecha Ingreso'], dayfirst=True, errors='coerce')
    
    # Recalculate Dias Cierre
    df_full['Dias Cierre'] = (df_full['Fecha Ingreso DT'] - df_full['fecha agenda']).dt.days
    
    out_path = os.path.join(DATA_OUT_DIR, 'pipeline_completo.csv')
    df_full.to_csv(out_path, index=False)
    print(f"Saved {out_path}")

if __name__ == "__main__":
    process_conversaciones()
    process_pipeline()
