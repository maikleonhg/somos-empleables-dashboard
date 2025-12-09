def calculate_kpis(df_pipeline_filtered, daily_conversations):
    """
    Calculates scalar KPI values and conversion rates.
    """
    # Calculate Totals for KPIs
    total_conv_val = daily_conversations['Conversaciones Activas'].sum()
    total_agendados_val = len(df_pipeline_filtered) # Use pipeline count for accuracy with filters
    total_contratados_val = len(df_pipeline_filtered[df_pipeline_filtered['contrata programa'] == 'Sí'])
    avg_dias_cierre = df_pipeline_filtered['Dias Cierre'].mean() if not df_pipeline_filtered.empty else 0

    # Rates
    rate_conv_agendados = (total_agendados_val / total_conv_val * 100) if total_conv_val > 0 else 0
    rate_cierre_contratados = (total_contratados_val / total_agendados_val * 100) if total_agendados_val > 0 else 0
    
    return {
        "total_conv_val": total_conv_val,
        "total_agendados_val": total_agendados_val,
        "total_contratados_val": total_contratados_val,
        "avg_dias_cierre": avg_dias_cierre,
        "rate_conv_agendados": rate_conv_agendados,
        "rate_cierre_contratados": rate_cierre_contratados
    }
