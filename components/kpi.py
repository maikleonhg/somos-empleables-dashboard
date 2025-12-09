import streamlit as st

def render_kpi(df, campo, titulo, key, grafica="linea", prefijo="", total_override=None, delta_override=None, delta_label=None):
    """
    Crea y muestra una métrica de Streamlit con su variación y un mini-gráfico.
    """
    if df.empty:
        with st.container():
            st.metric(label=titulo, value=f"{prefijo}0")
        return

    # Determine Main Value
    if total_override is not None:
        UltDato = total_override
    else:
        UltDato = df.iloc[-1][campo]
    
    # Calculate Variation or Use Override
    if delta_override is not None:
        if isinstance(delta_override, (int, float)):
            variacion_str = f"{delta_override:.1f}%"
        else:
            variacion_str = str(delta_override)
        
        if delta_label:
             variacion_str = f"{variacion_str} {delta_label}"
    else:
        # Default: Last vs Penultimate
        if len(df) > 1:
            LastDaily = df.iloc[-1][campo]
            PrevDaily = df.iloc[-2][campo]
            variacion = (LastDaily - PrevDaily) / PrevDaily if PrevDaily != 0 else 0
            variacion_str = f"{variacion:.2%} (vs ayer)"
        else:
            variacion_str = None
        
    # Sparkline Data
    arrDatosVentas = df[campo].to_list()
    
    # Container with specific key for CSS targeting
    with st.container():
        st.metric(label=titulo, value=f"{prefijo}{UltDato:,.0f}", delta=variacion_str, chart_data=arrDatosVentas, delta_color="normal", help=titulo)
