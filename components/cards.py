import streamlit as st

def load_style(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

def render_error(message, error):
    with st.expander(f"⚠️ {message}"):
        st.error(str(error))

def render_chart_card(title, fig, key=None):
    """
    Renders a chart within a styled card container (using Markdown HTML wrapper).
    This ensures consistent height and alignment as per CSS 'chart-card'.
    """
    st.markdown(f'''
    <div class="chart-card">
        <h3 style="margin-top:0; padding-top:0;">{title}</h3>
    ''', unsafe_allow_html=True)
    
    if fig:
        st.plotly_chart(fig, width="stretch", key=key)
    else:
        st.info("Sin datos disponibles")
    st.markdown('</div>', unsafe_allow_html=True)
