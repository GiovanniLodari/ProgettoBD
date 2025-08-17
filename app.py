
"""
Applicazione principale per l'analisi dei disastri naturali
"""

import streamlit as st
import sys
import os
from pathlib import Path

# Aggiungi il path src al PYTHONPATH
current_dir = Path(__file__).parent
src_path = current_dir / "src"
sys.path.insert(0, str(src_path))

from src.config import Config
from src.spark_manager import cleanup_spark
import logging

# Configurazione logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurazione Streamlit
st.set_page_config(
    page_title=Config.PAGE_TITLE,
    page_icon=Config.PAGE_ICON,
    layout=Config.LAYOUT,
    initial_sidebar_state="expanded"
)

# Import delle pagine
try:
    from pages.home import show_home_page
    from pages.data_explorer import show_data_explorer
    from pages.analytics_page import show_analytics_page
    from pages.custom_query import show_custom_query_page
except ImportError as e:
    st.error(f"Errore nell'import delle pagine: {str(e)}")
    st.stop()

def main():
    """Funzione principale dell'applicazione"""
    
    # Sidebar per navigazione
    st.sidebar.title("🧭 Navigazione")
    
    pages = {
        "🏠 Home": "home",
        "📊 Esplora Dataset": "explorer", 
        "📈 Analisi Avanzate": "analytics",
        "🔍 Query Personalizzate": "query"
    }
    
    # Selezione pagina
    selected_page = st.sidebar.selectbox(
        "Seleziona una pagina:",
        list(pages.keys())
    )
    
    page_key = pages[selected_page]
    
    # Informazioni nella sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ℹ️ Informazioni")
    st.sidebar.markdown("""
    **Tecnologie utilizzate:**
    - Apache Spark per elaborazione dati
    - Streamlit per interfaccia web
    - Plotly per visualizzazioni
    
    **Formati supportati:**
    - CSV, JSON, Parquet
    - File compressi (.gz)
    """)
    
    # Mostra la pagina selezionata
    try:
        if page_key == "home":
            show_home_page()
        elif page_key == "explorer":
            show_data_explorer()
        elif page_key == "analytics":
            show_analytics_page()
        elif page_key == "query":
            show_custom_query_page()
    except Exception as e:
        st.error(f"Errore nel caricamento della pagina: {str(e)}")
        logger.error(f"Errore pagina {page_key}: {str(e)}")
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    <div style='text-align: center; color: gray; font-size: 0.8em;'>
    🌪️ Disaster Analysis App<br>
    Powered by Apache Spark & Streamlit
    </div>
    """, unsafe_allow_html=True)

def cleanup():
    """Pulizia risorse all'uscita"""
    try:
        cleanup_spark()
    except Exception as e:
        logger.error(f"Errore nella pulizia: {str(e)}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        st.write("Applicazione interrotta dall'utente")
        cleanup()
    except Exception as e:
        st.error(f"Errore critico: {str(e)}")
        logger.error(f"Errore critico: {str(e)}")
        cleanup()
    finally:
        # Cleanup automatico quando possibile
        cleanup()