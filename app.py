
"""
Applicazione principale per l'analisi dei disastri naturali
"""

import streamlit as st
import sys
import os
from pathlib import Path

current_dir = Path(__file__).parent
src_path = current_dir / "src"
sys.path.insert(0, str(src_path))

from src.config import Config
from src.spark_manager import cleanup_spark
import logging
from pages_logic.home import main as home_main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title=Config.PAGE_TITLE,
    page_icon=Config.PAGE_ICON,
    layout=Config.LAYOUT
)

def main():
    """Funzione principale dell'applicazione"""
           
    # st.sidebar.markdown("---")
    # st.sidebar.markdown("""
    # <div style='text-align: center; color: gray; font-size: 0.8em;'>
    # 🌪️ Disaster Analysis App<br>
    # Powered by Apache Spark & Streamlit
    # </div>
    # """, unsafe_allow_html=True)

    home_main()

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
        #cleanup()
    except Exception as e:
        st.error(f"Errore critico: {str(e)}")
        logger.error(f"Errore critico: {str(e)}")
        #cleanup()
    finally:
        cleanup()