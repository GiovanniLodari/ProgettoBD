"""
Analizzatore Disastri Naturali con Spark Integration
Versione semplificata per focus su Custom Query
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime

import logging
import sys
import traceback

# Import delle utilities Spark
from src.spark_manager import SparkManager, should_use_spark, get_file_size_mb, detect_data_schema
from src.data_loader import DataLoader, FileHandler
from src.analytics import DisasterAnalytics
from pyspark.sql import DataFrame as SparkDataFrame
from pages_logic.custom_query import show_simplified_custom_query_page

# Configurazione del Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Configurazione pagina - RIMUOVE LA SIDEBAR
st.set_page_config(
    page_title="Disaster Analytics - Spark Edition",
    page_icon="🌪️",
    layout="wide"
)

# CSS ottimizzato - nasconde completamente la sidebar
st.markdown("""
<style>
    .stApp > header {visibility: hidden;}
    .main .block-container {padding-top: 1rem;}
    
    /* Nasconde completamente la sidebar */
    section[data-testid="stSidebar"] {
        display: none !important;
    }
    
    /* Assicura che il contenuto principale occupi tutto lo spazio */
    .main {
        margin-left: 0 !important;
    }
    .stButton button {
        margin-top: 2.4rem;
        margin-left: 1.5rem;
    }
</style>
""", unsafe_allow_html=True)

def main():
    """Applicazione principale semplificata"""
    logger.info("Avvio funzione main dell'applicazione Streamlit.")

    try:
        if 'spark_manager' not in st.session_state:
            st.session_state.spark_manager = SparkManager()
            logger.debug("Inizializzato SparkManager in session_state.")
    except Exception as e:
        logger.critical(f"Inizializzazione di SparkManager fallita: {e}", exc_info=True)
        st.error(f"Errore critico con Spark. L'applicazione non può continuare. Dettagli: {e}")
        st.stop()

    st.title("🌪️ Disaster Analytics - Spark Edition")
    st.markdown("**High-Performance Multi-Dataset Analysis**")
    
    if 'datasets_loaded' not in st.session_state:
        logger.info("Inizializzazione dello stato della sessione.")
        st.session_state.datasets_loaded = False
        st.session_state.data = None
        st.session_state.load_info = None
        st.session_state.is_spark = False
        st.session_state.performance_info = None

    with st.container():
        col1, col2 = st.columns([3, 1])
        
        with col1:
            uploaded_files = st.file_uploader(
                label="Upload your dataset here",
                type=['json.gz', 'parquet'],
                accept_multiple_files=True
            )
        
        with col2:
            if st.button("🔄 Reset"):
                logger.info("Resetting session state.")
                
                if 'spark_manager' in st.session_state:
                    logger.debug("Pulizia della sessione Spark in corso.")
                    st.session_state.spark_manager.cleanup()
                
                for key in ['datasets_loaded', 'data', 'load_info', 'is_spark', 'performance_info']:
                    if key in st.session_state:
                        del st.session_state[key]
                        logger.debug(f"Chiave '{key}' rimossa da session_state.")
                
                st.rerun()
    
    if uploaded_files:
        if not st.session_state.datasets_loaded:
            logger.info("Avvio del processo di caricamento dei file.")
            total_size_mb = get_file_size_mb(uploaded_files)
            use_spark = should_use_spark(total_size_mb, len(uploaded_files))
                        
            if use_spark:
                try:
                    with st.spinner(f"Initializing Spark Session..."):
                        logger.info("Tentativo di inizializzazione della sessione Spark.")
                        spark = st.session_state.spark_manager.get_spark_session(total_size_mb)

                        if not spark:
                            raise RuntimeError("La sessione Spark non è stata inizializzata correttamente (restituito None).")

                        logger.info("Sessione Spark inizializzata. Decoompressione dei file in corso.")
                        st.session_state.data_loader = DataLoader(spark)

                    with st.spinner(f"Decompressing {len(uploaded_files)} files..."):
                        temp_paths = []
                        progress_bar = st.progress(0)
                        for idx, uploaded_file in enumerate(uploaded_files):
                            path = FileHandler.handle_uploaded_file(uploaded_file)
                            progress = int((idx+1)/len(uploaded_files) * 100)
                            progress_bar.progress(progress, text=f"Decompressed {idx+1} of {len(uploaded_files)} files")
                            if path:
                                temp_paths.append(path)
                        progress_bar.empty()

                    combined_data = st.session_state.data_loader.load_multiple_files(
                        file_paths=temp_paths,
                        schema_type='twitter'
                    )

                except Exception as e:
                    logger.error(f"Caricamento con Spark fallito. Errore: {e}", exc_info=True)
                    st.error(f"❌ Caricamento con Spark fallito. L'app tenterà di continuare con Pandas.")

                else:
                    if not isinstance(combined_data, SparkDataFrame):
                        logger.error("Conversione del DataFrame Pandas in DataFrame Spark.")
                        try:
                            spark_df = spark.createDataFrame(combined_data)
                            combined_data = spark_df
                        except Exception as e:
                            logger.error(f"Errore nella conversione del DataFrame: {e}", exc_info=True)
                            return None
                        
                with st.spinner('🧹 Cleaning the dataset...'):
                    cleaned_data = DataLoader.clean_twitter_dataframe(combined_data)

                    if cleaned_data:
                        logger.info("Pulizia del DataFrame riuscita.")
                        st.session_state.data = cleaned_data
                        st.session_state.datasets_loaded = True
                        st.session_state.is_spark = True
                        st.session_state.performance_info = {
                            'cores': spark.sparkContext.defaultParallelism,
                            'memory': "Dynamic"
                        }

                        final_record_count = st.session_state.data.count()
                        st.success(f"✅ Ready to analyze: {final_record_count:,} records from {len(uploaded_files)} files")
                    else:
                        logger.warning("La pulizia è fallita. Si procede con i dati non puliti.")
                        st.error("❌ Cleaning failed. Using raw data. Check app.log for details.")
                        st.session_state.data = combined_data
                        st.session_state.is_spark = True
                        st.session_state.datasets_loaded = True
                        st.session_state.performance_info = {
                            'cores': spark.sparkContext.defaultParallelism,
                            'memory': "Dynamic",
                            'engine': 'Apache Spark'
                        }
                
            if not use_spark:
                logger.info("Caricamento con Pandas in corso.")
                all_dfs = []
                load_info = []
                for file in uploaded_files:
                    df = pd.read_csv(file)
                    all_dfs.append(df)
                    load_info.append({'name': file.name, 'size_mb': file.size / 1024 / 1024, 'rows': len(df)})
                combined_data = pd.concat(all_dfs, ignore_index=True)
                st.session_state.data = combined_data
                st.session_state.load_info = load_info
                st.session_state.is_spark = False
                st.session_state.datasets_loaded = True
                logger.info(f"Dati caricati con Pandas. Totale record: {len(combined_data):,}.")
                st.success(f"✅ Loaded: {len(combined_data):,} records from {len(load_info)} files")
    
    if st.session_state.datasets_loaded and st.session_state.data is not None:
        show_simplified_custom_query_page()
    else:
        st.info("👆 Upload your disaster datasets above to start the analysis")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        tb_str = traceback.format_exc()
        logger.critical(f"ERRORE NON GESTITO A LIVELLO GLOBALE: {e}\nTRACEBACK:\n{tb_str}")
        st.error(f"Si è verificato un errore critico nell'applicazione: {e}")
        with st.expander("Dettagli Tecnici dell'Errore (dal log)"):
            st.code(tb_str)
        if st.button("🔄 Riavvia l'Applicazione"):
            st.rerun()