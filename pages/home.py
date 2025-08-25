"""
Analizzatore Disastri Naturali con Spark Integration
Versione ottimizzata per dataset di grandi dimensioni
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime
import io
import tempfile
import shutil
import logging
import sys
import traceback

# Import delle utilities Spark (presumendo che esistano i file src/spark_manager.py e src/analytics.py)
from src.spark_manager import SparkManager, should_use_spark, get_file_size_mb, detect_data_schema
from src.data_loader import DataLoader, FileHandler 
from src.analytics import DisasterAnalytics

# --- Configurazione del Logger ---
# Il logger è configurato per scrivere su console (per Streamlit) e su un file
logging.basicConfig(
    level=logging.DEBUG,  # Imposta il livello di log minimo a DEBUG per catturare tutto
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("app.log", mode="w", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)
logger.info("Configurazione del logger completata. Livello di log impostato su DEBUG.")

# Configurazione pagina
st.set_page_config(
    page_title="Disaster Analytics - Spark Edition",
    page_icon="🌪️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS ottimizzato
st.markdown("""
<style>
    .stApp > header {visibility: hidden;}
    .main .block-container {padding-top: 1rem;}
    .engine-indicator {
        background: linear-gradient(90deg, #ff6b6b, #4ecdc4);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: bold;
        display: inline-block;
        margin: 0.5rem 0;
    }
    .performance-metrics {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #4ecdc4;
    }
</style>
""", unsafe_allow_html=True)

def load_sample_data():
    """Genera dati di esempio più grandi per testare Spark"""
    logger.info("Generazione di dati di esempio in corso...")
    np.random.seed(42)
    
    disaster_types = ['Earthquake', 'Flood', 'Hurricane', 'Wildfire', 'Tornado', 'Tsunami', 'Drought', 'Avalanche']
    countries = ['Italy', 'USA', 'Japan', 'Australia', 'Germany', 'Brazil', 'India', 'China', 'Mexico', 'Turkey']
    
    n_records = 50000
    
    data = {
        'disaster_id': range(1, n_records + 1),
        'type': np.random.choice(disaster_types, n_records),
        'country': np.random.choice(countries, n_records),
        'date': pd.date_range('2015-01-01', '2024-12-31', periods=n_records),
        'magnitude': np.random.uniform(1, 9, n_records).round(1),
        'casualties': np.random.poisson(50, n_records),
        'economic_loss': np.random.lognormal(15, 2, n_records).astype(int),
        'duration_days': np.random.exponential(5, n_records).astype(int) + 1,
        'affected_population': np.random.exponential(10000, n_records).astype(int),
        'recovery_time_months': np.random.gamma(2, 3, n_records).round().astype(int)
    }
    
    df = pd.DataFrame(data)
    logger.info(f"Generazione dati di esempio completata. Creato un DataFrame di {len(df):,} record.")
    return df

def display_engine_info(engine_type, file_info=None, performance_info=None):
    """Mostra informazioni sul motore di elaborazione utilizzato"""
    logger.debug(f"Visualizzazione informazioni motore: {engine_type}")
    
    if engine_type == "spark":
        st.markdown('<div class="engine-indicator">⚡ Spark Engine Active</div>', unsafe_allow_html=True)
        logger.info("Spark Engine in uso.")
        
        if performance_info:
            logger.debug(f"Visualizzazione metriche performance Spark: {performance_info}")
            with st.container():
                st.markdown('<div class="performance-metrics">', unsafe_allow_html=True)
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Cores Used", performance_info.get('cores', 'N/A'))
                with col2:
                    st.metric("Memory Allocated", performance_info.get('memory', 'N/A'))
                with col3:
                    st.metric("Processing Mode", "Distributed")
                with col4:
                    st.metric("Engine", "Apache Spark")
                
                st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div style="background: #ffd93d; color: #333; padding: 0.5rem 1rem; border-radius: 20px; display: inline-block;">🐼 Pandas Engine (Small Data)</div>', unsafe_allow_html=True)
        logger.info("Pandas Engine in uso.")

def create_overview_metrics_spark(df_or_spark, is_spark=False, load_info=None):
    """Metriche overview ottimizzate per Spark"""
    logger.info("Calcolo delle metriche di overview.")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if is_spark:
            logger.debug("Conteggio eventi totali con Spark.")
            total_events = df_or_spark.count()
        else:
            logger.debug("Conteggio eventi totali con Pandas.")
            total_events = len(df_or_spark)
        
        st.metric("Total Events", f"{total_events:,}")
        if load_info:
            st.caption(f"From {len(load_info)} files")
    
    with col2:
        if is_spark:
            if 'casualties' in df_or_spark.columns:
                logger.debug("Calcolo 'Avg Casualties' con Spark.")
                from pyspark.sql.functions import avg
                avg_casualties = df_or_spark.agg(avg('casualties')).collect()[0][0]
                st.metric("Avg Casualties", f"{avg_casualties:.0f}" if avg_casualties else "N/A")
            else:
                logger.warning("Colonna 'casualties' non trovata in Spark DataFrame.")
                st.metric("Datasets", f"{df_or_spark.select('source_id').distinct().count()}")
        else:
            if 'casualties' in df_or_spark.columns:
                logger.debug("Calcolo 'Avg Casualties' con Pandas.")
                avg_casualties = df_or_spark['casualties'].mean()
                st.metric("Avg Casualties", f"{avg_casualties:.0f}")
            else:
                logger.warning("Colonna 'casualties' non trovata in Pandas DataFrame.")
                st.metric("Datasets", f"{df_or_spark['source_id'].nunique() if 'source_id' in df_or_spark.columns else 1}")
    
    with col3:
        if is_spark and 'economic_loss' in df_or_spark.columns:
            logger.debug("Calcolo 'Total Loss' con Spark.")
            from pyspark.sql.functions import sum as spark_sum
            total_loss = df_or_spark.agg(spark_sum('economic_loss')).collect()[0][0]
            if total_loss:
                st.metric("Total Loss", f"${total_loss/1e9:.1f}B")
            else:
                logger.warning("Valore 'Total Loss' nullo.")
                st.metric("Total Loss", "N/A")
        elif not is_spark and 'economic_loss' in df_or_spark.columns:
            logger.debug("Calcolo 'Total Loss' con Pandas.")
            total_loss = df_or_spark['economic_loss'].sum() / 1e9
            st.metric("Total Loss", f"${total_loss:.1f}B")
        else:
            logger.warning("Colonna 'economic_loss' non trovata.")
            st.metric("Columns", f"{len(df_or_spark.columns)}")
    
    with col4:
        if is_spark and 'date' in df_or_spark.columns:
            logger.debug("Calcolo 'Period' con Spark.")
            from pyspark.sql.functions import max, min, datediff
            date_stats = df_or_spark.agg(max('date').alias('max_date'), min('date').alias('min_date')).collect()[0]
            if date_stats['max_date'] and date_stats['min_date']:
                date_range = (date_stats['max_date'] - date_stats['min_date']).days
                st.metric("Period (days)", f"{date_range:,}")
            else:
                logger.warning("Valori di data nulli o mancanti.")
                st.metric("Period", "N/A")
        elif not is_spark and 'date' in df_or_spark.columns:
            logger.debug("Calcolo 'Period' con Pandas.")
            date_range = (df_or_spark['date'].max() - df_or_spark['date'].min()).days
            st.metric("Period (days)", f"{date_range:,}")
        else:
            logger.warning("Colonna 'date' non trovata.")
            st.metric("Total Rows", f"{total_events:,}")
    logger.info("Calcolo metriche di overview completato.")

def create_charts_from_spark(spark_df):
    """Crea grafici ottimizzati usando aggregazioni Spark"""
    logger.info("Avvio della creazione dei grafici con Spark.")
    charts = {}

    analytics = DisasterAnalytics(spark_df)

    # 1. Distribuzione per tipo
    if 'type' in spark_df.columns:
        logger.debug("Analisi della distribuzione per tipo di disastro.")
        type_counts = analytics.analyze_by_disaster_type()

        if type_counts is not None and not type_counts.empty:
            logger.debug("Dati per il grafico 'type_distribution' pronti.")
            fig = px.bar(
                type_counts.head(10),
                x='count',
                y='type',
                orientation='h',
                title="Top 10 Events by Type",
                color='count',
                color_continuous_scale="viridis"
            )
            fig.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
            charts['type_distribution'] = fig
            logger.debug("Grafico 'type_distribution' creato con successo.")
        else:
            logger.warning("Nessun dato valido per il grafico di distribuzione per tipo.")

    # 2. Distribuzione geografica per paese
    if 'country' in spark_df.columns:
        logger.debug("Analisi della distribuzione geografica.")
        country_counts = analytics.analyze_geographical_distribution()
        if country_counts is not None and not country_counts.empty:
            logger.debug("Dati per il grafico 'country_distribution' pronti.")
            fig = px.choropleth(
                country_counts,
                locations='country',
                locationmode='country names',
                color='disaster_count',
                hover_name='country',
                color_continuous_scale=px.colors.sequential.Plasma,
                title="Disaster Events by Country"
            )
            charts['country_distribution'] = fig
            logger.debug("Grafico 'country_distribution' creato con successo.")
        else:
            logger.warning("Nessun dato valido per il grafico di distribuzione geografica.")

    # 3. Trend temporale
    if 'date' in spark_df.columns:
        logger.debug("Analisi del trend temporale.")
        temporal_trends_df = analytics.analyze_temporal_trends()
        if temporal_trends_df is not None and not temporal_trends_df.empty:
            logger.debug("Dati per il grafico 'temporal_trend' pronti.")
            fig = px.line(
                temporal_trends_df,
                x='year',
                y='disaster_count',
                title="Temporal Trend of Disasters"
            )
            fig.update_layout(height=400)
            charts['temporal_trend'] = fig
            logger.debug("Grafico 'temporal_trend' creato con successo.")
        else:
            logger.warning("Nessun dato valido per il grafico del trend temporale.")

    # 4. Distribuzione del costo economico
    if 'economic_loss' in spark_df.columns:
        logger.debug("Analisi della distribuzione del costo economico.")
        economic_loss_summary = spark_df.select('economic_loss').toPandas()
        if not economic_loss_summary.empty:
            logger.debug("Dati per il grafico 'economic_loss_distribution' pronti.")
            fig = px.histogram(
                economic_loss_summary,
                x='economic_loss',
                title="Distribution of Economic Loss",
                color_discrete_sequence=['#4ecdc4']
            )
            fig.update_layout(bargap=0.2, height=400)
            charts['economic_loss_distribution'] = fig
            logger.debug("Grafico 'economic_loss_distribution' creato con successo.")
        else:
            logger.warning("Nessun dato valido per il grafico di distribuzione dei costi economici.")
            
    logger.info("Creazione grafici completata.")
    return charts

def main():
    """Applicazione principale con supporto Spark"""
    logger.info("Avvio funzione main dell'applicazione Streamlit.")

    try:
        # Inizializza Spark Manager
        if 'spark_manager' not in st.session_state:
            st.session_state.spark_manager = SparkManager()
            logger.debug("Inizializzato SparkManager in session_state.")
        else:
            logger.debug("SparkManager già presente in session_state.")
    except Exception as e:
        logger.critical(f"Inizializzazione di SparkManager fallita: {e}", exc_info=True)
        st.error(f"Errore critico con Spark. L'applicazione non può continuare. Dettagli: {e}")
        st.stop()


    # Header
    st.title("🌪️ Disaster Analytics - Spark Edition")
    st.markdown("**High-Performance Multi-Dataset Analysis**")
    
    # Inizializza session state
    if 'datasets_loaded' not in st.session_state:
        logger.info("Inizializzazione dello stato della sessione.")
        st.session_state.datasets_loaded = False
        st.session_state.data = None
        st.session_state.load_info = None
        st.session_state.is_spark = False
        st.session_state.performance_info = None

    logger.debug("Hola.")
    
    # Upload section
    with st.container():
        col1, col2 = st.columns([3, 1])
        
        with col1:
            uploaded_files = st.file_uploader(
                "Upload datasets (CSV, JSON, JSON.GZ, Parquet) - Auto-optimized with Spark for large files",
                type=['csv', 'json', 'gz', 'parquet'],
                accept_multiple_files=True,
                help="Files >100MB or multiple files will automatically use Spark for processing"
            )
        
        with col2:
            use_sample = st.checkbox("Use sample data", value=not uploaded_files)
            if st.button("🔄 Reset"):
                logger.info("Bottone 'Reset' premuto. Resetting session state.")
                
                if 'spark_manager' in st.session_state:
                    logger.debug("Pulizia della sessione Spark in corso.")
                    st.session_state.spark_manager.cleanup()
                
                for key in ['datasets_loaded', 'data', 'load_info', 'is_spark', 'performance_info']:
                    if key in st.session_state:
                        del st.session_state[key]
                        logger.debug(f"Chiave '{key}' rimossa da session_state.")
                
                st.rerun()
    
    # Mostra decisione engine
    if uploaded_files:
        total_size_mb = get_file_size_mb(uploaded_files)
        will_use_spark = should_use_spark(total_size_mb, len(uploaded_files))
        logger.info(f"Caricati {len(uploaded_files)} file, totale {total_size_mb:.1f}MB. "
                    f"Decisione engine: {'Spark' if will_use_spark else 'Pandas'}")

        if will_use_spark:
            st.info(f"🚀 **Auto-optimization**: {total_size_mb:.1f}MB across {len(uploaded_files)} files → **Spark Engine** will be used")
        else:
            st.info(f"🐼 **Small dataset**: {total_size_mb:.1f}MB → **Pandas Engine** will be used")
    
    # Process data
    if uploaded_files or use_sample:
        if uploaded_files and not st.session_state.datasets_loaded:
            logger.info("Avvio del processo di caricamento dei file.")
            total_size_mb = get_file_size_mb(uploaded_files)
            use_spark = should_use_spark(total_size_mb, len(uploaded_files))
            logger.debug(f"Processo file uploadati con {'Spark' if use_spark else 'Pandas'}.")
            
            with st.spinner(f"Loading {len(uploaded_files)} files with {'Spark' if use_spark else 'Pandas'}..."):
                if use_spark:
                    try:
                        # 1. Il blocco 'try' contiene SOLO le operazioni rischiose
                        logger.info("Tentativo di inizializzazione della sessione Spark.")
                        spark = st.session_state.spark_manager.get_spark_session(total_size_mb)

                        # Se la sessione non parte, solleva un errore per andare all'except
                        if not spark:
                            raise RuntimeError("La sessione Spark non è stata inizializzata correttamente (restituito None).")

                        logger.info("Sessione Spark OK. Lettura dei file in corso.")
                        #combined_data, load_info = st.session_state.spark_manager.read_files_with_spark(uploaded_files)
                        st.session_state.data_loader = DataLoader(spark)

                        temp_paths = []
                        for uploaded_file in uploaded_files:
                            path = FileHandler.handle_uploaded_file(uploaded_file)
                            if path:
                                temp_paths.append(path)

                        combined_data = st.session_state.data_loader.load_multiple_files(
                            file_paths=temp_paths,
                            schema_type='twitter'
                        )

                    except Exception as e:
                        # 2. Il blocco 'except' gestisce QUALSIASI fallimento avvenuto nel 'try'
                        logger.error(f"Caricamento con Spark fallito. Errore: {e}", exc_info=True)
                        st.error(f"❌ Caricamento con Spark fallito. L'app tenterà di continuare con Pandas.")
                        st.info("Controlla il file app.log per i dettagli tecnici dell'errore.")
                        # La variabile rimane False

                    else:
                        # 3. Il blocco 'else' viene eseguito SOLO SE il 'try' ha avuto successo
                        logger.info(f"Dati caricati con Spark. Totale record: {combined_data.count()}.")
                        st.session_state.data = combined_data
                        #st.session_state.load_info = load_info
                        st.session_state.is_spark = True
                        st.session_state.datasets_loaded = True
                        st.session_state.performance_info = {
                            'cores': spark.sparkContext.defaultParallelism,
                            'memory': "Dynamic",
                            'engine': 'Apache Spark'
                        }
                        st.success(f"✅ Loaded with Spark: {combined_data.count():,} records from {len(uploaded_files)} files")
                        spark_successful = True # Imposta il flag di successo
                
                if not use_spark:
                    logger.info("Caricamento con Pandas in corso.")
                    # Fallback a Pandas (codice originale semplificato)
                    all_dfs = []
                    load_info = []
                    for file in uploaded_files:
                        df = pd.read_csv(file) # Esempio semplificato
                        all_dfs.append(df)
                        load_info.append({'name': file.name, 'size_mb': file.size / 1024 / 1024, 'rows': len(df)})
                    combined_data = pd.concat(all_dfs, ignore_index=True)
                    st.session_state.data = combined_data
                    st.session_state.load_info = load_info
                    st.session_state.is_spark = False
                    st.session_state.datasets_loaded = True
                    logger.info(f"Dati caricati con Pandas. Totale record: {len(combined_data):,}.")
                    st.success(f"✅ Loaded with Pandas: {len(combined_data):,} records from {len(load_info)} files")
        
        elif use_sample and not st.session_state.datasets_loaded:
            logger.info("Opzione 'Use sample data' selezionata. Generazione dati.")
            sample_data = load_sample_data()
            
            sample_size_mb = sample_data.memory_usage(deep=True).sum() / 1024 / 1024
            
            if sample_size_mb > 50:
                logger.info(f"Dimensioni sample ({sample_size_mb:.2f}MB) sufficienti per usare Spark.")
                spark = st.session_state.spark_manager.get_spark_session(sample_size_mb)
                if spark:
                    spark_df = spark.createDataFrame(sample_data)
                    st.session_state.data = spark_df
                    st.session_state.is_spark = True
                    st.session_state.performance_info = {
                        'cores': spark.sparkContext.defaultParallelism,
                        'memory': "1GB",
                        'engine': 'Apache Spark'
                    }
                    logger.info("Convertito Pandas DataFrame in Spark DataFrame per analisi.")
                else:
                    logger.warning("Inizializzazione Spark fallita per dati di esempio. Rimane in Pandas.")
                    st.session_state.data = sample_data
                    st.session_state.is_spark = False
            else:
                logger.info("Dimensioni sample piccole. Utilizzo di Pandas.")
                st.session_state.data = sample_data
                st.session_state.is_spark = False
            
            st.session_state.datasets_loaded = True
            st.info("📊 Using sample data - upload your files above for real analysis")
    
    # Display loaded data
    if st.session_state.datasets_loaded and st.session_state.data is not None:
        logger.info("I dati sono caricati. Avvio della visualizzazione e dell'analisi.")
        
        # Engine indicator
        display_engine_info(
            "spark" if st.session_state.is_spark else "pandas",
            st.session_state.load_info,
            st.session_state.performance_info
        )
        
        # File info
        if st.session_state.load_info:
            logger.debug("Visualizzazione dei dettagli dei file caricati.")
            with st.expander(f"📁 File Details ({len(st.session_state.load_info)} files)"):
                info_df = pd.DataFrame(st.session_state.load_info)
                st.dataframe(info_df, use_container_width=True)
        
        data = st.session_state.data
        is_spark = st.session_state.is_spark
        
        create_overview_metrics_spark(data, is_spark, st.session_state.load_info)
        
        st.divider()
        
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Analysis", "🔍 Custom Query", "📈 Advanced", "💾 Export"])
        
        with tab1:
            try:
                logger.info("Passaggio al Tab 'Analysis'.")
                if is_spark:
                    logger.info("Generazione di grafici analitici con Spark.")
                    charts = create_charts_from_spark(data)
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'type_distribution' in charts:
                            st.plotly_chart(charts['type_distribution'], use_container_width=True)
                        if 'country_distribution' in charts: # Nota: il codice originale ha 'geo_distribution' ma la funzione crea 'country_distribution'
                            st.plotly_chart(charts['country_distribution'], use_container_width=True)
                    
                    with col2:
                        if 'temporal_trend' in charts:
                            st.plotly_chart(charts['temporal_trend'], use_container_width=True)
                        if 'economic_loss_distribution' in charts: # Aggiunto per coerenza
                            st.plotly_chart(charts['economic_loss_distribution'], use_container_width=True)
                    
                    if st.button("📊 Get Detailed Statistics"):
                        logger.info("Richiesta di statistiche dettagliate.")
                        with st.spinner("Computing statistics with Spark..."):
                            stats = st.session_state.spark_manager.get_basic_stats(data)
                            logger.info("Statistiche calcolate con successo.")
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.subheader("Dataset Overview")
                                st.metric("Total Rows", f"{stats['total_rows']:,}")
                                st.metric("Total Columns", stats['total_columns'])
                                
                                if 'describe' in stats:
                                    st.subheader("Numeric Columns Summary")
                                    st.dataframe(stats['describe'], use_container_width=True)
                            
                            with col2:
                                st.subheader("Data Quality")
                                null_df = pd.DataFrame([
                                    {"Column": col, "Null Count": count, "Null %": f"{(count/stats['total_rows']*100):.1f}%"}
                                    for col, count in stats['null_counts'].items()
                                ])
                                st.dataframe(null_df, use_container_width=True)
                
                else:
                    logger.info("Generazione di grafici analitici con Pandas.")
                    st.info("🐼 Using Pandas engine - consider Spark for larger datasets")
                    if 'type' in data.columns:
                        type_counts = data['type'].value_counts().head(10)
                        fig = px.bar(x=type_counts.values, y=type_counts.index, orientation='h', 
                                    title="Events by Type")
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        logger.warning("Colonna 'type' non trovata in Pandas DataFrame. Impossibile generare grafico.")           
            except Exception as e:
                logger.error(f"Errore nella visualizzazione delle analisi: {e}", exc_info=True)
                st.error("Could not display charts.")
        
        with tab2:
            logger.info("Passaggio al Tab 'Custom Query'.")
            st.subheader("🔍 Custom Aggregation")
            try:
                if is_spark:
                    st.info("⚡ Using Spark SQL for high-performance aggregations")
                    logger.info("Interfaccia di aggregazione personalizzata per Spark.")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        group_col = st.selectbox("Group by:", data.columns, key="agg_group_col")
                    
                    with col2:
                        from pyspark.sql.types import IntegerType, LongType, FloatType, DoubleType
                        numeric_cols = [field.name for field in data.schema.fields 
                                        if isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType))]
                        available_cols = ['count'] + numeric_cols
                        agg_col = st.selectbox("Aggregate:", available_cols, key="agg_col")
                    
                    with col3:
                        if agg_col != 'count':
                            agg_func = st.selectbox("Function:", ['sum', 'avg', 'max', 'min'], key="agg_func")
                        else:
                            agg_func = 'count'
                    
                    with col4:
                        if st.button("🚀 Run Spark Query", type="primary"):
                            logger.info(f"Esecuzione query Spark custom: GROUP BY {group_col}, AGGREGATE {agg_func} on {agg_col}.")
                            with st.spinner("Running Spark aggregation..."):
                                result = st.session_state.spark_manager.create_aggregation_spark(
                                    data, group_col, agg_col if agg_col != 'count' else None, agg_func
                                )
                                
                                if result is not None and not result.empty:
                                    logger.info("Query Spark eseguita con successo. Visualizzazione risultati.")
                                    col_left, col_right = st.columns(2)
                                    
                                    with col_left:
                                        st.subheader("Results")
                                        st.dataframe(result, use_container_width=True)
                                    
                                    with col_right:
                                        st.subheader("Visualization")
                                        fig = px.bar(
                                            result.head(10),
                                            x=result.columns[-1],
                                            y=result.columns[0],
                                            orientation='h',
                                            title=f"{agg_func.title()} of {agg_col} by {group_col}",
                                            color=result.columns[-1],
                                            color_continuous_scale="viridis"
                                        )
                                        st.plotly_chart(fig, use_container_width=True)
                                    
                                    st.success(f"✅ Query completed using Spark distributed processing")
                                else:
                                    logger.warning("La query Spark ha prodotto un risultato vuoto.")
                                    st.warning("La query non ha prodotto risultati.")
                else:
                    logger.info("Interfaccia di aggregazione personalizzata per Pandas.")
                    st.info("🐼 Using Pandas for aggregation")
                
            except Exception as e:
                logger.error(f"Errore durante l'esecuzione della query SQL: {e}", exc_info=True)
                st.error(f"SQL Error: {e}")
            
        with tab3:
            logger.info("Passaggio al Tab 'Advanced'.")
            st.subheader("📈 Advanced Analytics")
            
            if is_spark:
                logger.info("Interfaccia di analytics avanzata per Spark.")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("⚡ Spark SQL Query")
                    
                    data.createOrReplaceTempView("disasters")
                    logger.debug("Creata vista temporanea 'disasters' per le query SQL.")
                    
                    sql_query = st.text_area(
                        "Write custom Spark SQL query:",
                        value="SELECT type, COUNT(*) as count FROM disasters GROUP BY type ORDER BY count DESC LIMIT 10",
                        height=150,
                        key="spark_sql_query"
                    )
                    
                    if st.button("Execute SQL"):
                        logger.info(f"Esecuzione query Spark SQL: {sql_query}")
                        try:
                            with st.spinner("Executing Spark SQL..."):
                                spark = st.session_state.spark_manager.spark
                                sql_result = spark.sql(sql_query)
                                result_pd = sql_result.toPandas()
                                logger.info("Query SQL eseguita con successo. Risultati convertiti in Pandas.")
                                
                                st.subheader("Query Results")
                                st.dataframe(result_pd, use_container_width=True)
                                
                                if len(result_pd.columns) == 2 and len(result_pd) <= 20:
                                    fig = px.bar(result_pd, x=result_pd.columns[1], y=result_pd.columns[0], 
                                                 orientation='h', title="Query Results")
                                    st.plotly_chart(fig, use_container_width=True)
                                    logger.debug("Visualizzazione automatica dei risultati SQL.")
                        
                        except Exception as e:
                            logger.error(f"Errore durante l'esecuzione della query SQL: {e}")
                            st.error(f"SQL Error: {str(e)}")
                
                with col2:
                    st.subheader("🎯 Performance Monitoring")
                    
                    spark = st.session_state.spark_manager.spark
                    if spark:
                        st.write("**Spark Configuration:**")
                        st.write(f"• App Name: {spark.sparkContext.appName}")
                        st.write(f"• Cores: {spark.sparkContext.defaultParallelism}")
                        st.write(f"• Master: {spark.sparkContext.master}")
                        
                        if hasattr(data, 'is_cached') and data.is_cached:
                            st.success("✅ Data is cached in memory")
                            logger.info("Il DataFrame Spark è nella cache.")
                        else:
                            if st.button("💾 Cache Dataset"):
                                data.cache()
                                st.success("✅ Dataset cached for faster subsequent queries")
                                logger.info("Il DataFrame Spark è stato messo in cache.")
                    
                    st.subheader("📋 Data Schema")
                    schema_info = []
                    for field in data.schema.fields:
                        schema_info.append({
                            'Column': field.name,
                            'Type': str(field.dataType),
                            'Nullable': field.nullable
                        })
                    
                    schema_df = pd.DataFrame(schema_info)
                    st.dataframe(schema_df, use_container_width=True)
                    logger.debug("Visualizzazione dello schema dei dati.")
            
            else:
                st.info("🐼 Advanced analytics available with Spark engine for larger datasets")
                logger.info("Funzionalità avanzate non disponibili con Pandas Engine.")
        
        with tab4:
            logger.info("Passaggio al Tab 'Export'.")
            st.subheader("💾 Export Options")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.subheader("📄 Standard Export")
                
                if is_spark:
                    if st.button("📊 Export Sample (CSV)"):
                        logger.info("Richiesta di esportazione di un campione (CSV).")
                        with st.spinner("Generating sample with Spark..."):
                            sample_pd = st.session_state.spark_manager.spark_df_to_pandas_sample(data, 10000)
                            csv = sample_pd.to_csv(index=False)
                            st.download_button(
                                "⬇️ Download Sample CSV",
                                csv,
                                f"disaster_sample_{len(sample_pd)}_records.csv",
                                "text/csv"
                            )
                            logger.info(f"Campione di {len(sample_pd)} record generato per il download.")
                    
                    if st.button("🚀 Export Full Dataset (Parquet)"):
                        st.info("💡 For large Spark datasets, use 'Advanced Export' to save directly to file system")
                
                else:
                    if hasattr(data, 'to_csv'):
                        csv = data.to_csv(index=False)
                        st.download_button(
                            "📄 Download CSV",
                            csv,
                            f"disaster_data_{len(data)}_records.csv",
                            "text/csv"
                        )
                        logger.info("Dati completi (Pandas) preparati per il download CSV.")
            
            with col2:
                st.subheader("⚡ Advanced Export")
                
                if is_spark:
                    logger.info("Interfaccia di esportazione avanzata per Spark.")
                    export_path = st.text_input("Output path:", "/tmp/disaster_export", key="export_path")
                    export_format = st.selectbox("Format:", ["parquet", "csv", "json"], key="export_format")
                    
                    if st.button("🚀 Export with Spark"):
                        logger.info(f"Avvio dell'esportazione avanzata in formato {export_format} al percorso {export_path}.")
                        try:
                            with st.spinner(f"Exporting to {export_format}..."):
                                if export_format == "parquet":
                                    data.write.mode("overwrite").parquet(export_path)
                                elif export_format == "csv":
                                    data.write.mode("overwrite").option("header", "true").csv(export_path)
                                elif export_format == "json":
                                    data.write.mode("overwrite").json(export_path)
                                
                                st.success(f"✅ Data exported to {export_path}")
                                st.info("💡 Files saved to local file system - check the specified path")
                                logger.info("Esportazione Spark completata con successo.")
                        
                        except Exception as e:
                            logger.error(f"Errore durante l'esportazione avanzata: {e}")
                            st.error(f"Export error: {str(e)}")
                else:
                    st.info("⚡ Advanced export available with Spark engine")
            
            with col3:
                st.subheader("📊 Analytics Export")
                
                if st.button("📋 Generate Report"):
                    logger.info("Generazione del report di analisi.")
                    report_data = {
                        'Analysis Summary': [
                            f"Total Records: {data.count() if is_spark else len(data):,}",
                            f"Total Columns: {len(data.columns)}",
                            f"Processing Engine: {'Apache Spark' if is_spark else 'Pandas'}",
                            f"Files Processed: {len(st.session_state.load_info) if st.session_state.load_info else 1}",
                            f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                        ]
                    }
                    
                    report_df = pd.DataFrame([(k, v) for k, values in report_data.items() for v in values], 
                                             columns=['Category', 'Value'])
                    
                    report_csv = report_df.to_csv(index=False)
                    st.download_button(
                        "📊 Download Analysis Report",
                        report_csv,
                        "disaster_analysis_report.csv",
                        "text/csv"
                    )
                    logger.info("Report di analisi generato e pronto per il download.")
    else:
        # Mostra un messaggio di benvenuto e istruzioni chiare per l'utente.
        # st.info crea una casella informativa con un'icona.
        st.info("👆 Upload your disaster datasets above to start the analysis")

        # Aggiunge una sottosezione per evidenziare le funzionalità basate su Spark.
        st.subheader("🚀 Spark-Powered Features")
        
        # Divide la larghezza della pagina in due colonne per un layout più pulito.
        col1, col2 = st.columns(2)
        
        # Inizia il blocco per la prima colonna.
        with col1:
            # Usa il markdown per formattare il testo con grassetto e elenchi.
            # Questo testo descrive i vantaggi dell'auto-ottimizzazione di Spark.
            st.markdown("""
            **⚡ Auto-Optimization**
            - Files >100MB → Spark Engine
            - Multiple files → Distributed processing  
            - Smart memory management
            - Lazy evaluation for efficiency
            
            **🎯 Performance Benefits**
            - Handle datasets up to several GB
            - Parallel processing across CPU cores
            - Optimized for aggregations and joins
            - Memory-efficient operations
            """)
        
        # Inizia il blocco per la seconda colonna.
        with col2:
            # Anche qui, usa il markdown per formattare e descrivere le funzionalità
            # di analisi avanzata ed esportazione.
            st.markdown("""
            **📊 Advanced Analytics**
            - Custom Spark SQL queries
            - Real-time performance monitoring
            - Distributed aggregations
            - Schema optimization
            
            **💾 Export Options**
            - Direct file system export
            - Optimized Parquet format
            - Large dataset handling
            - Batch processing support
            """)
        
        # Aggiunge una sottosezione per mostrare il formato dati supportato.
        st.subheader("📋 Supported Data Formats")
        
        # st.code mostra un blocco di codice formattato.
        # L'esempio JSON è utile per guidare gli utenti sul tipo di struttura dati da caricare.
        st.code("""
    # Example disaster data structure
    {
    "disaster_id": 1,
    "type": "Earthquake", 
    "country": "Italy",
    "date": "2023-01-15",
    "magnitude": 6.5,
    "casualties": 150,
    "economic_loss": 1500000000,
    "duration_days": 7
    }
        """, language='json')

if __name__ == "__main__":
    try:
        # Esegui l'intera applicazione all'interno di un blocco try
        main()
    except Exception as e:
        # Questo blocco catturerà QUALSIASI errore non gestito all'interno di main()
        
        # 1. Logga il traceback completo, che ti darà il file e la riga esatta
        tb_str = traceback.format_exc()
        logger.critical(f"ERRORE NON GESTITO A LIVELLO GLOBALE: {e}\nTRACEBACK:\n{tb_str}")
        
        # 2. Mostra un messaggio chiaro all'utente nell'interfaccia
        st.error(f"Si è verificato un errore critico nell'applicazione: {e}")
        with st.expander("Dettagli Tecnici dell'Errore (dal log)"):
            st.code(tb_str)
            
        # 3. Offri un modo per riavviare
        if st.button("🔄 Riavvia l'Applicazione"):
            st.rerun()
