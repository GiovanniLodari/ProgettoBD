"""
Pagina per l'esplorazione del dataset
"""

import streamlit as st
import pandas as pd
from src.data_loader import DataLoader, FileHandler
from src.spark_manager import SparkManager
from src.visualizations import GeneralVisualizations, QualityVisualizations
from src.config import Config
import logging

logger = logging.getLogger(__name__)

# Inizializza componenti globali per la pagina
if 'data_loader' not in st.session_state:
    st.session_state.data_loader = DataLoader()
if 'general_viz' not in st.session_state:
    st.session_state.general_viz = GeneralVisualizations()
if 'quality_viz' not in st.session_state:
    st.session_state.quality_viz = QualityVisualizations()

def show_data_explorer():
    """Mostra la pagina di esplorazione del dataset"""
    
    st.title("📊 Esplorazione Dataset")
    st.markdown("Carica e esplora il tuo dataset sui disastri naturali")
    
    # Sezione caricamento file
    st.subheader("📁 Caricamento Dataset")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        uploaded_file = st.file_uploader(
            "Seleziona il file del dataset",
            type=['csv', 'json', 'parquet', 'gz'],
            help="Formati supportati: CSV, JSON, Parquet e file compressi .gz"
        )
    
    with col2:
        if uploaded_file is not None:
            st.info(f"""
            **File:** {uploaded_file.name}
            **Dimensione:** {uploaded_file.size / 1024:.1f} KB
            **Tipo:** {uploaded_file.type}
            """)
    
    # Processa il file caricato
    if uploaded_file is not None:
        if st.button("🚀 Carica Dataset", type="primary"):
            with st.spinner("Caricamento dataset in corso..."):
                # Gestisci il file uploadato
                temp_file_path = FileHandler.handle_uploaded_file(uploaded_file)
                
                if temp_file_path:
                    # Carica con Spark
                    dataset = st.session_state.data_loader.load_file(temp_file_path)
                    
                    if dataset is not None:
                        st.session_state.dataset = dataset
                        st.session_state.dataset_loaded = True
                        st.success("✅ Dataset caricato con successo!")
                        st.rerun()
                    else:
                        st.error("❌ Errore nel caricamento del dataset")
                else:
                    st.error("❌ Errore nella gestione del file")
    
    # Se il dataset è caricato, mostra l'esplorazione
    if hasattr(st.session_state, 'dataset_loaded') and st.session_state.dataset_loaded:
        show_dataset_exploration()
    else:
        show_dataset_info()

def show_dataset_exploration():
    """Mostra l'esplorazione del dataset caricato"""
    
    dataset = st.session_state.dataset
    data_loader = st.session_state.data_loader
    
    st.markdown("---")
    st.subheader("🔍 Esplorazione Dataset")
    
    # Tabs per diverse viste
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 Overview", "🗂️ Struttura", "📊 Statistiche", 
        "🎯 Qualità Dati", "👀 Anteprima"
    ])
    
    with tab1:
        show_overview_tab(data_loader)
    
    with tab2:
        show_structure_tab(dataset)
    
    with tab3:
        show_statistics_tab(dataset)
    
    with tab4:
        show_quality_tab(data_loader)
    
    with tab5:
        show_preview_tab(data_loader)

def show_overview_tab(data_loader):
    """Mostra overview generale del dataset"""
    
    metadata = data_loader.get_metadata()
    
    # Metriche principali
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "📊 Record Totali", 
            f"{metadata.get('record_count', 0):,}",
            help="Numero totale di righe nel dataset"
        )
    
    with col2:
        st.metric(
            "🗂️ Colonne", 
            metadata.get('column_count', 0),
            help="Numero di colonne nel dataset"
        )
    
    with col3:
        file_size_mb = metadata.get('file_size_mb', 0)
        st.metric(
            "💾 Dimensione", 
            f"{file_size_mb:.1f} MB",
            help="Dimensione del file originale"
        )
    
    with col4:
        st.metric(
            "⚡ Partizioni Spark", 
            metadata.get('partitions', 0),
            help="Numero di partizioni per elaborazione distribuita"
        )
    
    # Informazioni aggiuntive
    st.markdown("### ℹ️ Dettagli File")
    col1, col2 = st.columns(2)
    
    with col1:
        st.write(f"**Percorso:** {metadata.get('file_path', 'N/A')}")
        st.write(f"**Dimensione bytes:** {metadata.get('file_size_bytes', 0):,}")
    
    with col2:
        # Calcola densità dati
        if metadata.get('record_count', 0) > 0 and file_size_mb > 0:
            density = metadata['record_count'] / file_size_mb
            st.write(f"**Densità:** {density:.0f} record/MB")
        
        # Stima memoria Spark
        estimated_memory = max(file_size_mb * 2, 512)  # Almeno 512MB
        st.write(f"**Memoria stimata:** {estimated_memory:.0f} MB")

def show_structure_tab(dataset):
    """Mostra la struttura del dataset"""
    
    st.markdown("### 🏗️ Schema Dataset")
    
    # Informazioni sulle colonne
    column_info = []
    for field in dataset.schema.fields:
        column_info.append({
            'Colonna': field.name,
            'Tipo Dati': str(field.dataType),
            'Nullable': "Sì" if field.nullable else "No"
        })
    
    column_df = pd.DataFrame(column_info)
    st.dataframe(column_df, use_container_width=True)
    
    # Analisi tipi di dati
    st.markdown("### 📊 Distribuzione Tipi di Dati")
    
    type_counts = {}
    for _, row in column_df.iterrows():
        data_type = row['Tipo Dati'].split('(')[0]  # Rimuovi dettagli tipo
        type_counts[data_type] = type_counts.get(data_type, 0) + 1
    
    if type_counts:
        type_df = pd.DataFrame(list(type_counts.items()), columns=['Tipo', 'Conteggio'])
        
        # Grafico distribuzione tipi
        viz = st.session_state.general_viz
        fig = viz.create_pie_chart(type_df, 'Tipo', 'Conteggio', 'Distribuzione Tipi di Dati')
        st.plotly_chart(fig, use_container_width=True)
    
    # Suggerimenti per ottimizzazione
    with st.expander("💡 Suggerimenti per Ottimizzazione"):
        st.markdown("""
        **Per migliorare le performance:**
        
        - **Colonne con molti valori null:** Considera di rimuoverle o sostituire i valori
        - **Tipi string lunghi:** Valuta se convertire in categorie per risparmiare memoria
        - **Colonne timestamp:** Assicurati che siano nel formato corretto per analisi temporali
        - **Colonne numeriche:** Verifica che non contengano valori testuali
        """)

def show_statistics_tab(dataset):
    """Mostra statistiche descrittive"""
    
    st.markdown("### 📈 Statistiche Descrittive")
    
    # Identifica colonne numeriche
    numeric_columns = [col for col, dtype in dataset.dtypes 
                      if dtype in ['int', 'bigint', 'double', 'float']]
    
    if numeric_columns:
        # Selezione colonna per statistiche dettagliate
        selected_col = st.selectbox(
            "Seleziona colonna per statistiche dettagliate:",
            numeric_columns
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Statistiche base
            stats_df = dataset.select(selected_col).describe().toPandas()
            st.write("**Statistiche Base:**")
            st.dataframe(stats_df, use_container_width=True)
        
        with col2:
            # Percentili
            try:
                percentiles = dataset.select(selected_col).approxQuantile(
                    selected_col, [0.05, 0.25, 0.5, 0.75, 0.95], 0.05
                )
                
                percentile_df = pd.DataFrame({
                    'Percentile': ['5%', '25%', '50% (Mediana)', '75%', '95%'],
                    'Valore': percentiles
                })
                
                st.write("**Percentili:**")
                st.dataframe(percentile_df, use_container_width=True)
            except Exception as e:
                st.warning(f"Impossibile calcolare percentili: {str(e)}")
        
        # Istogramma
        sample_data = dataset.select(selected_col).sample(0.1).toPandas()
        if not sample_data.empty:
            viz = st.session_state.general_viz
            fig = viz.create_histogram(
                sample_data, 
                selected_col, 
                f'Distribuzione di {selected_col}'
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Nessuna colonna numerica trovata per le statistiche descrittive")
    
    # Statistiche categoriche
    categorical_columns = [col for col, dtype in dataset.dtypes 
                          if dtype == 'string' or 'int' not in dtype.lower()]
    
    if categorical_columns:
        st.markdown("### 🏷️ Analisi Colonne Categoriche")
        
        cat_col = st.selectbox("Seleziona colonna categorica:", categorical_columns)
        
        # Top valori
        top_values = dataset.groupBy(cat_col).count().orderBy("count", ascending=False).limit(10)
        top_values_pd = top_values.toPandas()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Top 10 Valori:**")
            st.dataframe(top_values_pd, use_container_width=True)
        
        with col2:
            # Grafico
            if not top_values_pd.empty:
                viz = st.session_state.general_viz
                fig = viz.create_horizontal_bar_chart(
                    top_values_pd, cat_col, 'count', 
                    f'Top Valori - {cat_col}'
                )
                st.plotly_chart(fig, use_container_width=True)

def show_quality_tab(data_loader):
    """Mostra analisi qualità dati"""
    
    from src.analytics import GeneralAnalytics
    
    st.markdown("### 🎯 Analisi Qualità Dati")
    
    # Genera report qualità
    analytics = GeneralAnalytics(st.session_state.dataset)
    quality_report = analytics.generate_data_quality_report()
    
    if 'error' in quality_report:
        st.error(f"Errore nel report qualità: {quality_report['error']}")
        return
    
    # Punteggio qualità generale
    quality_score = quality_report.get('overall_quality_score', 0)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Gauge per punteggio qualità
        viz = st.session_state.quality_viz
        fig = viz.create_quality_score_gauge(quality_score)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.metric("📊 Record Totali", quality_report['overview']['total_rows'])
        st.metric("🗂️ Colonne Totali", quality_report['overview']['total_columns'])
    
    with col3:
        duplicates = quality_report['consistency']['duplicates']
        st.metric(
            "🔄 Duplicati", 
            duplicates['duplicate_rows'],
            delta=f"{duplicates['duplicate_percentage']:.1f}%"
        )
    
    # Grafico completezza
    st.markdown("### 📋 Completezza per Colonna")
    completeness_data = quality_report['completeness']
    
    if completeness_data:
        fig = viz.create_completeness_chart(completeness_data)
        st.plotly_chart(fig, use_container_width=True)
    
    # Tabella dettagliata completezza
    with st.expander("📄 Dettagli Completezza"):
        completeness_df = pd.DataFrame([
            {
                'Colonna': col,
                'Valori Completi (%)': data['complete_percentage'],
                'Valori Mancanti': data['null_count'],
                'Valori Mancanti (%)': data['null_percentage']
            }
            for col, data in completeness_data.items()
        ])
        st.dataframe(completeness_df, use_container_width=True)
    
    # Rilevamento outlier
    st.markdown("### 🎯 Rilevamento Outlier")
    
    numeric_columns = [col for col, dtype in st.session_state.dataset.dtypes 
                      if dtype in ['int', 'bigint', 'double', 'float']]
    
    if numeric_columns:
        outlier_col = st.selectbox("Seleziona colonna per rilevamento outlier:", numeric_columns)
        outlier_method = st.selectbox("Metodo rilevamento:", ['iqr', 'zscore'])
        
        if st.button("🔍 Rileva Outlier"):
            outliers_df = analytics.detect_outliers(outlier_col, outlier_method)
            
            if outliers_df is not None and not outliers_df.empty:
                st.write(f"**Trovati {len(outliers_df)} outlier:**")
                st.dataframe(outliers_df.head(20), use_container_width=True)
                
                # Visualizzazione outlier
                fig = viz.create_outliers_visualization(outliers_df, outlier_col)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("✅ Nessun outlier trovato con il metodo selezionato")
    else:
        st.info("Nessuna colonna numerica disponibile per il rilevamento outlier")

def show_preview_tab(data_loader):
    """Mostra anteprima dei dati"""
    
    st.markdown("### 👀 Anteprima Dataset")
    
    # Controlli per l'anteprima
    col1, col2, col3 = st.columns(3)
    
    with col1:
        num_rows = st.slider("Numero di righe da mostrare:", 10, 500, 100)
    
    with col2:
        if st.button("🔄 Aggiorna Anteprima"):
            st.rerun()
    
    with col3:
        show_types = st.checkbox("Mostra tipi di dati", value=False)
    
    # Ottieni campione dati
    sample_data = data_loader.get_sample_data(num_rows)
    
    if not sample_data.empty:
        if show_types:
            # Mostra con informazioni sui tipi
            st.write("**Dati con tipi:**")
            buffer = []
            for col in sample_data.columns:
                dtype = str(sample_data[col].dtype)
                buffer.append(f"**{col}** ({dtype})")
            
            st.write(" | ".join(buffer))
        
        st.dataframe(sample_data, use_container_width=True)
        
        # Opzioni di download
        col1, col2 = st.columns(2)
        
        with col1:
            csv = sample_data.to_csv(index=False)
            st.download_button(
                label="📥 Scarica Anteprima (CSV)",
                data=csv,
                file_name=f"anteprima_{num_rows}_righe.csv",
                mime="text/csv"
            )
        
        with col2:
            json_data = sample_data.to_json(orient='records', indent=2)
            st.download_button(
                label="📥 Scarica Anteprima (JSON)",
                data=json_data,
                file_name=f"anteprima_{num_rows}_righe.json",
                mime="application/json"
            )
    else:
        st.warning("⚠️ Impossibile ottenere anteprima dei dati")

def show_dataset_info():
    """Mostra informazioni sui dataset supportati"""
    
    st.markdown("---")
    st.subheader("📋 Informazioni sui Dataset Supportati")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        #### 📁 Formati File
        - **CSV**: File separati da virgole con header
        - **JSON**: Formato JavaScript Object Notation
        - **Parquet**: Formato colonnare ottimizzato
        - **File .gz**: Tutti i formati supportano compressione gzip
        """)
        
        st.markdown("""
        #### 🔧 Configurazioni Automatiche
        - **Inferenza schema**: Rilevamento automatico tipi dati
        - **Encoding**: Supporto UTF-8 per caratteri speciali
        - **Partizionamento**: Ottimizzazione automatica Spark
        - **Caching**: Cache intelligente per performance
        """)
    
    with col2:
        st.markdown("""
        #### 💡 Best Practices
        - Usa nomi colonne descrittivi (es: 'disaster_type', 'country')
        - Formati date standard (YYYY-MM-DD)
        - Evita caratteri speciali nei nomi colonne
        - Mantieni consistenza nei valori categorici
        """)
        
        st.markdown("""
        #### ⚠️ Limitazioni
        - Dimensione max raccomandata: 5GB
        - Evita file con migliaia di colonne
        - Alcuni caratteri Unicode potrebbero causare problemi
        - File corrotti possono interrompere il caricamento
        """)
    
    # Esempio struttura
    st.markdown("#### 📊 Esempio Struttura Dataset Disastri")
    
    example_data = {
        "disaster_id": [1, 2, 3, 4, 5],
        "disaster_type": ["Earthquake", "Flood", "Hurricane", "Wildfire", "Tornado"],
        "country": ["Italy", "Germany", "USA", "Australia", "USA"],
        "date": ["2023-01-15", "2023-02-20", "2023-03-10", "2023-04-05", "2023-05-12"],
        "magnitude": [6.2, None, "Category 4", None, "EF3"],
        "casualties": [150, 25, 89, 12, 45],
        "economic_loss_usd": [500000000, 100000000, 2000000000, 50000000, 25000000]
    }
    
    st.table(pd.DataFrame(example_data))