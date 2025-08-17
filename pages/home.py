"""
Pagina Home dell'applicazione
"""

import streamlit as st
from src.config import Config

def show_home_page():
    """Mostra la pagina home"""
    
    # Header principale
    st.title("🌪️ Analizzatore di Disastri Naturali")
    st.markdown("### Analisi avanzata con Apache Spark e Streamlit")
    
    # Introduzione
    st.markdown("""
    Benvenuto nell'applicazione per l'analisi di disastri naturali! Questa piattaforma ti permette di:
    
    - 📊 **Esplorare** grandi dataset sui disastri naturali
    - 📈 **Analizzare** trend temporali e distribuzioni geografiche  
    - 🔍 **Eseguire query** SQL personalizzate
    - 📋 **Visualizzare** risultati con grafici interattivi
    """)
    
    # Sezioni principali
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        #### 🚀 Tecnologie
        - **Apache Spark**: Elaborazione distribuita
        - **Streamlit**: Interfaccia web interattiva
        - **Plotly**: Visualizzazioni dinamiche
        - **Pandas**: Manipolazione dati
        """)
    
    with col2:
        st.markdown("""
        #### 📁 Formati Supportati
        - **CSV**: File separati da virgole
        - **JSON**: Formato JavaScript Object
        - **Parquet**: Formato colonnare ottimizzato
        - **File compressi**: Supporto .gz
        """)
    
    with col3:
        st.markdown("""
        #### 📊 Analisi Disponibili
        - Distribuzione per tipo di disastro
        - Analisi geografica
        - Trend temporali
        - Correlazioni tra variabili
        """)
    
    st.markdown("---")
    
    # Guida rapida
    st.subheader("🎯 Guida Rapida")
    
    steps = [
        ("1️⃣", "**Carica Dataset**", "Vai su 'Esplora Dataset' e carica il tuo file sui disastri naturali"),
        ("2️⃣", "**Esplora Dati**", "Visualizza la struttura e le statistiche del dataset"),
        ("3️⃣", "**Analizza**", "Usa 'Analisi Avanzate' per trend e correlazioni"),
        ("4️⃣", "**Query Custom**", "Esegui query SQL personalizzate per analisi specifiche")
    ]
    
    for icon, title, description in steps:
        col1, col2 = st.columns([1, 8])
        with col1:
            st.markdown(f"### {icon}")
        with col2:
            st.markdown(f"**{title}**")
            st.markdown(description)
    
    st.markdown("---")
    
    # Esempio di struttura dataset
    st.subheader("📋 Esempio Struttura Dataset")
    
    st.markdown("""
    Il tuo dataset sui disastri naturali dovrebbe contenere colonne simili a queste:
    """)
    
    # Tabella esempio
    example_data = {
        "disaster_id": [1, 2, 3],
        "disaster_type": ["Earthquake", "Flood", "Hurricane"],
        "country": ["Italy", "Germany", "USA"],
        "date": ["2023-01-15", "2023-02-20", "2023-03-10"],
        "magnitude": [6.2, None, "Category 4"],
        "casualties": [150, 25, 89],
        "economic_loss": [500000000, 100000000, 2000000000]
    }
    
    st.table(example_data)
    
    st.markdown("---")
    
    # Performance e configurazione
    with st.expander("⚙️ Configurazione e Performance"):
        st.markdown("""
        ### Configurazione Spark
        
        L'applicazione è configurata per ottimizzare automaticamente le performance in base alla dimensione del dataset:
        
        - **Dataset piccoli (<100MB)**: 1GB RAM per executor
        - **Dataset medi (100MB-1GB)**: 2GB RAM per executor  
        - **Dataset grandi (>1GB)**: 4GB RAM per executor
        
        ### Funzionalità Avanzate
        
        - **Caching intelligente**: I dati vengono automaticamente cachati per migliorare le performance
        - **Partizionamento adattivo**: Spark ottimizza automaticamente il partizionamento
        - **Query SQL**: Motore SQL completo per analisi personalizzate
        - **Rilevamento outlier**: Algoritmi per identificare anomalie nei dati
        """)
    
    # Tips & Tricks
    with st.expander("💡 Suggerimenti"):
        st.markdown("""
        ### Per ottenere i migliori risultati:
        
        1. **Pulizia dati**: Assicurati che le date siano in formato standard (YYYY-MM-DD)
        2. **Nomi colonne**: Usa nomi descrittivi come 'disaster_type', 'country', 'date'
        3. **Encoding**: Salva i file CSV in UTF-8 per caratteri speciali
        4. **Dimensioni**: File troppo grandi potrebbero richiedere più tempo per l'elaborazione
        5. **Backup**: Mantieni sempre una copia del dataset originale
        
        ### Problemi comuni:
        
        - **Errori di parsing**: Verifica il formato del file e la presenza di header
        - **Memoria insufficiente**: Prova a ridurre la dimensione del dataset
        - **Colonne non riconosciute**: Rinomina le colonne con nomi standard
        """)
    
    # Call to action
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; padding: 20px; background-color: #f0f2f6; border-radius: 10px;'>
        <h3>🚀 Inizia l'Analisi</h3>
        <p>Sei pronto per analizzare i tuoi dati sui disastri naturali?</p>
        <p>Inizia caricando il dataset nella sezione <strong>"Esplora Dataset"</strong></p>
    </div>
    """, unsafe_allow_html=True)
    
    # Status sistema (se disponibile)
    with st.expander("🔧 Status Sistema", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Stato Componenti:**")
            
            # Check Spark
            try:
                from src.spark_manager import SparkManager
                spark_info = SparkManager.get_session_info()
                if spark_info:
                    st.success("✅ Apache Spark: Attivo")
                    st.write(f"Versione: {spark_info.get('version', 'N/A')}")
                else:
                    st.info("ℹ️ Apache Spark: Non inizializzato")
            except Exception:
                st.warning("⚠️ Apache Spark: Errore di connessione")
            
            # Check Python libraries
            try:
                import pyspark
                st.success("✅ PySpark: Disponibile")
            except ImportError:
                st.error("❌ PySpark: Non installato")
                
            try:
                import plotly
                st.success("✅ Plotly: Disponibile") 
            except ImportError:
                st.error("❌ Plotly: Non installato")
        
        with col2:
            st.markdown("**Configurazione:**")
            st.write(f"**App Name:** {Config.SPARK_APP_NAME}")
            st.write(f"**Max Display Rows:** {Config.MAX_DISPLAY_ROWS}")
            st.write(f"**Default Query Limit:** {Config.DEFAULT_QUERY_LIMIT}")
            st.write(f"**Temp View Name:** disasters")
    
    return True