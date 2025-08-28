"""
Pagina per query SQL personalizzate
"""

import streamlit as st
import pandas as pd
from src.analytics import QueryEngine
from src.visualizations import GeneralVisualizations
from src.config import Config, DatabaseConfig
import logging

logger = logging.getLogger(__name__)

def show_custom_query_page():
    """Mostra la pagina delle query personalizzate"""
    
    st.title("🔍 Query SQL Personalizzate")
    st.markdown("Esegui query SQL personalizzate sui tuoi dati usando Apache Spark SQL")
    
    # Controlla se il dataset è caricato
    if not hasattr(st.session_state, 'dataset_loaded') or not st.session_state.dataset_loaded:
        st.warning("⚠️ Nessun dataset caricato. Vai alla sezione 'Esplora Dataset' per caricare i dati.")
        
        if st.button("📊 Vai al Caricamento Dataset"):
            st.switch_page("pages/data_explorer.py")
        
        return
    
    dataset = st.session_state.dataset
    
    # Inizializza query engine
    if 'query_engine' not in st.session_state:
        st.session_state.query_engine = QueryEngine()
    
    query_engine = st.session_state.query_engine
    general_viz = GeneralVisualizations()
    
    # Layout principale
    show_query_interface(query_engine, general_viz, dataset)

def show_query_interface(query_engine, general_viz, dataset):
    """Mostra l'interfaccia per le query"""
    
    # Tabs per diverse sezioni
    tab1, tab2, tab3, tab4 = st.tabs([
        "✍️ Editor Query", "📚 Query Template", "📊 Risultati", "📜 Cronologia"
    ])
    
    with tab1:
        show_query_editor_tab(query_engine, general_viz, dataset)
    
    with tab2:
        show_query_templates_tab(query_engine, dataset)
    
    with tab3:
        show_results_tab()
    
    with tab4:
        show_history_tab(query_engine)

def show_query_editor_tab(query_engine, general_viz, dataset):
    """Tab per l'editor delle query"""
    
    st.markdown("### ✍️ Editor SQL")
    
    # Informazioni sul dataset
    with st.expander("📊 Informazioni Dataset", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Nome Tabella SQL:**", f"`{DatabaseConfig.TEMP_VIEW_NAME}`")
            st.write("**Numero Righe:**", f"{dataset.count():,}")
            st.write("**Numero Colonne:**", len(dataset.columns))
        
        with col2:
            st.write("**Colonne Disponibili:**")
            for i, col in enumerate(dataset.columns):
                col_type = dict(dataset.dtypes)[col]
                st.write(f"`{col}` ({col_type})")
                if i > 10:  # Limita visualizzazione
                    remaining = len(dataset.columns) - i - 1
                    if remaining > 0:
                        st.write(f"... e altre {remaining} colonne")
                    break
    
    # Editor query
    st.markdown("#### 💻 Scrivi la tua Query SQL")
    
    # Query di default
    default_query = f"SELECT * FROM {DatabaseConfig.TEMP_VIEW_NAME} LIMIT 10"
    
    # Area di testo per la query
    query_text = st.text_area(
        "Query SQL:",
        value=default_query,
        height=150,
        help=f"Scrivi la tua query SQL. Usa '{DatabaseConfig.TEMP_VIEW_NAME}' come nome della tabella."
    )
    
    # Controlli esecuzione
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        execute_button = st.button("🚀 Esegui Query", type="primary")
    
    with col2:
        limit_results = st.checkbox("Limita Risultati", value=True)
    
    with col3:
        if limit_results:
            result_limit = st.number_input("Limite:", min_value=10, max_value=10000, value=100)
        else:
            result_limit = None
    
    with col4:
        validate_button = st.button("✅ Valida Query")
    
    # Validazione query
    if validate_button:
        validation = query_engine.validate_query(query_text)
        if validation['valid']:
            st.success("✅ Query valida!")
            if 'message' in validation:
                st.info(validation['message'])
        else:
            st.error(f"❌ Query non valida: {validation['error']}")
            if 'suggestion' in validation:
                st.info(f"💡 Suggerimento: {validation['suggestion']}")
    
    # Esecuzione query
    if execute_button:
        if not query_text.strip():
            st.error("⚠️ Inserisci una query SQL")
            return
        
        # Valida prima di eseguire
        validation = query_engine.validate_query(query_text)
        if not validation['valid']:
            st.error(f"❌ Query non valida: {validation['error']}")
            return
        
        with st.spinner("⚡ Esecuzione query in corso..."):
            try:
                result = query_engine.execute_custom_query(query_text, limit_results)
                
                if result is not None and not result.empty:
                    # Salva risultati nello stato della sessione
                    st.session_state.last_query_result = result
                    st.session_state.last_query_text = query_text
                    
                    st.success(f"✅ Query eseguita con successo! {len(result)} righe restituite.")
                    
                    # Mostra anteprima risultati
                    st.markdown("#### 👀 Anteprima Risultati")
                    st.dataframe(result.head(20), use_container_width=True)
                    
                    # Statistiche risultato
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("📊 Righe Totali", len(result))
                    
                    with col2:
                        st.metric("🗂️ Colonne", len(result.columns))
                    
                    with col3:
                        memory_usage = result.memory_usage(deep=True).sum() / 1024**2
                        st.metric("💾 Memoria", f"{memory_usage:.1f} MB")
                    
                    # Suggerisci visualizzazioni
                    if len(result) > 1 and len(result.columns) >= 2:
                        if st.checkbox("📊 Crea visualizzazione automatica"):
                            show_auto_visualization(result, general_viz)
                
                else:
                    st.warning("⚠️ Query eseguita ma nessun risultato restituito")
                    
            except Exception as e:
                st.error(f"❌ Errore nell'esecuzione della query: {str(e)}")
                logger.error(f"Errore query: {str(e)}")
    
    # Suggerimenti SQL
    with st.expander("💡 Suggerimenti SQL"):
        st.markdown(f"""
        **Esempi di Query Utili:**
        
        ```sql
        -- Contare i record per categoria
        SELECT categoria, COUNT(*) as count 
        FROM {DatabaseConfig.TEMP_VIEW_NAME} 
        GROUP BY categoria 
        ORDER BY count DESC;
        
        -- Statistiche su una colonna numerica
        SELECT 
            AVG(valore) as media,
            MIN(valore) as minimo,
            MAX(valore) as massimo,
            COUNT(*) as totale
        FROM {DatabaseConfig.TEMP_VIEW_NAME};
        
        -- Filtri con condizioni
        SELECT * 
        FROM {DatabaseConfig.TEMP_VIEW_NAME} 
        WHERE anno >= 2020 
        AND paese = 'Italy';
        
        -- Join con sottotabelle
        SELECT t1.*, t2.categoria
        FROM {DatabaseConfig.TEMP_VIEW_NAME} t1
        WHERE t1.valore > (SELECT AVG(valore) FROM {DatabaseConfig.TEMP_VIEW_NAME});
        ```
        
        **Funzioni Spark SQL Utili:**
        - `YEAR(data)`, `MONTH(data)` per estrarre parti di date
        - `UPPER(testo)`, `LOWER(testo)` per manipolare stringhe
        - `COALESCE(col1, col2)` per gestire valori null
        - `PERCENTILE_APPROX(col, 0.5)` per percentili
        """)

def show_query_templates_tab(query_engine, dataset):
    """Tab per i template delle query"""
    
    st.markdown("### 📚 Template Query Predefiniti")
    
    # Genera suggerimenti basati sul dataset
    suggestions = query_engine.get_query_suggestions(dataset.columns)
    
    # Template organizzati per categoria
    templates = {
        "🔍 Query di Base": {
            "Visualizza prime 10 righe": f"SELECT * FROM {DatabaseConfig.TEMP_VIEW_NAME} LIMIT 10",
            "Conta tutti i record": f"SELECT COUNT(*) as total_records FROM {DatabaseConfig.TEMP_VIEW_NAME}",
            "Mostra struttura tabella": f"DESCRIBE {DatabaseConfig.TEMP_VIEW_NAME}",
        },
        
        "📊 Aggregazioni": {
            "Conteggi per colonna": "SELECT colonna, COUNT(*) as count FROM disasters GROUP BY colonna ORDER BY count DESC",
            "Statistiche numeriche": "SELECT AVG(colonna_numerica) as media, MIN(colonna_numerica) as min, MAX(colonna_numerica) as max FROM disasters",
            "Top 10 valori": "SELECT colonna, COUNT(*) as frequenza FROM disasters GROUP BY colonna ORDER BY frequenza DESC LIMIT 10",
        },
        
        "📅 Query Temporali": {
            "Conteggi per anno": "SELECT YEAR(data) as anno, COUNT(*) as count FROM disasters GROUP BY YEAR(data) ORDER BY anno",
            "Trend mensile": "SELECT YEAR(data) as anno, MONTH(data) as mese, COUNT(*) as count FROM disasters GROUP BY YEAR(data), MONTH(data) ORDER BY anno, mese",
            "Ultimi 5 anni": "SELECT * FROM disasters WHERE YEAR(data) >= YEAR(CURRENT_DATE()) - 5",
        },
        
        "🌍 Query Geografiche": {
            "Conteggi per paese": "SELECT paese, COUNT(*) as count FROM disasters GROUP BY paese ORDER BY count DESC",
            "Top 10 paesi": "SELECT paese, COUNT(*) as disasters FROM disasters GROUP BY paese ORDER BY disasters DESC LIMIT 10",
            "Distribuzione continentale": "SELECT continente, COUNT(*) as count FROM disasters GROUP BY continente ORDER BY count DESC",
        },
        
        "🎯 Query Avanzate": {
            "Valori sopra la media": "SELECT * FROM disasters WHERE valore > (SELECT AVG(valore) FROM disasters)",
            "Percentili": "SELECT PERCENTILE_APPROX(valore, 0.25) as Q1, PERCENTILE_APPROX(valore, 0.5) as mediana, PERCENTILE_APPROX(valore, 0.75) as Q3 FROM disasters",
            "Duplicati": "SELECT *, COUNT(*) as duplicates FROM disasters GROUP BY colonna1, colonna2 HAVING COUNT(*) > 1",
        }
    }
    
    # Mostra template per categoria
    for category, queries in templates.items():
        with st.expander(category):
            for name, query in queries.items():
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.code(query, language="sql")
                
                with col2:
                    if st.button(f"Usa", key=f"use_{category}_{name}"):
                        st.session_state.selected_template = query
                        st.info("Query copiata! Vai al tab 'Editor Query' per eseguirla.")
    
    # Suggerimenti automatici basati sul dataset
    if suggestions:
        st.markdown("#### 🤖 Suggerimenti Automatici")
        st.info("Questi suggerimenti sono generati automaticamente basandosi sulle colonne del tuo dataset:")
        
        for i, suggestion in enumerate(suggestions):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.code(suggestion, language="sql")
            
            with col2:
                if st.button("Usa", key=f"auto_suggest_{i}"):
                    st.session_state.selected_template = suggestion
                    st.info("Query copiata! Vai al tab 'Editor Query' per eseguirla.")
    
    # Template personalizzati (da salvare)
    st.markdown("#### 💾 Template Personalizzati")
    
    # Inizializza se non esiste
    if 'custom_templates' not in st.session_state:
        st.session_state.custom_templates = {}
    
    # Form per aggiungere template personalizzato
    with st.form("add_template"):
        st.write("**Salva una query come template:**")
        
        template_name = st.text_input("Nome template:")
        template_query = st.text_area("Query SQL:")
        
        if st.form_submit_button("💾 Salva Template"):
            if template_name and template_query:
                st.session_state.custom_templates[template_name] = template_query
                st.success(f"Template '{template_name}' salvato!")
            else:
                st.error("Inserisci sia nome che query")
    
    # Mostra template salvati
    if st.session_state.custom_templates:
        st.write("**Template Salvati:**")
        
        for name, query in st.session_state.custom_templates.items():
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                st.write(f"**{name}**")
            
            with col2:
                if st.button(f"Usa", key=f"custom_{name}"):
                    st.session_state.selected_template = query
                    st.info("Query copiata!")
            
            with col3:
                if st.button(f"🗑️", key=f"delete_{name}", help="Elimina template"):
                    del st.session_state.custom_templates[name]
                    st.rerun()

def show_results_tab():
    """Tab per visualizzare i risultati dettagliati"""
    
    st.markdown("### 📊 Risultati Query")
    
    if 'last_query_result' not in st.session_state:
        st.info("Nessun risultato disponibile. Esegui una query nel tab 'Editor Query'.")
        return
    
    result = st.session_state.last_query_result
    query_text = st.session_state.get('last_query_text', 'N/A')
    
    # Informazioni sulla query eseguita
    st.markdown("#### 🔍 Query Eseguita")
    st.code(query_text, language="sql")
    
    # Controlli per i risultati
    col1, col2, col3 = st.columns(3)
    
    with col1:
        show_full_results = st.checkbox("Mostra tutti i risultati", value=False)
    
    with col2:
        if not show_full_results:
            display_limit = st.slider("Righe da mostrare:", 10, min(1000, len(result)), 100)
        else:
            display_limit = len(result)
    
    with col3:
        export_format = st.selectbox("Formato export:", ["CSV", "JSON", "Excel"])
    
    # Risultati
    st.markdown(f"#### 📋 Risultati ({len(result)} righe totali)")
    
    if show_full_results:
        st.dataframe(result, use_container_width=True)
    else:
        st.dataframe(result.head(display_limit), use_container_width=True)
        
        if len(result) > display_limit:
            st.info(f"Mostrando {display_limit} di {len(result)} righe. Attiva 'Mostra tutti i risultati' per vedere tutto.")
    
    # Statistiche risultato
    st.markdown("#### 📈 Statistiche Risultato")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Righe Totali", len(result))
    
    with col2:
        st.metric("🗂️ Colonne", len(result.columns))
    
    with col3:
        # Calcola memoria approssimativa
        memory_mb = result.memory_usage(deep=True).sum() / 1024**2
        st.metric("💾 Memoria", f"{memory_mb:.1f} MB")
    
    with col4:
        # Conta valori null
        total_nulls = result.isnull().sum().sum()
        st.metric("❓ Valori Null", total_nulls)
    
    # Analisi colonne
    if st.checkbox("📊 Mostra analisi dettagliata colonne"):
        st.markdown("#### 🔍 Analisi per Colonna")
        
        col_analysis = []
        for col in result.columns:
            col_data = result[col]
            col_info = {
                'Colonna': col,
                'Tipo': str(col_data.dtype),
                'Valori Null': col_data.isnull().sum(),
                'Valori Unici': col_data.nunique(),
                'Memoria (KB)': col_data.memory_usage(deep=True) / 1024
            }
            
            if col_data.dtype in ['int64', 'float64']:
                col_info.update({
                    'Media': col_data.mean() if not col_data.empty else None,
                    'Min': col_data.min() if not col_data.empty else None,
                    'Max': col_data.max() if not col_data.empty else None
                })
            
            col_analysis.append(col_info)
        
        analysis_df = pd.DataFrame(col_analysis)
        st.dataframe(analysis_df, use_container_width=True)
    
    # Export risultati
    st.markdown("#### 💾 Export Risultati")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if export_format == "CSV":
            csv_data = result.to_csv(index=False)
            st.download_button(
                "📥 Scarica CSV",
                csv_data,
                file_name="query_results.csv",
                mime="text/csv"
            )
    
    with col2:
        if export_format == "JSON":
            json_data = result.to_json(orient='records', indent=2)
            st.download_button(
                "📥 Scarica JSON",
                json_data,
                file_name="query_results.json",
                mime="application/json"
            )
    
    with col3:
        if export_format == "Excel":
            # Nota: richiede openpyxl
            try:
                excel_buffer = result.to_excel(index=False)
                st.download_button(
                    "📥 Scarica Excel",
                    excel_buffer,
                    file_name="query_results.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except Exception:
                st.warning("Export Excel non disponibile. Installa openpyxl.")

def show_history_tab(query_engine):
    """Tab per la cronologia delle query"""
    
    st.markdown("### 📜 Cronologia Query")
    
    query_history = query_engine.get_query_history()
    
    if not query_history:
        st.info("Nessuna query eseguita ancora.")
        return
    
    # Mostra cronologia
    st.markdown(f"**Query eseguite: {len(query_history)}**")
    
    for i, query_info in enumerate(reversed(query_history)):  # Più recenti prima
        with st.expander(f"Query #{len(query_history)-i} ({query_info.get('row_count', 0)} righe)"):
            
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.code(query_info['query'], language="sql")
            
            with col2:
                if st.button(f"🔄 Riusa", key=f"reuse_{i}"):
                    st.session_state.selected_template = query_info['query']
                    st.info("Query copiata! Vai al tab 'Editor Query'.")
                
                st.write(f"**Righe:** {query_info.get('row_count', 'N/A')}")
                if 'execution_time' in query_info and query_info['execution_time']:
                    st.write(f"**Tempo:** {query_info['execution_time']:.2f}s")
    
    # Opzioni cronologia
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🗑️ Pulisci Cronologia"):
            query_engine.query_history.clear()
            st.success("Cronologia pulita!")
            st.rerun()
    
    with col2:
        if query_history:
            # Export cronologia
            history_text = "\n\n".join([f"-- Query #{i+1}\n{q['query']}" for i, q in enumerate(query_history)])
            st.download_button(
                "📥 Esporta Cronologia",
                history_text,
                file_name="query_history.sql",
                mime="text/plain"
            )

def show_auto_visualization(result, general_viz):
    """Crea visualizzazioni automatiche basate sui risultati"""
    
    st.markdown("#### 📊 Visualizzazione Automatica")
    
    # Determina il tipo di visualizzazione migliore
    numeric_cols = result.select_dtypes(include=['int64', 'float64']).columns.tolist()
    categorical_cols = result.select_dtypes(include=['object', 'string']).columns.tolist()
    
    if len(result) == 1:
        st.info("Risultato con una sola riga - visualizzazione non applicabile")
        return
    
    # Caso 1: Una colonna categorica e una numerica (perfetto per bar chart)
    if len(categorical_cols) >= 1 and len(numeric_cols) >= 1:
        cat_col = categorical_cols[0]
        num_col = numeric_cols[0]
        
        chart_type = st.radio(
            "Tipo di grafico:",
            ["Barre", "Barre Orizzontali", "Torta"],
            key="auto_viz_type"
        )
        
        if chart_type == "Barre":
            fig = general_viz.create_horizontal_bar_chart(
                result.head(20), cat_col, num_col, f"{num_col} per {cat_col}"
            )
        elif chart_type == "Barre Orizzontali":
            fig = general_viz.create_horizontal_bar_chart(
                result.head(20), cat_col, num_col, f"{num_col} per {cat_col}"
            )
        else:  # Torta
            fig = general_viz.create_pie_chart(
                result.head(10), cat_col, num_col, f"Distribuzione {cat_col}"
            )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Caso 2: Due colonne numeriche (scatter plot)
    elif len(numeric_cols) >= 2:
        x_col, y_col = numeric_cols[0], numeric_cols[1]
        color_col = categorical_cols[0] if categorical_cols else None
        
        fig = general_viz.create_scatter_plot(
            result.head(1000),  # Limita per performance
            x_col, y_col,
            f"Relazione tra {x_col} e {y_col}",
            color_col
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Caso 3: Una sola colonna numerica (istogramma)
    elif len(numeric_cols) == 1:
        num_col = numeric_cols[0]
        
        fig = general_viz.create_histogram(
            result.head(1000),
            num_col,
            f"Distribuzione di {num_col}"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.info("Tipo di dati non ottimale per visualizzazione automatica. Prova query che restituiscono dati numerici o categorici.")

# Gestione template selezionato
if 'selected_template' in st.session_state:
    selected_template = st.session_state.selected_template
    del st.session_state.selected_template  # Rimuovi dopo l'uso
    
    # Questo verrà mostrato solo se viene chiamato da un altro tab
    st.info(f"Template selezionato: {selected_template[:50]}{'...' if len(selected_template) > 50 else ''}")