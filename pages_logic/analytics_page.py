"""
Pagina per analisi avanzate sui disastri naturali
"""

import streamlit as st
import pandas as pd
from src.analytics import DisasterAnalytics, GeneralAnalytics
from src.visualizations import DisasterVisualizations, GeneralVisualizations, InteractiveVisualizations
from src.config import Config
import logging

logger = logging.getLogger(__name__)

def show_analytics_page():
    """Mostra la pagina delle analisi avanzate"""
    
    st.title("📈 Analisi Avanzate")
    st.markdown("Analisi statistiche e visualizzazioni avanzate sui disastri naturali")
    
    # Controlla se il dataset è caricato
    if not hasattr(st.session_state, 'dataset_loaded') or not st.session_state.dataset_loaded:
        st.warning("⚠️ Nessun dataset caricato. Vai alla sezione 'Esplora Dataset' per caricare i dati.")
        
        # Link per tornare alla pagina di caricamento
        if st.button("📊 Vai al Caricamento Dataset"):
            st.switch_page("pages/data_explorer.py")
        
        return
    
    dataset = st.session_state.dataset
    
    # Inizializza componenti di analisi
    disaster_analytics = DisasterAnalytics(dataset)
    general_analytics = GeneralAnalytics(dataset)
    
    # Inizializza visualizzazioni
    disaster_viz = DisasterVisualizations()
    general_viz = GeneralVisualizations()
    interactive_viz = InteractiveVisualizations()
    
    # Tabs per diverse analisi
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🌪️ Analisi Disastri", "📊 Aggregazioni Custom", "🔗 Correlazioni",
        "📅 Analisi Temporale", "🌍 Analisi Geografica"
    ])
    
    with tab1:
        show_disaster_analysis_tab(disaster_analytics, disaster_viz)
    
    with tab2:
        show_custom_aggregation_tab(general_analytics, general_viz, dataset)
    
    with tab3:
        show_correlation_tab(disaster_analytics, disaster_viz, dataset)
    
    with tab4:
        show_temporal_analysis_tab(disaster_analytics, disaster_viz, dataset)
    
    with tab5:
        show_geographical_analysis_tab(disaster_analytics, disaster_viz, dataset)

def show_disaster_analysis_tab(disaster_analytics, disaster_viz):
    """Tab per analisi specifiche sui disastri"""
    
    st.markdown("### 🌪️ Analisi Specifica Disastri Naturali")
    
    # Summary generale
    with st.spinner("Generazione summary disastri..."):
        summary = disaster_analytics.get_disaster_summary()
    
    if 'error' in summary:
        st.error(f"Errore nell'analisi: {summary['error']}")
        return
    
    # Metriche principali
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📊 Disastri Totali", f"{summary['total_disasters']:,}")
    
    with col2:
        st.metric("🗂️ Colonne Dataset", summary['total_columns'])
    
    with col3:
        if 'disaster_types' in summary:
            unique_types = len(summary['disaster_types'])
            st.metric("🏷️ Tipi Disastro", unique_types)
    
    # Analisi per tipo di disastro
    if 'disaster_types' in summary:
        st.markdown("#### 📊 Distribuzione per Tipo di Disastro")
        
        type_analysis = disaster_analytics.analyze_by_disaster_type()
        if type_analysis is not None and not type_analysis.empty:
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.dataframe(type_analysis, use_container_width=True)
            
            with col2:
                # Grafico distribuzione tipi
                fig = disaster_viz.create_disaster_type_chart(type_analysis)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Impossibile analizzare i tipi di disastro. Verifica che esista una colonna 'disaster_type' o simile.")
    
    # Analisi di impatto/severità
    st.markdown("#### 💥 Analisi di Impatto e Severità")
    
    severity_analysis = disaster_analytics.analyze_severity_impact()
    if severity_analysis:
        
        # Selezione metrica per visualizzazione
        available_metrics = list(severity_analysis.keys())
        if available_metrics:
            selected_metric = st.selectbox(
                "Seleziona metrica di impatto:",
                available_metrics,
                format_func=lambda x: x.replace('_', ' ').title()
            )
            
            if selected_metric in severity_analysis:
                metric_data = severity_analysis[selected_metric]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Dati per {selected_metric.replace('_', ' ').title()}:**")
                    st.dataframe(metric_data, use_container_width=True)
                
                with col2:
                    # Visualizzazione specifica per la metrica
                    if 'statistics' in selected_metric:
                        st.write("**Statistiche Descrittive**")
                        st.dataframe(metric_data, use_container_width=True)
                    else:
                        # Grafico per top eventi
                        if not metric_data.empty and len(metric_data.columns) >= 2:
                            fig = general_viz.create_horizontal_bar_chart(
                                metric_data.head(10),
                                metric_data.columns[0],
                                metric_data.columns[1],
                                f"Top 10 - {selected_metric.replace('_', ' ').title()}"
                            )
                            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Nessuna colonna di impatto trovata. Cerca colonne come 'casualties', 'deaths', 'damage', 'loss'.")
    
    # Suggerimenti per analisi
    with st.expander("💡 Suggerimenti per Analisi Avanzate"):
        st.markdown("""
        **Per ottenere analisi più precise:**
        
        1. **Nomi Colonne Standard**: Rinomina le colonne usando nomi comuni:
           - `disaster_type` o `type` per il tipo di disastro
           - `country` o `region` per la localizzazione
           - `date` o `year` per l'aspetto temporale
           - `casualties`, `deaths`, `damage` per l'impatto
        
        2. **Pulizia Dati**: 
           - Standardizza i nomi dei tipi di disastro (es: "Earthquake" invece di "earthquake", "EARTHQUAKE")
           - Converti le date in formato standard
           - Gestisci i valori mancanti nelle colonne numeriche
        
        3. **Analisi Avanzate Disponibili**:
           - Correlazioni tra variabili numeriche
           - Trend temporali per anno/mese
           - Distribuzione geografica
           - Analisi stagionale
        """)

def show_custom_aggregation_tab(general_analytics, general_viz, dataset):
    """Tab per aggregazioni personalizzate"""
    
    st.markdown("### 📊 Aggregazioni Personalizzate")
    
    # Selettori per aggregazione
    col1, col2, col3 = st.columns(3)
    
    with col1:
        group_by_col = st.selectbox(
            "Raggruppa per:",
            dataset.columns,
            help="Seleziona la colonna per il raggruppamento"
        )
    
    with col2:
        # Filtra colonne numeriche per aggregazione
        numeric_cols = [col for col, dtype in dataset.dtypes 
                       if dtype in ['int', 'bigint', 'double', 'float']]
        
        if numeric_cols:
            agg_col = st.selectbox(
                "Colonna da aggregare:",
                numeric_cols + ["count"],
                index=len(numeric_cols) if numeric_cols else 0
            )
        else:
            agg_col = "count"
            st.info("Solo conteggio disponibile (nessuna colonna numerica)")
    
    with col3:
        agg_types = list(Config.AGGREGATION_TYPES.keys())
        if agg_col == "count":
            agg_types = ["count"]
        
        agg_type = st.selectbox(
            "Tipo di aggregazione:",
            agg_types,
            format_func=lambda x: Config.AGGREGATION_TYPES.get(x, x)
        )
    
    # Controlli aggiuntivi
    col1, col2 = st.columns(2)
    
    with col1:
        limit_results = st.checkbox("Limita risultati", value=True)
        if limit_results:
            result_limit = st.slider("Numero massimo risultati:", 5, 100, 20)
        else:
            result_limit = None
    
    with col2:
        sort_desc = st.checkbox("Ordina decrescente", value=True)
        show_percentage = st.checkbox("Mostra percentuali", value=False)
    
    # Esegui aggregazione
    if st.button("🚀 Esegui Aggregazione", type="primary"):
        with st.spinner("Elaborazione aggregazione..."):
            
            # Esegui aggregazione
            result = general_analytics.perform_aggregation(group_by_col, agg_col, agg_type)
            
            if result is not None and not result.empty:
                
                # Applica limite se specificato
                if limit_results and result_limit:
                    result = result.head(result_limit)
                
                # Calcola percentuali se richiesto
                if show_percentage and len(result.columns) >= 2:
                    value_col = result.columns[1]
                    total = result[value_col].sum()
                    if total > 0:
                        result['percentuale'] = (result[value_col] / total * 100).round(2)
                
                # Mostra risultati
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Risultati Aggregazione:**")
                    st.dataframe(result, use_container_width=True)
                    
                    # Download risultati
                    csv = result.to_csv(index=False)
                    st.download_button(
                        "📥 Scarica Risultati (CSV)",
                        csv,
                        file_name=f"aggregazione_{group_by_col}_{agg_type}.csv",
                        mime="text/csv"
                    )
                
                with col2:
                    # Visualizzazione
                    if len(result.columns) >= 2:
                        chart_type = st.radio("Tipo grafico:", ["Barre", "Barre orizzontali", "Torta"])
                        
                        x_col, y_col = result.columns[0], result.columns[1]
                        
                        if chart_type == "Barre":
                            fig = general_viz.create_horizontal_bar_chart(result, x_col, y_col, f"{agg_type.title()} di {agg_col} per {group_by_col}")
                        elif chart_type == "Barre orizzontali":
                            fig = general_viz.create_horizontal_bar_chart(result, x_col, y_col, f"{agg_type.title()} di {agg_col} per {group_by_col}")
                        else:  # Torta
                            fig = general_viz.create_pie_chart(result.head(10), x_col, y_col, f"Distribuzione {group_by_col}")
                        
                        st.plotly_chart(fig, use_container_width=True)
                
                # Insights automatici
                if len(result) > 1:
                    st.markdown("#### 🔍 Insights Automatici")
                    
                    if len(result.columns) >= 2:
                        value_col = result.columns[1]
                        top_value = result.iloc[0]
                        total = result[value_col].sum()
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric(
                                f"Valore Massimo",
                                f"{top_value[value_col]:,.0f}",
                                f"{top_value[group_by_col]}"
                            )
                        
                        with col2:
                            if total > 0:
                                top_percentage = (top_value[value_col] / total) * 100
                                st.metric(
                                    "% del Totale",
                                    f"{top_percentage:.1f}%"
                                )
                        
                        with col3:
                            avg_value = result[value_col].mean()
                            st.metric(
                                "Valore Medio",
                                f"{avg_value:,.0f}"
                            )
            else:
                st.error("❌ Errore nell'aggregazione o nessun risultato trovato")

def show_correlation_tab(disaster_analytics, disaster_viz, dataset):
    """Tab per analisi delle correlazioni"""
    
    st.markdown("### 🔗 Analisi delle Correlazioni")
    
    # Crea matrice di correlazione
    correlation_matrix = disaster_analytics.create_correlation_matrix()
    
    if correlation_matrix is not None:
        
        # Heatmap correlazioni
        st.markdown("#### 🌡️ Matrice di Correlazione")
        fig = disaster_viz.create_correlation_heatmap(correlation_matrix)
        st.plotly_chart(fig, use_container_width=True)
        
        # Analisi correlazioni forti
        st.markdown("#### 🔍 Correlazioni Significative")
        
        # Trova correlazioni forti (> 0.7 o < -0.7)
        strong_correlations = []
        
        for i in range(len(correlation_matrix.columns)):
            for j in range(i+1, len(correlation_matrix.columns)):
                corr_value = correlation_matrix.iloc[i, j]
                if abs(corr_value) > 0.7:  # Correlazione forte
                    strong_correlations.append({
                        'Variabile 1': correlation_matrix.columns[i],
                        'Variabile 2': correlation_matrix.columns[j],
                        'Correlazione': round(corr_value, 3),
                        'Forza': 'Forte Positiva' if corr_value > 0 else 'Forte Negativa'
                    })
        
        if strong_correlations:
            st.dataframe(pd.DataFrame(strong_correlations), use_container_width=True)
            
            # Scatter plot per correlazioni selezionate
            if st.checkbox("Mostra scatter plot per correlazioni forti"):
                selected_corr = st.selectbox(
                    "Seleziona correlazione:",
                    range(len(strong_correlations)),
                    format_func=lambda i: f"{strong_correlations[i]['Variabile 1']} vs {strong_correlations[i]['Variabile 2']}"
                )
                
                if selected_corr is not None:
                    var1 = strong_correlations[selected_corr]['Variabile 1']
                    var2 = strong_correlations[selected_corr]['Variabile 2']
                    
                    # Ottieni campione di dati
                    sample_data = dataset.select(var1, var2).sample(0.1).toPandas()
                    
                    if not sample_data.empty:
                        general_viz = GeneralVisualizations()
                        fig = general_viz.create_scatter_plot(
                            sample_data, var1, var2,
                            f"Correlazione: {var1} vs {var2}"
                        )
                        st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Nessuna correlazione forte trovata (|r| > 0.7)")
        
        # Matrice di correlazione dettagliata
        with st.expander("📊 Matrice di Correlazione Completa"):
            st.dataframe(correlation_matrix, use_container_width=True)
    
    else:
        st.info("Impossibile calcolare correlazioni. Assicurati che il dataset contenga almeno 2 colonne numeriche.")
        
        # Mostra colonne disponibili
        st.write("**Colonne numeriche disponibili:**")
        numeric_cols = [col for col, dtype in dataset.dtypes 
                       if dtype in ['int', 'bigint', 'double', 'float']]
        
        if numeric_cols:
            for col in numeric_cols:
                st.write(f"- {col}")
        else:
            st.write("Nessuna colonna numerica trovata.")

def show_temporal_analysis_tab(disaster_analytics, disaster_viz, dataset):
    """Tab per analisi temporale"""
    
    st.markdown("### 📅 Analisi Temporale")
    
    # Analisi trend temporale
    temporal_analysis = disaster_analytics.analyze_temporal_trends()
    
    if temporal_analysis is not None and not temporal_analysis.empty:
        
        # Grafico trend
        st.markdown("#### 📈 Trend Temporale dei Disastri")
        fig = disaster_viz.create_temporal_trend(temporal_analysis)
        st.plotly_chart(fig, use_container_width=True)
        
        # Statistiche temporali
        col1, col2, col3 = st.columns(3)
        
        year_col = temporal_analysis.columns[0]
        count_col = temporal_analysis.columns[1]
        
        with col1:
            min_year = temporal_analysis[year_col].min()
            max_year = temporal_analysis[year_col].max()
            st.metric("📅 Periodo Analisi", f"{min_year} - {max_year}")
        
        with col2:
            avg_disasters = temporal_analysis[count_col].mean()
            st.metric("📊 Media Annuale", f"{avg_disasters:.1f}")
        
        with col3:
            max_disasters_year = temporal_analysis.loc[temporal_analysis[count_col].idxmax()]
            st.metric("🔺 Anno Peggiore", f"{max_disasters_year[year_col]} ({max_disasters_year[count_col]} disastri)")
        
        # Analisi dettagliata per anno
        st.markdown("#### 📊 Dati Dettagliati per Anno")
        
        # Aggiungi calcoli aggiuntivi
        temporal_detailed = temporal_analysis.copy()
        temporal_detailed['trend'] = temporal_detailed[count_col].rolling(window=3, center=True).mean()
        temporal_detailed['variazione_annuale'] = temporal_detailed[count_col].pct_change() * 100
        
        st.dataframe(temporal_detailed, use_container_width=True)
        
        # Analisi pattern stagionale (se disponibili dati mensili)
        with st.expander("🔍 Analisi Avanzata Temporale"):
            
            # Cerca colonne data più dettagliate
            date_columns = [col for col, dtype in dataset.dtypes 
                           if 'timestamp' in str(dtype).lower() or 'date' in str(dtype).lower()]
            
            if date_columns:
                st.write("**Colonne temporali disponibili per analisi dettagliata:**")
                for col in date_columns:
                    st.write(f"- {col}")
                
                selected_date_col = st.selectbox("Seleziona colonna data:", date_columns)
                
                if st.button("🔍 Analisi Stagionale"):
                    with st.spinner("Elaborazione analisi stagionale..."):
                        try:
                            # Estrai mese e giorno della settimana
                            from pyspark.sql.functions import month, dayofweek
                            
                            seasonal_data = dataset.withColumn("mese", month(selected_date_col)) \
                                                  .withColumn("giorno_settimana", dayofweek(selected_date_col)) \
                                                  .groupBy("mese").count() \
                                                  .orderBy("mese")
                            
                            seasonal_pd = seasonal_data.toPandas()
                            
                            if not seasonal_pd.empty:
                                # Nomi mesi
                                month_names = {1: 'Gen', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'Mag', 6: 'Giu',
                                             7: 'Lug', 8: 'Ago', 9: 'Set', 10: 'Ott', 11: 'Nov', 12: 'Dic'}
                                seasonal_pd['mese_nome'] = seasonal_pd['mese'].map(month_names)
                                
                                # Grafico stagionale
                                general_viz = GeneralVisualizations()
                                fig = general_viz.create_horizontal_bar_chart(
                                    seasonal_pd, 'mese_nome', 'count', 'Distribuzione Mensile dei Disastri'
                                )
                                st.plotly_chart(fig, use_container_width=True)
                        
                        except Exception as e:
                            st.error(f"Errore nell'analisi stagionale: {str(e)}")
            else:
                st.info("Nessuna colonna temporale dettagliata trovata per l'analisi stagionale.")
    
    else:
        st.info("Impossibile eseguire analisi temporale. Verifica che esista una colonna temporale nel dataset.")
        
        # Suggerimenti
        with st.expander("💡 Suggerimenti per Analisi Temporale"):
            st.markdown("""
            **Per abilitare l'analisi temporale:**
            
            1. **Colonne Data**: Il dataset deve contenere una colonna con date o anni
            2. **Formati Supportati**: 
               - Date: YYYY-MM-DD, DD/MM/YYYY
               - Anni: colonna numerica con anni (es: 2020, 2021)
            3. **Nomi Colonne**: Usa nomi come 'date', 'year', 'occurred_date', 'time'
            4. **Qualità Dati**: Assicurati che le date siano valide e non null
            """)

def show_geographical_analysis_tab(disaster_analytics, disaster_viz, dataset):
    """Tab per analisi geografica"""
    
    st.markdown("### 🌍 Analisi Geografica")
    
    # Analisi distribuzione geografica
    geo_analysis = disaster_analytics.analyze_geographical_distribution()
    
    if geo_analysis is not None and not geo_analysis.empty:
        
        # Mappa geografica
        st.markdown("#### 🗺️ Distribuzione Geografica Globale")
        
        try:
            fig = disaster_viz.create_geographical_map(geo_analysis)
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.warning(f"Impossibile creare mappa geografica: {str(e)}")
            # Fallback a grafico a barre
            fig = disaster_viz.create_horizontal_bar_chart(
                geo_analysis.head(20), 
                geo_analysis.columns[0], 
                geo_analysis.columns[1], 
                "Top 20 Paesi/Regioni per Numero di Disastri"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Statistiche geografiche
        col1, col2, col3 = st.columns(3)
        
        country_col = geo_analysis.columns[0]
        count_col = geo_analysis.columns[1]
        
        with col1:
            total_countries = len(geo_analysis)
            st.metric("🌍 Paesi/Regioni Totali", total_countries)
        
        with col2:
            most_affected = geo_analysis.iloc[0]
            st.metric("🔺 Più Colpito", f"{most_affected[country_col]} ({most_affected[count_col]} disastri)")
        
        with col3:
            avg_disasters = geo_analysis[count_col].mean()
            st.metric("📊 Media per Paese", f"{avg_disasters:.1f}")
        
        # Top paesi tabella
        st.markdown("#### 📊 Classifica Paesi/Regioni")
        
        # Aggiungi percentuali
        geo_detailed = geo_analysis.copy()
        total_disasters = geo_detailed[count_col].sum()
        geo_detailed['percentuale'] = (geo_detailed[count_col] / total_disasters * 100).round(2)
        geo_detailed['percentuale_cumulata'] = geo_detailed['percentuale'].cumsum()
        
        # Mostra top 20
        st.dataframe(geo_detailed.head(20), use_container_width=True)
        
        # Download dati geografici
        csv = geo_detailed.to_csv(index=False)
        st.download_button(
            "📥 Scarica Dati Geografici (CSV)",
            csv,
            file_name="analisi_geografica_disastri.csv",
            mime="text/csv"
        )
        
        # Analisi continentale (se possibile)
        with st.expander("🌍 Analisi per Continente/Regione"):
            st.markdown("""
            **Raggruppamento Geografico Avanzato:**
            
            Per analisi più dettagliate, considera di:
            - Aggiungere una colonna 'continent' al dataset
            - Usare coordinate geografiche per clustering
            - Standardizzare i nomi dei paesi
            """)
            
            # Tentativo di raggruppamento semplice per continente
            if st.checkbox("Tenta raggruppamento automatico per continente"):
                continent_mapping = {
                    # Europa
                    'Italy': 'Europa', 'Germany': 'Europa', 'France': 'Europa', 'Spain': 'Europa',
                    'United Kingdom': 'Europa', 'Netherlands': 'Europa', 'Belgium': 'Europa',
                    
                    # Nord America
                    'USA': 'Nord America', 'United States': 'Nord America', 'Canada': 'Nord America',
                    'Mexico': 'Nord America',
                    
                    # Asia
                    'China': 'Asia', 'Japan': 'Asia', 'India': 'Asia', 'South Korea': 'Asia',
                    'Indonesia': 'Asia', 'Thailand': 'Asia',
                    
                    # Altri
                    'Australia': 'Oceania', 'Brazil': 'Sud America', 'Argentina': 'Sud America'
                }
                
                geo_with_continent = geo_analysis.copy()
                geo_with_continent['Continente'] = geo_with_continent[country_col].map(
                    lambda x: continent_mapping.get(x, 'Altro')
                )
                
                continent_summary = geo_with_continent.groupby('Continente')[count_col].sum().reset_index()
                continent_summary = continent_summary.sort_values(count_col, ascending=False)
                
                if not continent_summary.empty:
                    st.write("**Distribuzione per Continente (mappatura automatica):**")
                    st.dataframe(continent_summary, use_container_width=True)
                    
                    # Grafico continenti
                    general_viz = GeneralVisualizations()
                    fig = general_viz.create_pie_chart(
                        continent_summary, 'Continente', count_col,
                        'Distribuzione per Continente'
                    )
                    st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.info("Impossibile eseguire analisi geografica. Verifica che esista una colonna geografica nel dataset.")
        
        # Suggerimenti
        with st.expander("💡 Suggerimenti per Analisi Geografica"):
            st.markdown("""
            **Per abilitare l'analisi geografica:**
            
            1. **Colonne Geografiche**: Il dataset deve contenere colonne come:
               - 'country', 'nation', 'state', 'region'
               - Coordinate: 'latitude', 'longitude'
            
            2. **Standardizzazione Nomi**: 
               - USA vs United States vs US
               - UK vs United Kingdom vs Britain
            
            3. **Qualità Dati**:
               - Evita abbreviazioni non standard
               - Controlla spelling dei nomi paesi
               - Rimuovi valori null o vuoti
            
            4. **Miglioramenti Suggeriti**:
               - Aggiungi colonna 'continent' per raggruppamenti
               - Usa codici ISO per standardizzazione
               - Considera coordinate GPS per mappe precise
            """)
        
        # Mostra colonne disponibili
        st.write("**Colonne disponibili nel dataset:**")
        for col in dataset.columns:
            st.write(f"- {col}")
        
        # Suggerisci possibili colonne geografiche
        possible_geo_cols = [col for col in dataset.columns 
                           if any(keyword in col.lower() 
                                for keyword in ['country', 'nation', 'state', 'region', 'location', 'place'])]
        
        if possible_geo_cols:
            st.write("**Possibili colonne geografiche rilevate:**")
            for col in possible_geo_cols:
                st.write(f"- ✅ {col}")
        else:
            st.write("**Nessuna colonna geografica ovvia rilevata.**")
            st.write("Rinomina una colonna geografica con un nome standard come 'country' per abilitare l'analisi.")