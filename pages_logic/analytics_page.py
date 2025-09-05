"""
Pagina Streamlit per l'analisi visiva automatica e intelligente dei dati.
Questo modulo tenta di generare grafici in autonomia e fornisce un'interfaccia
manuale come opzione secondaria.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, Tuple, Optional

### 1. CLASSE DI ANALISI PRINCIPALE
# ==============================================================================

class GeneralAnalytics:
    """
    Classe helper per eseguire aggregazioni sui dati utilizzando pandas.
    """
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe

    def perform_aggregation(self, group_by_col: str, agg_col: str, agg_type: str, sort_desc: bool = True) -> Optional[pd.DataFrame]:
        """
        Esegue un'aggregazione su un DataFrame pandas.
        """
        try:
            # Gestisce il caso speciale del conteggio
            if agg_col == "count" or agg_type == "count":
                result = self.df.groupby(group_by_col).size().reset_index(name='count')
                sort_column = 'count'
            # Gestisce le aggregazioni numeriche standard
            else:
                result = self.df.groupby(group_by_col)[agg_col].agg(agg_type).reset_index()
                sort_column = agg_col
            
            # Rinomina la colonna aggregata per maggiore chiarezza (es. 'val' -> 'sum_val')
            new_col_name = f"{agg_type}_{agg_col}" if agg_col != 'count' else 'count'
            result = result.rename(columns={agg_col: new_col_name})
            sort_column = new_col_name
            
            # Ordina i risultati come richiesto
            if sort_column in result.columns:
                result = result.sort_values(by=sort_column, ascending=(not sort_desc))
            
            return result
        except Exception as e:
            st.error(f"Errore durante l'aggregazione dei dati: {e}")
            return None

### 2. FUNZIONI PER L'ANALISI AUTOMATICA
# ==============================================================================

def find_best_columns_for_analysis(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], str]:
    """
    Analizza il DataFrame per trovare le migliori colonne per un'analisi automatica.
    Questa versione è più robusta e ignora le colonne con dati non "hashable".
    """
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns
    numerical_cols = df.select_dtypes(include=np.number).columns
    best_group_by = None
    
    suitable_candidates = {}

    if not categorical_cols.empty:
        for col in categorical_cols:
            try:
                # Tenta di calcolare il numero di valori unici.
                # Questo fallirà con un TypeError se la colonna contiene dizionari/liste.
                n_unique = df[col].nunique()
                
                # Considera la colonna adatta solo se ha un numero di categorie "ragionevole"
                if 3 < n_unique < 50:
                    suitable_candidates[col] = n_unique
            except TypeError:
                # Se la colonna non è "hashable", la ignora e continua
                st.sidebar.warning(f"ℹ️ Colonna '{col}' ignorata per l'analisi automatica perché contiene dati complessi (es. liste o dizionari).")
                continue

    # Se abbiamo trovato candidati adatti, scegliamo quello con meno categorie
    if suitable_candidates:
        best_group_by = min(suitable_candidates, key=suitable_candidates.get)

    if not best_group_by:
        return None, None, 'count'

    # Trova la migliore colonna da aggregare
    if not numerical_cols.empty:
        possible_agg_cols = [col for col in numerical_cols if col != best_group_by]
        if possible_agg_cols:
            return best_group_by, possible_agg_cols[0], 'sum'

    return best_group_by, 'count', 'count'


def run_automatic_analysis(general_analytics: GeneralAnalytics, dataset: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Tenta di eseguire un'analisi completamente automatica.
    Restituisce un dizionario con i risultati se ha successo, altrimenti None.
    """
    st.write("🧠 Analisi automatica del dataset in corso per trovare insights...")
    group_by_col, agg_col, agg_type = find_best_columns_for_analysis(dataset)
    
    if not group_by_col:
        st.warning("Non è stato possibile identificare una combinazione di colonne ideale per un'analisi automatica.")
        return None
        
    st.info(f"Trovata combinazione promettente: Aggregazione di **{agg_col}** per **{group_by_col}**.")
    
    result_df = general_analytics.perform_aggregation(group_by_col, agg_col, agg_type, sort_desc=True)
    
    if result_df is None or result_df.empty:
        return None
    
    result_df = result_df.head(20) # Limita i risultati per grafici più puliti
    chart_suggestions = suggest_charts(result_df)
    
    return {
        "result_df": result_df,
        "chart_suggestions": chart_suggestions,
        "params": (group_by_col, agg_col, agg_type)
    }

### 3. FUNZIONI PER LA GENERAZIONE DI GRAFICI E INSIGHTS
# ==============================================================================

def suggest_charts(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Analizza un DataFrame aggregato e suggerisce i tipi di grafico più appropriati.
    """
    suggestions = {}
    if df is None or df.empty or len(df.columns) < 2:
        return suggestions

    cat_col = df.columns[0]
    num_col = df.columns[1]
    unique_categories = df[cat_col].nunique()

    # Suggerimento #1: Grafico a barre orizzontali (quasi sempre utile)
    suggestions['horizontal_bar'] = {
        'type': 'horizontal_bar',
        'title': f'Classifica per {cat_col}',
        'description': f'Confronto dei valori di "{num_col}" tra le diverse categorie.',
        'priority': 1, 'config': {'x_col': cat_col, 'y_col': num_col}
    }

    # Suggerimento #2: Grafico a torta (se ci sono poche categorie)
    if unique_categories <= 10 and df[num_col].min() >= 0:
        suggestions['pie_chart'] = {
            'type': 'pie', 'title': f'Distribuzione per {cat_col}',
            'description': f'Ripartizione percentuale di "{num_col}".',
            'priority': 2, 'config': {'labels_col': cat_col, 'values_col': num_col}
        }

    # Suggerimento #3: Treemap (se ci sono molte categorie)
    if unique_categories > 10:
        suggestions['treemap'] = {
            'type': 'treemap', 'title': f'Mappa Gerarchica per {cat_col}',
            'description': 'Visualizzazione gerarchica per un gran numero di categorie.',
            'priority': 3, 'config': {'labels_col': cat_col, 'values_col': num_col}
        }
        
    return dict(sorted(suggestions.items(), key=lambda item: item[1]['priority']))


def _get_chart_icon(chart_type: str) -> str:
    """Restituisce un'icona emoji per un dato tipo di grafico."""
    icons = {'horizontal_bar': '📊', 'pie': '🥧', 'treemap': '🗂️'}
    return icons.get(chart_type, '📈')


def _create_and_display_chart(df: pd.DataFrame, config: Dict[str, Any]):
    """Crea e visualizza un singolo grafico Plotly basato sulla configurazione."""
    try:
        st.markdown(f"**{config['title']}**: {config['description']}")
        chart_type = config['type']
        chart_config = config['config']
        fig = None

        if chart_type == 'horizontal_bar':
            fig = px.bar(df, x=chart_config['y_col'], y=chart_config['x_col'], orientation='h',
                         title=config['title'], color=chart_config['y_col'],
                         color_continuous_scale='viridis')
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        
        elif chart_type == 'pie':
            fig = px.pie(df, names=chart_config['labels_col'], values=chart_config['values_col'],
                         title=config['title'])
            fig.update_traces(textposition='inside', textinfo='percent+label')

        elif chart_type == 'treemap':
            fig = px.treemap(df, path=[chart_config['labels_col']], values=chart_config['values_col'],
                             title=config['title'], color=chart_config['values_col'],
                             color_continuous_scale='RdYlGn')

        if fig:
            fig.update_layout(height=500, template='plotly_white')
            st.plotly_chart(fig, width='stretch')
            
    except Exception as e:
        st.error(f"❌ Errore nella creazione del grafico '{config.get('title', 'N/A')}': {e}")


def display_charts(df: pd.DataFrame, suggestions: Dict[str, Dict[str, Any]]):
    """Mostra i grafici suggeriti in un layout a tab."""
    if not suggestions:
        st.info("Nessuna visualizzazione specifica è stata suggerita per questi dati.")
        return

    tab_names = [f"{_get_chart_icon(cfg['type'])} {cfg['type'].replace('_', ' ').title()}" for cfg in suggestions.values()]
    tabs = st.tabs(tab_names)
    
    for tab, config in zip(tabs, suggestions.values()):
        with tab:
            _create_and_display_chart(df, config)


def display_automatic_insights(result: pd.DataFrame, group_by_col: str, agg_col: str, agg_type: str):
    """Mostra insights testuali generati automaticamente dai dati aggregati."""
    st.markdown("### 🔍 Insights Automatici")
    if result is None or result.empty or len(result.columns) < 2:
        st.info("Non ci sono dati sufficienti per generare insights.")
        return
    
    value_col = result.columns[1]
    category_col = result.columns[0]
    
    top_entry = result.iloc[0]
    total_value = result[value_col].sum()
    
    st.info(f"""
    - **Elemento Principale**: La categoria **{top_entry[category_col]}** ha il valore più alto, con **{top_entry[value_col]:,.0f}**.
    - **Concentrazione**: I primi 3 elementi rappresentano il **{(result.head(3)[value_col].sum() / total_value * 100 if total_value > 0 else 0):.1f}%** del totale.
    - **Distribuzione**: Sono presenti **{len(result)}** categorie uniche in questa analisi.
    """)

def is_hashable(series):
    try:
        series.nunique()
        return True
    except TypeError:
        return False

### 4. FUNZIONI PER L'INTERFACCIA UTENTE E L'ORCHESTRAZIONE
# ==============================================================================

def show_manual_aggregation_ui(dataset: pd.DataFrame) -> Dict[str, Any]:
    """
    Mostra i widget per la configurazione manuale e restituisce i parametri scelti.
    """
    st.markdown("### ⚙️ Configura la Tua Analisi")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        group_by_col = st.selectbox("🏷️ Raggruppa per:", 
                            [col for col in dataset.columns if is_hashable(dataset[col]) and dataset[col].nunique() < 100],
                            help="Seleziona una colonna con un numero ragionevole di categorie.")
    with col2:
        numeric_cols = list(dataset.select_dtypes(include=np.number).columns)
        agg_col = st.selectbox("🔢 Aggrega colonna:", numeric_cols + ["count"], index=len(numeric_cols))
    with col3:
        agg_types = ['count'] if agg_col == 'count' else ['sum', 'mean', 'max', 'min']
        agg_type = st.selectbox("📊 Tipo aggregazione:", agg_types)

    result_limit = st.slider("📏 Limita risultati a:", 5, 100, 20)
    sort_desc = st.toggle("⬇️ Ordina decrescente", value=True)
    
    execute_manual = st.button("🚀 Genera Analisi Personalizzata", type="primary", width='stretch')
    
    return {
        "execute": execute_manual,
        "params": (group_by_col, agg_col, agg_type),
        "options": (result_limit, sort_desc)
    }


def execute_smart_aggregation(general_analytics: GeneralAnalytics, group_by_col: str, agg_col: str, agg_type: str, 
                              result_limit: Optional[int], sort_desc: bool):
    """
    Esegue l'aggregazione richiesta dall'utente e mostra l'output completo.
    """
    with st.spinner("🔄 Elaborazione analisi personalizzata in corso..."):
        result = general_analytics.perform_aggregation(group_by_col, agg_col, agg_type, sort_desc=sort_desc)
        
        if result is not None and not result.empty:
            if result_limit:
                result = result.head(result_limit)
            
            st.markdown("---")
            st.markdown("## 📊 Risultati Analisi Personalizzata")
            st.dataframe(result, width='stretch')
            
            st.markdown("## 🎨 Visualizzazioni Suggerite")
            chart_suggestions = suggest_charts(result)
            display_charts(result, chart_suggestions)
            display_automatic_insights(result, group_by_col, agg_col, agg_type)
        else:
            st.error("❌ Nessun risultato trovato per l'aggregazione specificata.")

### 5. FUNZIONE PRINCIPALE DELLA PAGINA
# ==============================================================================

def show_analytics_page():
    """
    Funzione principale che orchestra l'intera pagina, dando priorità all'analisi automatica.
    """
    st.title("✨ Analisi Visiva Automatica")
    
    dataset = st.session_state.get('last_query_result')
    if dataset is None:
        st.warning("Esegui una query SQL per caricare un dataset prima di accedere alle analisi.")
        return

    if not isinstance(dataset, pd.DataFrame):
        try:
            dataset = dataset.toPandas()
        except Exception as e:
            st.error(f"Errore nella conversione del dataset in formato pandas: {e}")
            return
            
    general_analytics = GeneralAnalytics(dataset)

    # 1. Tenta l'analisi automatica
    with st.spinner("🤖 Eseguo l'analisi automatica del dataset..."):
        automatic_result = run_automatic_analysis(general_analytics, dataset)

    # 2. Se l'analisi automatica ha successo, mostra i risultati
    if automatic_result:
        st.success("🎉 Analisi automatica completata!")
        display_charts(automatic_result["result_df"], automatic_result["chart_suggestions"])
        
        with st.expander("📄 Visualizza i dati e gli insights automatici"):
            st.dataframe(automatic_result["result_df"], width='stretch')
            display_automatic_insights(automatic_result["result_df"], *automatic_result["params"])

    # 3. Offri sempre l'opzione manuale in un expander
    st.markdown("---")
    with st.expander("🔧 Crea un'analisi personalizzata", expanded=(not automatic_result)):
        manual_config = show_manual_aggregation_ui(dataset)
        
        if manual_config["execute"]:
            (group_by_col, agg_col, agg_type) = manual_config["params"]
            (result_limit, sort_desc) = manual_config["options"]
            
            execute_smart_aggregation(
                general_analytics, group_by_col, agg_col, agg_type, result_limit, sort_desc
            )