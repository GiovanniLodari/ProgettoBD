"""
Pagina per query SQL personalizzate - Versione semplificata
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from src.analytics import QueryEngine
#from src.visualizations import GeneralVisualizations
from src.config import Config, DatabaseConfig
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging
import os, json
import re
import traceback
from typing import Optional, Dict, Any, List, Tuple
from utils.utils import get_twitter_query_templates, force_open_sidebar, force_close_sidebar
from pages_logic.analytics_page import show_analytics_page

logger = logging.getLogger(__name__)

class RobustDataConverter:
    """Classe per gestire conversioni robuste da Spark a Pandas con gestione dimensioni"""
    
    DEFAULT_MAX_ROWS = 10000
    
    @staticmethod
    def _is_complex_type(data_type) -> bool:
        """Controlla se un tipo di dato è complesso (struct, array, map)"""
        return isinstance(data_type, (StructType, ArrayType, MapType))

class EnhancedQueryEngine(QueryEngine):
    """QueryEngine migliorato con gestione errori robusta"""
    
    def __init__(self):
        super().__init__()
        if not hasattr(self, 'query_history'):
            self.query_history = []
    
    def execute_custom_query_safe(self, query: str, limit_results: Optional[int] = None) -> Dict[str, Any]:
        """
        Esegue query con gestione errori comprehensiva
        
        Returns:
            Dict con 'success', 'data', 'error', 'warning', 'stats'
        """
        import time
        start_time = time.time()
        
        result = {
            'success': False,
            'data': None,
            'error': None,
            'warning': None,
            'stats': {}
        }
        
        try:
            validation = self.validate_query(query)
            if not validation['valid']:
                result['error'] = f"Query non valida: {validation['error']}"
                self._add_to_history(query, 0, False, result['error'])
                return result
            
            spark_session = st.session_state.spark_manager.get_spark_session()
            spark_df = spark_session.sql(query)

            try:
                total_rows_count = spark_df.count()
            except Exception as e:
                total_rows_count = "N/A"
                st.error(f"❌ Impossibile contare le righe totali: {e}")

            logger.info(f"Conteggio totale righe: {total_rows_count}")
            
            # if limit_results:
            #     spark_df = spark_df.limit(limit_results)
            # else:
            #     spark_df = spark_df.limit(RobustDataConverter.DEFAULT_MAX_ROWS)
            
            if self._is_empty_spark_df(spark_df):
                result['warning'] = "Query eseguita ma nessun risultato restituito"
                result['success'] = True
                result['data'] = pd.DataFrame()
                execution_time = time.time() - start_time
                self._add_to_history(query, 0, True, None, execution_time)
                return result

            if limit_results:
                pandas_df = spark_df.toPandas()
                if total_rows_count != "N/A" and total_rows_count > limit_results:
                    st.info(f"Mostrando {limit_results} righe di {total_rows_count}.")
            else:
                pandas_df = spark_df.toPandas()
                if total_rows_count != "N/A" and total_rows_count > RobustDataConverter.DEFAULT_MAX_ROWS:
                    st.warning(f"La query completa è troppo grande. Visualizzazione limitata a {RobustDataConverter.DEFAULT_MAX_ROWS} righe su {total_rows_count} per evitare errori.")

            result['stats'] = self._calculate_stats(pandas_df, spark_df)
            result['stats']['total_rows'] = total_rows_count
            result['success'] = True
            result['data'] = pandas_df
            
            execution_time = time.time() - start_time
            self._add_to_history(query, len(pandas_df), True, None, execution_time)
            
            return result
            
        except Exception as e:
            logger.error(f"Errore nell'esecuzione query: {str(e)}")
            logger.error(traceback.format_exc())
            result['error'] = f"Errore nell'esecuzione: {str(e)}"
            execution_time = time.time() - start_time
            self._add_to_history(query, 0, False, str(e), execution_time)
            return result
    
    def _add_to_history(self, query: str, row_count: int, success: bool = True, 
                       error: Optional[str] = None, execution_time: Optional[float] = None):
        """Aggiunge query alla cronologia"""
        history_entry = {
            'query': query.strip(),
            'row_count': row_count,
            'success': success,
            'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
            'execution_time': execution_time or 0
        }
        
        if error:
            history_entry['error'] = error
        
        self.query_history.append(history_entry)
        
        if len(self.query_history) > 50:
            self.query_history = self.query_history[-50:]
    
    def get_query_history(self) -> List[Dict[str, Any]]:
        """Restituisce la cronologia delle query"""
        return getattr(self, 'query_history', [])
    
    def clear_history(self):
        """Pulisce la cronologia delle query"""
        self.query_history = []
    
    def validate_query(self, query: str) -> Dict[str, Any]:
        """Valida una query SQL"""
        
        if not query.strip():
            return {'valid': False, 'error': 'Query vuota'}
        
        query_upper = query.upper().strip()
        
        if not any(query_upper.startswith(cmd) for cmd in ['SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'EXPLAIN']):
            return {'valid': False, 'error': 'Query deve iniziare con SELECT, WITH, SHOW, DESCRIBE o EXPLAIN'}
        
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        tokens = re.split(r'[\s(),;]+', query_upper)
        for token in tokens:
            for keyword in dangerous_keywords:
                if token == keyword:
                    return {'valid': False, 'error': f'Comando non permesso: trovata la keyword "{token}"'}
        return {'valid': True}
        
    def _is_empty_spark_df(self, df) -> bool:
        """Controlla se DataFrame Spark è vuoto"""
        try:
            return len(df.take(1)) == 0
        except:
            return True
    
    def _calculate_stats(self, pandas_df: pd.DataFrame, spark_df) -> Dict[str, Any]:
        """Calcola statistiche sui risultati"""
        stats = {
            'rows': len(pandas_df),
            'columns': len(pandas_df.columns),
            'memory_mb': pandas_df.memory_usage(deep=True).sum() / 1024**2,
            'null_values': pandas_df.isnull().sum().sum(),
        }
        
        numeric_cols = pandas_df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = pandas_df.select_dtypes(include=['object', 'string']).columns.tolist()
        
        stats.update({
            'numeric_columns': len(numeric_cols),
            'categorical_columns': len(categorical_cols),
            'complex_columns_converted': any('json' in str(col).lower() for col in pandas_df.columns)
        })
        
        return stats

def show_simplified_custom_query_page():
    """Mostra la pagina semplificata per query personalizzate"""
    
    if not hasattr(st.session_state, 'datasets_loaded') or not st.session_state.datasets_loaded:
        st.warning("⚠️ Nessun dataset caricato. Carica i dati per iniziare l'analisi.")
        return
    
    dataset = st.session_state.data
    
    if 'enhanced_query_engine' not in st.session_state:
        st.session_state.enhanced_query_engine = EnhancedQueryEngine()
    query_engine = st.session_state.enhanced_query_engine

    try:
        view_name = DatabaseConfig.TEMP_VIEW_NAME
        dataset.createOrReplaceTempView(view_name)
        
    except Exception as e:
        st.error(f"❌ Errore critico: impossibile registrare la vista temporanea: {e}")
        return

    show_dataset_info_safe(st.session_state.data)

    tab1, tab2 = st.tabs(["📝 Editor & Template", "📜 Cronologia"])
    
    with tab1:
        show_simplified_editor_tab(query_engine, dataset)
    
    with tab2:
        show_history_tab(query_engine)
    
    if st.session_state.get('show_save_form', False):
        show_save_template_sidebar(
            st.session_state.get('save_form_query', ''),
            st.session_state.get('save_form_mode', 'new'),
            st.session_state.get('save_form_existing_data', None)
        )


def show_dataset_info_safe(dataset):
    """Mostra informazioni dataset con gestione errori in una singola colonna."""
    with st.expander("Informazioni Dataset"):
        with st.spinner("Caricamento informazioni dataset..."):
            if dataset is not None:
                # Recupero delle generalità
                try:
                    view_name = DatabaseConfig.TEMP_VIEW_NAME
                except NameError:
                    view_name = "N/A"
                    st.warning("⚠️ `DatabaseConfig.TEMP_VIEW_NAME` non definita.")
                
                if 'row_count' not in st.session_state:
                    try:
                        st.session_state.row_count = dataset.count()
                    except Exception as e:
                        st.session_state.row_count = f"Errore: {e}"
                        st.error(f"❌ Errore nel conteggio: {e}")
                
                row_count = st.session_state.row_count
                
                st.write("") 
                st.write("") 
                # --- Sezione Generalità ---
                col0, col1, col2, col3, col5 = st.columns([0.5, 1, 1, 1, 0.5])

                with col1:
                    st.markdown(f"Nome Tabella SQL: `{view_name}`")
            
                with col2:
                    formatted_rows = f"{row_count:,}" if isinstance(row_count, int) else row_count
                    st.markdown(f"Numero Righe: `{formatted_rows}`")

                with col3:
                    st.markdown(f"Numero Colonne: `{len(dataset.columns)}`")
                    
                st.markdown("---")

                # --- Sezione Schema Colonne ---
                st.subheader("Schema Colonne")
                try:
                    schema_list = []
                    complex_fields = {}
                    processed_complex_types = set()

                    # Prendi il primo record per ispezionare i dati complessi
                    first_row = dataset.take(1)
                    if first_row:
                        first_row = first_row[0]
                    else:
                        first_row = None

                    # Itera sui campi dello schema per raccogliere info e campi complessi
                    for field in dataset.schema.fields:
                        col_name = field.name
                        col_type = field.dataType.simpleString()
                        is_complex = RobustDataConverter._is_complex_type(field.dataType)

                        display_type = "Tipo Complesso - Vedi dettagli sotto" if is_complex else col_type

                        schema_list.append({
                            "Nome Colonna": col_name,
                            "Tipo di Dato": display_type,
                        })

                        if is_complex and col_name not in processed_complex_types:
                            complex_fields[col_name] = field.dataType
                            processed_complex_types.add(col_name)

                    # Mostra la tabella dello schema del DataFrame principale
                    schema_df = pd.DataFrame(schema_list)
                    st.dataframe(schema_df, width='stretch')

                    st.markdown("---")

                    # --- Sezione Dettagli Campi Complessi (una tabella per ciascuno, senza ripetizioni) ---
                    st.subheader("Dettagli Campi Complessi")
                    if not complex_fields:
                        st.info("ℹ️ Nessun campo complesso (STRUCT o ARRAY) trovato nel dataset principale.")
                    else:
                        for col_name, complex_type in complex_fields.items():
                            st.markdown(f"**Campo:** `{col_name}`")
                            
                            # Genera una tabella per lo schema del campo complesso
                            details_list = []
                            if isinstance(complex_type, StructType):
                                for sub_field in complex_type.fields:
                                    is_complex = RobustDataConverter._is_complex_type(sub_field.dataType)
                                    display_type = "Tipo Complesso - Vedi dettagli nella relativa tabella" if is_complex else sub_field.dataType.simpleString()
                                    details_list.append({
                                        "Nome Campo": sub_field.name,
                                        "Tipo di Dato": display_type
                                    })
                            elif isinstance(complex_type, ArrayType):
                                element_type = complex_type.elementType
                                if isinstance(element_type, StructType):
                                    for sub_field in element_type.fields:
                                        is_complex = RobustDataConverter._is_complex_type(sub_field.dataType)
                                        display_type = "Tipo Complesso - Vedi dettagli nella relativa tabella" if is_complex else sub_field.dataType.simpleString()
                                        details_list.append({
                                            "Nome Campo": sub_field.name,
                                            "Tipo di Dato": display_type
                                        })
                                else:
                                    details_list.append({
                                        "Nome Campo": "element",
                                        "Tipo di Dato": element_type.simpleString()
                                    })
                            
                            details_df = pd.DataFrame(details_list)
                            st.dataframe(details_df, width='stretch')
                            st.markdown("---")

                except Exception as e:
                    st.error(f"❌ Errore nella lettura dello schema o dei campi complessi: {e}")
                    st.write("Colonne disponibili:", ", ".join(dataset.columns[:10]))
    
def show_simplified_editor_tab(query_engine, dataset):
    """Tab editor semplificato con template integrati - VERSIONE CORRETTA V2"""
    
    st.markdown("#### 📚 Query Predefinite")
    
    all_templates = get_twitter_query_templates()
    
    for category, queries in all_templates.items():
        with st.expander(f"**{category}**", expanded=False):
            num_queries = len(queries)
            for i, (name, query_data) in enumerate(queries.items()):
                
                query_text = query_data.get('query', '# Errore: Query non trovata')
                charts_config = query_data.get('charts', [])
                ml_config = query_data.get('ml_algorithms', [])

                st.markdown(f"##### {name}")
                
                col_query, col_charts, col_edit = st.columns([4, 3, 1])
                
                with col_query:
                    st.code(query_text, language="sql")
                
                with col_charts:
                    if charts_config:
                        with st.container(border=True):
                            st.markdown("**📊 Grafici Predefiniti:**")
                            for chart in charts_config:
                                chart_type = chart.get('type', 'N/D')
                                x_axis = chart.get('x', 'N/D')
                                y_axis = chart.get('y', 'N/D')
                                z_axis = chart.get('z', 'N/D')
                                
                                if chart_type in ["Barre", "Linee"]:
                                    if y_axis:
                                        st.markdown(
                                            f"- Grafico a {chart_type}\n"
                                            f"  - x: `{x_axis}`\n"
                                            f"  - y: `{y_axis}`"
                                        )
                                    else:
                                        st.markdown(
                                            f"- Grafico a {chart_type}\n"
                                            f"  - x: `{x_axis}`"
                                        )
                                elif chart_type == "Heatmap":
                                    agg_func = chart.get('agg', 'N/D') 
                                    if z_axis != 'N/D' and agg_func != 'N/D':
                                        st.markdown(
                                            f"- {chart_type}\n"
                                            f"  - x: `{x_axis}`\n"
                                            f"  - y: `{y_axis}`\n"
                                            f"  - z: `{z_axis}`\n"
                                            f"  - Aggregazione: `{agg_func}`"
                                        )
                                    elif agg_func == 'N/D' and z_axis != 'N/D':
                                        st.markdown(
                                            f"- {chart_type}\n"
                                            f"  - x: `{x_axis}`\n"
                                            f"  - y: `{y_axis}`\n"
                                            f"  - z: `{z_axis}`"
                                        )
                                    else:
                                        st.markdown(
                                            f"- {chart_type}\n"
                                            f"  - x: `{x_axis}`\n"
                                            f"  - y: `{y_axis}`"
                                        )
                                else:
                                    if y_axis:
                                        st.markdown(
                                            f"- Grafico a {chart_type}\n"
                                            f"  - Categoria: `{x_axis}`\n"
                                            f"  - Valori: `{y_axis}`"
                                        )
                                    else:
                                        st.markdown(
                                            f"- Grafico a {chart_type}\n"
                                            f"  - Categoria: `{x_axis}`"
                                        )
                    else:
                        st.info("Nessun grafico predefinito.")

                    if ml_config:
                        with st.container(border=True):
                            st.markdown("**🤖 Algoritmi ML Predefiniti:**")
                            for ml in ml_config:
                                alg_name = ml.get('algorithm', 'N/D')
                                features = ml.get('features', [])
                                target = ml.get('target')

                                display_text = f"- **{alg_name}**"
                                if target:
                                    display_text += f"\n  - Target: `{target}`"                                    
                                else:
                                    if features:
                                        if len(features) < 5:
                                            feature_display = ', '.join(f'`{f}`' for f in features)
                                            display_text += f"\n  - Features: {feature_display}"
                                        else:
                                            display_text += f"\n  - Features: `{len(features)}`"
                                    
                                st.markdown(display_text)
                    else:
                        st.info("Nessun algoritmo di ML predefinito.")

                with col_edit:
                    execute_btn = st.button("▶️", key=f"template_{category}_{name}", help="Use query")
                    if execute_btn:
                        # CORREZIONE CHIAVE: Salva la query da eseguire in modo persistente
                        st.session_state.pending_query = query_text
                        st.session_state.query_timestamp = pd.Timestamp.now().isoformat()
                        st.session_state.execute_pending = True
                        st.rerun()
                    
                    if st.button("✏️", key=f"edit_{category}_{name}", help="Modifica query"):
                        edit_data = {
                            'category': category,
                            'name': name,
                            'query': query_text,
                            'charts': charts_config,
                            'ml_algorithms': ml_config
                        }
                        open_save_form(query_text, mode='edit', existing_data=edit_data)
                    
                    if st.button("🗑️", key=f"delete_{category}_{name}", help="Elimina query"):
                        show_delete_confirmation_dialog(category, name)
                
                if i < num_queries - 1:
                    st.markdown("---")

    
    st.markdown("### ✏️ Editor SQL")
    default_query = f"SELECT * FROM {DatabaseConfig.TEMP_VIEW_NAME}"
    
    if 'pending_query' in st.session_state:
        default_query = st.session_state.pending_query
        st.success("✅ Template caricato! Attendi l'esecuzione della query...")
    
    text_area_key = f"query_editor_{st.session_state.get('query_timestamp', 'default')}"
    
    query_text = st.text_area(
        "Query SQL:",
        value=default_query,
        height=150,
        key=text_area_key,
        help=f"Scrivi la tua query SQL. Usa '{DatabaseConfig.TEMP_VIEW_NAME}' come nome della tabella."
    )
    
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    with col1:
        execute_button = st.button("🚀 Esegui Query", type="primary")
    
    with col2:
        limit_results = st.checkbox("Limita Risultati", value=True)
        if limit_results:
            result_limit = st.number_input("Limite:", min_value=10, max_value=10000, value=1000)
        else:
            st.write("")
            result_limit = None
    with col3:
        validate_button = st.button("✅ Valida Query")
    with col4:
        save_template_button = st.button("💾 Salva Query")
    
    if save_template_button:
        if query_text.strip():
            open_save_form(query_text)
        else:
            st.error("❌ Scrivi una query prima di salvarla!")
    
    if validate_button:
        with st.spinner("🔍 Validazione query..."):
            validation = query_engine.validate_query(query_text)
            if validation['valid']:
                st.success("✅ Query sintatticamente corretta!")
                if 'message' in validation:
                    st.info(f"📝 {validation['message']}")
            else:
                st.error(f"❌ Errori nella query: {validation['error']}")
                if 'suggestion' in validation:
                    st.info(f"💡 Suggerimento: {validation['suggestion']}")

    should_execute = False
    
    if execute_button:
        should_execute = True
        if 'pending_query' in st.session_state:
            del st.session_state.pending_query
        if 'execute_pending' in st.session_state:
            del st.session_state.execute_pending
    
    elif st.session_state.get('execute_pending', False):
        should_execute = True
        if 'execute_pending' in st.session_state:
            del st.session_state.execute_pending
    
    if should_execute:
        if not query_text.strip():
            st.error("⚠️ Inserisci una query SQL valida")
            return
        
        with st.spinner("⚡ Esecuzione query in corso..."):
            result = query_engine.execute_custom_query_safe(query_text, result_limit)
            
            # Dopo l'esecuzione, pulisci pending_query
            if 'pending_query' in st.session_state:
                del st.session_state.pending_query
            
            if result['success']:
                data = result['data']
                stats = result['stats']
                st.session_state.last_query_result = data
                st.session_state.last_query_text = query_text
                st.session_state.last_query_stats = stats
                
                if result['warning']:
                    st.warning(f"⚠️ {result['warning']}")
            else:
                st.error(f"❌ {result['error']}")
                if result.get('error'):
                    show_error_suggestions(result['error'], query_text)
    
    # Visualizzazione risultati
    if 'last_query_result' in st.session_state:
        st.markdown("---")
        st.markdown("### 📊 Risultati")
        result_data = st.session_state.last_query_result
        stats = st.session_state.last_query_stats
        saved_query_text = st.session_state.last_query_text
        total_rows = stats.get('total_rows')

        col1, col2, col3, col4 = st.columns(4)
        label = "Righe Caricate / Totali"
        if isinstance(total_rows, int) and result_limit and result_limit < total_rows:
            value = f"{result_limit:,} / {total_rows:,}"
        elif isinstance(total_rows, int):
            value = f"{total_rows:,} / {total_rows:,}"
        else:
            value = f"{len(result_data):,}"
        
        with col1:
            st.metric(label=label, value=value)
        with col2:
            st.metric("Colonne", len(result_data.columns))
        with col3:
            memory_mb = result_data.memory_usage(deep=True).sum() / 1024**2
            st.metric("Memoria Utilizzata", f"{memory_mb:.1f} MB")
        with col4:
            null_count = result_data.isnull().sum().sum()
            st.metric("Valori Null", null_count)
        
        column1, column2, column3 = st.columns(3)
        
        with column1:
            show_all = st.checkbox("Mostra tutte le righe", key="show_all_results", value=False)
        
        with column2:
            if not show_all:
                if limit_results and isinstance(total_rows, int):
                    max_rows = min(result_limit, total_rows)
                elif isinstance(total_rows, int):
                    max_rows = min(10000, total_rows)
                else:
                    max_rows = len(result_data)

                display_limit = st.slider("Righe da mostrare:", 10, max(10, max_rows), min(100, max_rows), key="results_slider")
            else:
                display_limit = len(result_data)
        
        with column3:
            search_term = st.text_input("🔍 Cerca nei risultati:", placeholder="Termine di ricerca...")
        
        display_data = result_data.copy()
        if result_limit and len(display_data) > result_limit:
            display_data = display_data.head(result_limit)

        if search_term:
            text_cols = display_data.select_dtypes(include=['object', 'string']).columns
            if len(text_cols) > 0:
                mask = display_data[text_cols].astype(str).apply(
                    lambda x: x.str.contains(search_term, case=False, na=False)
                ).any(axis=1)
                display_data = display_data[mask]
                st.info(f"🔍 Trovate {len(display_data)} righe contenenti '{search_term}'")

        if show_all:
            st.dataframe(display_data, width='stretch')
        else:
            st.dataframe(display_data.head(display_limit), width='stretch')
            
            if len(display_data) > display_limit:
                st.info(f"Mostrando {display_limit} di {len(display_data)} righe.")

        if saved_query_text and has_predefined_analytics(saved_query_text):
            with st.expander("🔬 Analisi Dati"):
                show_analytics_page()
        else:
            st.info("ℹ️ Non ci sono grafici o algoritmi di ML personalizzati: usa il pulsante 'Salva Query' per aggiungere grafici e analisi.")

def show_delete_confirmation_dialog(category: str, name: str):
    """Mostra dialog di conferma per eliminazione query"""
    
    @st.dialog(f"🗑️ Elimina Query: {name}")
    def delete_confirmation_dialog():
        st.warning(f"⚠️ Sei sicuro di voler eliminare la query **'{name}'** dalla categoria **'{category}'**?")
        st.write("Questa azione non può essere annullata.")
        
        col1, col2 = st.columns(2)
        
        deleted = False

        with col1:
            if st.button("✅ Sì, elimina", type="primary", width='stretch'):
                # Procedi con l'eliminazione
                try:
                    file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'custom_query.json')
                    templates = get_twitter_query_templates()
                    
                    # Rimuovi la query
                    if category in templates and name in templates[category]:
                        del templates[category][name]
                        
                        # Se la categoria è vuota, rimuovila
                        if not templates[category]:
                            del templates[category]
                        
                        # Salva il file aggiornato
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(templates, f, indent=4, ensure_ascii=False)
                        
                        deleted = True
                        
                        # Aggiorna anche la sessione se presente
                        if ('custom_templates' in st.session_state and 
                            category in st.session_state.custom_templates and 
                            name in st.session_state.custom_templates[category]):
                            del st.session_state.custom_templates[category][name]
                            if not st.session_state.custom_templates[category]:
                                del st.session_state.custom_templates[category]
                        
                    else:
                        st.error("❌ Query non trovata nei template.")
                    
                except Exception as e:
                    st.error(f"❌ Errore durante l'eliminazione: {e}")
                
                import time
                time.sleep(1)
                st.rerun()
        
        with col2:
            if st.button("❌ Annulla", width='stretch'):
                st.rerun()

        if deleted:
            st.success(f"✅ Query '{name}' eliminata con successo!")
    
    delete_confirmation_dialog()


def show_save_template_sidebar(query_text: str, mode: str = 'new', existing_data: Dict = None):
    """Mostra form nella sidebar per salvare o modificare un template personalizzato."""
    
    # Controlla se il form deve essere mostrato
    show_form = st.session_state.get('show_save_form', False)
    
    if not show_form:
        return
    
    with st.sidebar:
        st.markdown('<div class="sidebar-content">', unsafe_allow_html=True)
        form_title = "💾 Salva Nuova Query" if mode == 'new' else "✏️ Modifica Query"
        st.markdown(f"### {form_title}")
        
        current_query = query_text

        # Inizializzazione dati per modalità edit
        if mode == 'edit' and existing_data:
            if 'form_initialized' not in st.session_state or not st.session_state['form_initialized']:
                # Se siamo in modifica, FORZA il caricamento dei dati esistenti
                st.session_state['charts_config'] = existing_data.get('charts', [])
                st.session_state['ml_config'] = existing_data.get('ml_algorithms', [])
                st.session_state['form_initialized'] = True
                
            default_category = existing_data.get('category', '')
            default_name = existing_data.get('name', '')
            query_to_save = existing_data.get('query', query_text)
            
            st.info(f"Stai modificando la query '{default_name}' nella categoria '{default_category}'.")
            
            # if st.session_state.get('charts_config', []):
            #     st.success(f"📊 Caricati {len(st.session_state['charts_config'])} grafici esistenti")
            # if st.session_state.get('ml_config', []):
            #     st.success(f"🤖 Caricati {len(st.session_state['ml_config'])} algoritmi ML esistenti")
        else:
            # Modalità new - inizializza solo se non esistono già
            st.session_state.setdefault('charts_config', [])
            st.session_state.setdefault('ml_config', [])
            st.session_state.setdefault('form_initialized', True)
            default_category = ""
            default_name = ""
            query_to_save = query_text

        if not current_query.strip():
            st.error("❌ Nessuna query presente nell'editor!")
            return
        
        # Sezione categoria e nome
        
        category_type = st.radio(
            "Tipo categoria:",
            ["Esistente", "Nuova"],
            help="Scegli se usare una categoria esistente o crearne una nuova",
            key="sidebar_category_radio_choice"
        )

        category = ""
        all_templates = get_twitter_query_templates()
        existing_categories = list(all_templates.keys())

        if category_type == "Esistente":
            if existing_categories:
                # In modalità edit, pre-seleziona la categoria corrente se esiste
                default_index = 0
                if mode == 'edit' and default_category in existing_categories:
                    default_index = existing_categories.index(default_category)
                
                category = st.selectbox(
                    "Categoria:", 
                    existing_categories, 
                    index=default_index,
                    key="sidebar_existing_category_select"
                )
            else:
                st.info("Nessuna categoria esistente trovata. Sarà creata una nuova categoria.")
                category = st.text_input(
                    "Nome della nuova categoria:", 
                    value=default_category if mode == 'edit' else "Custom", 
                    key="sidebar_new_category_text_1"
                )
        else:
            category = st.text_input(
                "Nome della nuova categoria:", 
                value=default_category if mode == 'edit' else "",
                placeholder="es. Analisi Custom", 
                key="sidebar_new_category_text_2"
            )

        template_name = st.text_input(
            "Nome query:", 
            value=default_name if mode == 'edit' else "",
            placeholder="es. example query",
            key="sidebar_template_name"
        )
        
        modified_query = ""
        with st.expander("📝 Query", expanded=False):
            if mode == 'edit' and existing_data:
                modified_query = st.text_area(label="Query da modificare:", value=query_to_save, height=150, disabled=False)
            else:
                modified_query = current_query
                st.code(current_query, language="sql")

        try:
            columns = get_query_columns(current_query, st.session_state.spark_manager.get_spark_session())
        except Exception:
            columns = []

        st.divider()
        
        # ----------------- SEZIONE GRAFICI -----------------
        st.markdown("#### 📊 Configurazione Grafici")

        # Pulsante per aggiungere grafico
        if st.button("➕ Aggiungi grafico", key="sidebar_add_chart_btn"):
            st.session_state.charts_config.append({"type": "Barre", "x": None, "y": None})
            st.rerun()

        chart_types = ["Barre", "Linee", "Torta", "Heatmap"]

        # Visualizza tutti i grafici configurati
        if st.session_state.get('charts_config', []):
            for i, cfg in enumerate(st.session_state.charts_config):
                with st.expander(f"📊 Grafico {i+1}", expanded=False):
                    # Tipo di grafico
                    type_index = 0
                    if cfg.get("type") and cfg["type"] in chart_types:
                        type_index = chart_types.index(cfg["type"])
                    
                    new_type = st.selectbox(
                        "Tipo:",
                        chart_types,
                        index=type_index,
                        key=f"sidebar_chart_type_{i}"
                    )
                    cfg["type"] = new_type

                    if columns:
                        # Asse X
                        x_index = 0
                        if cfg.get("x") and cfg["x"] in columns:
                            x_index = columns.index(cfg["x"])

                        is_pie_chart = (new_type == "Torta")
                        x_label = "Categorie:" if is_pie_chart else "Asse X:"
                        
                        new_x = st.selectbox(
                            x_label,
                            columns,
                            index=x_index,
                            key=f"sidebar_x_col_{i}"
                        )
                        cfg["x"] = new_x

                        # Asse Y
                        y_label = "Valori (opz.):" if is_pie_chart else "Asse Y:"
                        y_options = ([""] + columns) if is_pie_chart else columns

                        # Calcola l'indice iniziale correttamente
                        y_index = 0
                        if cfg.get("y") and cfg["y"] in columns:
                            y_index = y_options.index(cfg["y"])

                        # 2. Ora il selettore usa le variabili corrette
                        new_y = st.selectbox(
                            y_label,
                            y_options,
                            index=y_index,
                            key=f"sidebar_y_col_{i}"
                        )
                        cfg["y"] = new_y if new_y != "" else None
                        if new_type == "Heatmap":
                            z_label = "Valore (opzionale):"
                            z_options = [""] + columns

                            z_index = 0
                            if cfg.get("z") and cfg["z"] in z_options:
                                z_index = z_options.index(cfg["z"])

                            new_z = st.selectbox(
                                z_label,
                                z_options,
                                index=z_index,
                                key=f"sidebar_z_col_{i}",
                                help="Seleziona la colonna per il valore/colore della heatmap. Lascia vuoto per una heatmap di frequenza."
                            )
                            cfg["z"] = new_z if new_z != "" else None
                            if new_z:
                                agg_label = "Metodo di aggregazione:"
                                agg_options = ['mean', 'max', 'sum', 'count']
                                
                                default_agg = cfg.get('agg', 'max')
                                agg_index = agg_options.index(default_agg) if default_agg in agg_options else 0
                                
                                new_agg = st.selectbox(
                                    agg_label,
                                    agg_options,
                                    index=agg_index,
                                    key=f"sidebar_agg_func_{i}",
                                    help="Come aggregare più valori che cadono nella stessa cella (x, y)."
                                )
                                cfg['agg'] = new_agg
                            elif 'agg' in cfg:
                                del cfg['agg']
                        else:
                            if 'z' in cfg:
                                del cfg['z']
                            if 'agg' in cfg:
                                del cfg['agg']
                    else:
                        st.warning("⚠️ Impossibile determinare le colonne dalla query.")
                    
                    # Pulsante rimozione
                    if st.button("🗑️ Rimuovi", key=f"sidebar_remove_chart_{i}"):
                        st.session_state.charts_config.pop(i)
                        st.rerun()
        else:
            st.info("Nessun grafico configurato.")

        st.divider()

        # ----------------- SEZIONE MACHINE LEARNING -----------------
        st.markdown("#### 🤖 Configurazione ML")

        # Pulsante per aggiungere algoritmo ML
        if st.button("➕ Aggiungi algoritmo ML", key="sidebar_add_ml_btn"):
            st.session_state.ml_config.append({
                "algorithm": "K-Means", 
                "features": [], 
                "target": None
            })
            st.rerun()


        ml_algorithms = {
            # Spark MLlib - Clustering
            "K-Means": {"type": "clustering", "engine": "spark"},
            
            # Spark MLlib - Classificazione  
            #"Random Forest Classifier": {"type": "supervised", "engine": "spark"},
            #"GBT Classifier": {"type": "supervised", "engine": "spark"},
            
            # Spark MLlib - Regressione
            #"Linear Regression": {"type": "supervised", "engine": "spark"},
            
            # Scikit-learn - Altri algoritmi
            "DBSCAN": {"type": "clustering", "engine": "sklearn"},
            "Isolation Forest": {"type": "anomaly", "engine": "sklearn"}
        }

        
        # Visualizza tutti gli algoritmi ML configurati
        if st.session_state.get('ml_config', []):
            for i, ml_cfg in enumerate(st.session_state.ml_config):
                algorithm = ml_cfg.get('algorithm', 'K-Means')
                
                with st.expander(f"🤖 ML {i+1} - {algorithm}", expanded=False):
                    # Selezione algoritmo
                    algorithm_options = list(ml_algorithms.keys())
                    current_alg = ml_cfg.get("algorithm", "K-Means")
                    if current_alg not in algorithm_options:
                        current_alg = "K-Means"
                    
                    alg_index = algorithm_options.index(current_alg)
                    new_algorithm = st.selectbox(
                        "Algoritmo:",
                        algorithm_options,
                        index=alg_index,
                        key=f"sidebar_ml_algorithm_{i}"
                    )
                    ml_cfg["algorithm"] = new_algorithm

                    selected_algorithm = ml_cfg["algorithm"]
                    algorithm_info = ml_algorithms[selected_algorithm]

                    if columns:
                        if algorithm_info["type"] != "supervised":
                            current_features = ml_cfg.get("features", [])
                            valid_features = [f for f in current_features if f in columns]
                            
                            new_features = st.multiselect(
                                "Features:",
                                columns,
                                default=valid_features,
                                key=f"sidebar_ml_features_{i}",
                                help="Seleziona le colonne da utilizzare come input"
                            )
                            ml_cfg["features"] = new_features
                            ml_cfg["target"] = None # Assicura che il target sia nullo
                        
                        else:
                            # Messaggio per l'utente
                            st.info("Tutte le colonne verranno usate come features (esclusa la colonna target). La selezione è automatica.")
                            
                            # Selettore per il target (come prima)
                            target_options = [""] + columns
                            current_target = ml_cfg.get("target", "")
                            target_index = 0
                            if current_target and current_target in columns:
                                target_index = target_options.index(current_target)
                            
                            new_target = st.selectbox(
                                "Target:",
                                target_options,
                                index=target_index,
                                key=f"sidebar_ml_target_{i}",
                                help="Variabile dipendente per la predizione"
                            )
                            ml_cfg["target"] = new_target if new_target != "" else None

                            # NUOVA LOGICA: Popola automaticamente le features
                            if ml_cfg["target"]:
                                # Escludi la colonna target dalla lista di tutte le colonne
                                feature_list = [col for col in columns if col != ml_cfg["target"]]
                                ml_cfg["features"] = feature_list
                            else:
                                # Se non c'è un target, la lista features è vuota (verrà bloccata dalla validazione)
                                ml_cfg["features"] = []
                    else:
                        st.warning("⚠️ Impossibile determinare le colonne dalla query.")
                    
                    # Pulsante rimozione
                    if st.button("🗑️ Rimuovi", key=f"sidebar_remove_ml_{i}"):
                        st.session_state.ml_config.pop(i)
                        st.rerun()
        else:
            st.info("Nessun algoritmo ML configurato.")

        st.markdown('</div>', unsafe_allow_html=True)
        
        # ----------------- PULSANTI SALVATAGGIO -----------------        
        col_save, col_cancel = st.columns(2)

        with col_save:
            save_button_text = "💾 Salva Query" if mode == 'edit' else "💾 Salva Query"
            save_clicked = st.button(save_button_text, type="primary", key="sidebar_save_btn")

        with col_cancel:
            cancel_clicked = st.button("❌ Annulla", key="sidebar_cancel_btn")

        st.markdown('<div class="sidebar-footer">', unsafe_allow_html=True)
        st.sidebar.markdown("""
            <div style='text-align: center; color: gray; font-size: 0.8em;'>
            🌪️ Disaster Analysis App<br>
            Powered by Apache Spark & Streamlit
            </div>
        """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
                
        # Gestione click pulsanti
        if save_clicked:
            if not category.strip() or not template_name.strip():
                st.error("❌ Categoria e nome template sono obbligatori!")
                return
            
            # Validazione algoritmi ML
            for i, ml_cfg in enumerate(st.session_state.get('ml_config', [])):
                if not ml_cfg.get("features"):
                    st.error(f"❌ L'algoritmo ML {i+1} deve avere almeno una feature selezionata!")
                    return
                
                if ml_algorithms[ml_cfg["algorithm"]]["type"] != "supervised" and not ml_cfg.get("features"):
                    st.error(f"❌ L'algoritmo ML '{ml_cfg['algorithm']}' deve avere almeno una feature selezionata!")
                    return
                
                # Il controllo del target rimane valido solo per i supervisionati
                if ml_algorithms[ml_cfg["algorithm"]]["type"] == "supervised" and not ml_cfg.get("target"):
                    st.error(f"❌ L'algoritmo supervisionato '{ml_cfg['algorithm']}' deve avere un target!")
                    return
            
            # Gestione modifica (rimozione del vecchio template se cambiato nome/categoria)
            if mode == 'edit' and existing_data:
                original_category = existing_data.get('category', '')
                original_name = existing_data.get('name', '')
                
                if original_category != category or original_name != template_name:
                    try:
                        file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'custom_query.json')
                        templates_to_update = get_twitter_query_templates()
                        
                        if (original_category in templates_to_update and 
                            original_name in templates_to_update[original_category]):
                            del templates_to_update[original_category][original_name]
                            
                            if not templates_to_update[original_category]:
                                del templates_to_update[original_category]
                            
                            with open(file_path, 'w', encoding='utf-8') as f:
                                json.dump(templates_to_update, f, indent=4, ensure_ascii=False)
                    except Exception as e:
                        st.warning(f"⚠️ Avviso durante la rimozione del template originale: {e}")
            
            # Salvataggio del nuovo/modificato template
            if 'custom_templates' not in st.session_state:
                st.session_state.custom_templates = {}
            
            if category not in st.session_state.custom_templates:
                st.session_state.custom_templates[category] = {}
            
            st.session_state.custom_templates[category][template_name] = {
                'query': current_query,
                'created_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
                'charts': st.session_state.get('charts_config', []),
                'ml_algorithms': st.session_state.get('ml_config', [])
            }

            file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'custom_query.json')
            templates_to_save = get_twitter_query_templates()
            
            if category not in templates_to_save:
                templates_to_save[category] = {}

            current_query_formatted = format_sql_for_json(modified_query)

            templates_to_save[category][template_name] = {
                "query": current_query_formatted,
                "charts": st.session_state.get('charts_config', []),
                "ml_algorithms": st.session_state.get('ml_config', [])
            }
            
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(templates_to_save, f, indent=4, ensure_ascii=False)
                
                success_message = (f"✅ Template '{template_name}' modificato con successo!" 
                                 if mode == 'edit' 
                                 else f"✅ Template '{template_name}' salvato nella categoria '{category}'!")
                st.success(success_message)
                
                # Chiudi il form dopo il salvataggio
                st.session_state.show_save_form = False
                if 'charts_config' in st.session_state:
                    del st.session_state['charts_config']
                if 'ml_config' in st.session_state:
                    del st.session_state['ml_config']
                if 'form_initialized' in st.session_state:
                    del st.session_state['form_initialized']
                
                st.rerun()
                
            except Exception as e:
                st.error(f"❌ Errore durante il salvataggio del file: {e}")
        
        force_close_sidebar()

        if cancel_clicked:
            st.session_state.show_save_form = False
            # Pulisci le configurazioni temporanee
            if 'charts_config' in st.session_state:
                del st.session_state['charts_config']
            if 'ml_config' in st.session_state:
                del st.session_state['ml_config']
            if 'form_initialized' in st.session_state:
                del st.session_state['form_initialized']
            force_close_sidebar()
            st.rerun()

def open_save_form(query_text: str, mode: str = 'new', existing_data: Dict = None):
    """Apre il form di salvataggio nella sidebar."""
    st.info("Apri la sidebar a sinistra per continuare.")
    st.session_state.show_save_form = True
    st.session_state.save_form_query = query_text
    st.session_state.save_form_mode = mode
    st.session_state.save_form_existing_data = existing_data
    st.session_state.form_initialized = False  # Reset per permettere re-inizializzazione
    force_open_sidebar()
    #st.rerun()

def normalize_query_for_comparison(query: str) -> str:
    """Normalizza una query per il confronto."""
    if not query: 
        return ""
    # Ho corretto la tua regex per rimuovere i commenti SQL in modo più robusto
    query = re.sub(r"--.*", "", query)
    query = re.sub(r'\s+', ' ', query.strip())
    return query.lower()

def find_predefined_config(query_text: str, config_key: str) -> List[Dict[str, Any]]:
    """Trova configurazioni predefinite (grafici o ML) per una query."""
    if not query_text.strip(): return []
    query_normalized = normalize_query_for_comparison(query_text)
    
    try:
        all_templates = get_twitter_query_templates()
        for category, queries in all_templates.items():
            for name, query_data in queries.items():
                if isinstance(query_data, dict) and 'query' in query_data:
                    template_normalized = normalize_query_for_comparison(query_data.get('query', ''))
                    if query_normalized == template_normalized:
                        return query_data.get(config_key, [])
    except Exception as e:
        print(f"Errore nel caricamento delle configurazioni predefinite: {e}")
    return []

def has_predefined_analytics(query_text: str) -> bool:
    """
    Funzione helper che controlla se esistono configurazioni di grafici O ML 
    per una data query. È più efficiente perché si ferma alla prima trovata.
    """
    if find_predefined_config(query_text, 'charts'):
        return True
    if find_predefined_config(query_text, 'ml_algorithms'):
        return True
    return False

def is_numeric_column(column_name: str, query: str = None, spark_session=None) -> bool:
    """
    Determina se una colonna è numerica utilizzando diversi metodi:
    1. Metadati dal Spark DataFrame (se disponibile)
    2. Analisi del nome della colonna
    3. Pattern comuni nei nomi delle colonne
    """
    
    if spark_session and query:
        try:
            limited_query = f"SELECT * FROM ({query}) LIMIT 1"
            df = spark_session.sql(limited_query)
            
            for field in df.schema.fields:
                if field.name.lower() == column_name.lower():
                    field_type = str(field.dataType).lower()
                    numeric_types = ['int', 'integer', 'long', 'float', 'double', 'decimal', 'bigint', 'smallint', 'tinyint']
                    return any(num_type in field_type for num_type in numeric_types)
        except Exception:
            pass
    
    column_lower = column_name.lower().strip()
    
    numeric_keywords = [
        'count', 'cnt', 'num', 'number', 'total', 'sum', 'amount',
        'avg', 'average', 'mean', 'median', 'min', 'max', 'std', 'variance',
        'price', 'cost', 'revenue', 'profit', 'sales', 'budget',
        'score', 'rating', 'rank', 'points', 'grade', 'percentage', 'percent',
        'id', 'index', 'key', 'code',
        'size', 'length', 'width', 'height', 'weight', 'volume', 'area',
        'year', 'month', 'day', 'hour', 'minute', 'second', 'timestamp',
        'balance', 'debt', 'credit', 'interest', 'rate', 'value', 'worth',
        'likes', 'shares', 'comments', 'retweets', 'followers', 'following',
        'views', 'impressions', 'clicks', 'engagement'
    ]
    
    if any(keyword in column_lower for keyword in numeric_keywords):
        return True
    
    numeric_patterns = [
        r'.*_cnt$',
        r'.*_count$',
        r'.*_num$',
        r'.*_id$',
        r'.*_amount$',
        r'.*_total$',
        r'.*_sum$',
        r'.*_avg$',
        r'.*_rate$',
        r'.*_score$',
        r'.*_value$',
        r'^num_.*',
        r'^cnt_.*',
        r'^total_.*',
        r'^avg_.*',
        r'.*\d+.*',
    ]
    
    for pattern in numeric_patterns:
        if re.match(pattern, column_lower):
            return True
    
    text_keywords = [
        'name', 'title', 'description', 'text', 'content', 'message',
        'comment', 'note', 'address', 'email', 'url', 'link',
        'status', 'type', 'category', 'tag', 'label', 'class',
        'username', 'user_name', 'full_name', 'first_name', 'last_name'
    ]
    
    if any(keyword in column_lower for keyword in text_keywords):
        return False
    return True


def get_default_param_value(param_name: str):
    """
    Restituisce i valori di default per i parametri ML.
    Ora contiene solo i parametri che l'utente può configurare manualmente.
    """
    defaults = {
        'n_components': 2
    }
    return defaults.get(param_name, 1)

def show_delete_confirmation_dialog(category: str, name: str):
    """Mostra dialog di conferma per eliminazione query"""
    
    @st.dialog(f"🗑️ Elimina Query: {name}")
    def delete_confirmation_dialog():
        st.warning(f"⚠️ Sei sicuro di voler eliminare la query **'{name}'** dalla categoria **'{category}'**?")
        st.write("Questa azione non può essere annullata.")
        
        col1, col2 = st.columns(2)
        
        deleted = False

        with col1:
            if st.button("✅ Sì, elimina", type="primary", width='stretch'):
                # Procedi con l'eliminazione
                try:
                    file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'custom_query.json')
                    templates = get_twitter_query_templates()
                    
                    # Rimuovi la query
                    if category in templates and name in templates[category]:
                        del templates[category][name]
                        
                        # Se la categoria è vuota, rimuovila
                        if not templates[category]:
                            del templates[category]
                        
                        # Salva il file aggiornato
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(templates, f, indent=4, ensure_ascii=False)
                        
                        deleted = True
                        
                        # Aggiorna anche la sessione se presente
                        if ('custom_templates' in st.session_state and 
                            category in st.session_state.custom_templates and 
                            name in st.session_state.custom_templates[category]):
                            del st.session_state.custom_templates[category][name]
                            if not st.session_state.custom_templates[category]:
                                del st.session_state.custom_templates[category]
                        
                    else:
                        st.error("❌ Query non trovata nei template.")
                    
                except Exception as e:
                    st.error(f"❌ Errore durante l'eliminazione: {e}")
                
                import time
                time.sleep(1)
                st.rerun()
        
        with col2:
            if st.button("❌ Annulla", width='stretch'):
                st.rerun()

        if deleted:
            st.success(f"✅ Query '{name}' eliminata con successo!")
    
    delete_confirmation_dialog()

def get_query_columns(query: str, spark) -> list[str]:
    """
    Restituisce la lista di colonne da una query Spark SQL senza eseguire righe.
    """
    try:
        df = spark.sql(query)
        return df.columns
    except Exception as e:
        print(f"Errore estrazione colonne: {e}")
        return []


def format_sql_for_json(sql: str) -> str:
    """
    Formatta SQL con newline leggibili e indentazione minima.
    """
    sql = sql.strip()

    keywords = ["FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT"]
    for kw in keywords:
        sql = re.sub(rf"\s+{kw}\b", f"\n{kw}", sql, flags=re.IGNORECASE)

    sql = re.sub(r"SELECT\s+", "SELECT\n    ", sql, flags=re.IGNORECASE)
    #sql = re.sub(r",\s*", ",\n    ", sql)
    sql = re.sub(r"\n\s+\n", "\n", sql)

    return sql

def show_export_options(result: pd.DataFrame, query_text: str):
    """Opzioni di export dei risultati"""
    
    st.markdown("#### 💾 Export Risultati")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        csv_data = result.to_csv(index=False)
        st.download_button(
            "📥 CSV",
            csv_data,
            file_name=f"query_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            help="Scarica i risultati in formato CSV"
        )
    
    with col2:
        json_data = result.to_json(orient='records', indent=2)
        st.download_button(
            "📥 JSON",
            json_data,
            file_name=f"query_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            help="Scarica i risultati in formato JSON"
        )
    
    with col3:
        sql_export = f"-- Query eseguita il {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n-- Righe restituite: {len(result)}\n\n{query_text};"
        st.download_button(
            "📥 SQL",
            sql_export,
            file_name=f"query_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain",
            help="Scarica la query SQL"
        )
    
    with col4:
        try:
            from io import BytesIO
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                result.to_excel(writer, sheet_name='Query Results', index=False)
            
            st.download_button(
                "📥 Excel",
                excel_buffer.getvalue(),
                file_name=f"query_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Scarica i risultati in formato Excel"
            )
        except ImportError:
            st.button("📥 Excel", disabled=True, help="Installa openpyxl per abilitare l'export Excel")

def show_error_suggestions(error: str, query: str):
    """Mostra suggerimenti basati sugli errori comuni"""
    
    st.markdown("#### 💡 Suggerimenti per la Risoluzione")
    
    error_lower = error.lower()    
    if "table or view not found" in error_lower:
        st.info(f"🔍 Assicurati di usare il nome tabella corretto: `{DatabaseConfig.TEMP_VIEW_NAME}`")    
    elif "column" in error_lower and "not found" in error_lower:
        st.info("🔍 Controlla che i nomi delle colonne siano corretti.")    
    elif "syntax error" in error_lower:
        st.info("🔍 Controlla la sintassi SQL. Assicurati che tutte le parentesi siano bilanciate.")    
    elif "type" in error_lower and "conversion" in error_lower:
        st.info("⚙️ Problema di conversione tipo dati. Prova query più semplici.")   
    elif "memory" in error_lower or "out of" in error_lower:
        st.info("💾 Prova a limitare i risultati o usare filtri WHERE più restrittivi.")
    else:
        st.info("📖 Controlla la sintassi SQL e assicurati che la query sia compatibile con Spark SQL.")

def show_history_tab(query_engine):
    """Tab cronologia"""
    
    st.markdown("### 📜 Cronologia Query")
    
    history = query_engine.get_query_history()
    
    if not history:
        st.info("📝 Nessuna query eseguita ancora. Inizia dal tab 'Editor & Template'!")
        return
    
    st.markdown(f"**Totale query eseguite: {len(history)}**")
    
    # Filtri cronologia
    col1, col2 = st.columns(2)
    with col1:
        show_successful_only = st.checkbox("Solo query riuscite", value=False)
    with col2:
        search_history = st.text_input("🔍 Cerca nella cronologia:", placeholder="Cerca testo...")
    
    # Filtra cronologia
    filtered_history = history.copy()
    
    if show_successful_only:
        filtered_history = [h for h in filtered_history if h.get('success', True)]
    
    if search_history:
        filtered_history = [h for h in filtered_history 
                          if search_history.lower() in h.get('query', '').lower()]
    
    if not filtered_history:
        st.info("🔍 Nessuna query trovata con i filtri attuali")
        return
    
    # Mostra cronologia filtrata
    for i, query_info in enumerate(reversed(filtered_history)):
        index = len(filtered_history) - i
        
        status_icon = "✅" if query_info.get('success', True) else "❌"
        rows = query_info.get('row_count', 'N/A')
        exec_time = query_info.get('execution_time', 0)
        
        with st.expander(f"{status_icon} Query #{index} - {rows} righe" + 
                        (f" - {exec_time:.2f}s" if exec_time > 0 else "")):
            
            col1, col2 = st.columns([4, 1])
            
            with col1:
                st.code(query_info['query'], language="sql")
                
                if query_info.get('timestamp'):
                    st.caption(f"⏰ Eseguita: {query_info['timestamp']}")
                
                if query_info.get('error'):
                    st.error(f"❌ Errore: {query_info['error']}")
            
            with col2:
                st.write(f"**Righe:** {rows}")
                if exec_time > 0:
                    st.write(f"**Tempo:** {exec_time:.2f}s")
                
                if st.button("🔄 Riusa", key=f"history_reuse_{index}"):
                    st.session_state.selected_template = query_info['query']
                    st.success("✅ Query caricata nell'editor!")
                    st.rerun()
    
    # Azioni cronologia
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🗑️ Pulisci Cronologia", type="secondary"):
            if st.button("⚠️ Conferma Eliminazione", type="secondary"):
                query_engine.clear_history()
                st.success("🗑️ Cronologia pulita!")
                st.rerun()
    
    with col2:
        if history:
            # Export cronologia
            history_export = generate_history_export(history)
            st.download_button(
                "📥 Esporta Cronologia",
                history_export,
                file_name=f"query_history_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.sql",
                mime="text/plain"
            )

def generate_history_export(history: List[Dict]) -> str:
    """Genera export della cronologia in formato SQL"""
    
    export_lines = [
        "-- Cronologia Query SQL",
        f"-- Generata il: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"-- Totale query: {len(history)}",
        "--" + "="*60
    ]
    
    for i, query_info in enumerate(history, 1):
        export_lines.extend([
            f"",
            f"-- Query #{i}",
            f"-- Timestamp: {query_info.get('timestamp', 'N/A')}",
            f"-- Righe restituite: {query_info.get('row_count', 'N/A')}",
            f"-- Tempo esecuzione: {query_info.get('execution_time', 0):.2f}s",
            f"-- Stato: {'SUCCESS' if query_info.get('success', True) else 'ERROR'}",
        ])
        
        if query_info.get('error'):
            export_lines.append(f"-- Errore: {query_info['error']}")
        
        export_lines.extend([
            "",
            query_info['query'],
            ";"
        ])
    
    return "\n".join(export_lines)