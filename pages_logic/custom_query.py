"""
Pagina per query SQL personalizzate - Versione semplificata
"""

import streamlit as st
import pandas as pd
from src.analytics import QueryEngine
from src.visualizations import GeneralVisualizations
from src.config import Config, DatabaseConfig
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging
import os, json
import re
import traceback
from typing import Optional, Dict, Any, List, Tuple

logger = logging.getLogger(__name__)

@staticmethod
def _map_to_pandas(iterator):
    """Funzione helper per convertire una partizione in un DataFrame Pandas."""
    try:
        return [pd.DataFrame(list(iterator))]
    except Exception as e:
        logger.error(f"Errore nella conversione di una partizione: {e}")
        raise e

class RobustDataConverter:
    """Classe per gestire conversioni robuste da Spark a Pandas con gestione dimensioni"""
    
    # Limiti configurabili per evitare l'errore maxResultSize
    DEFAULT_MAX_ROWS = 10000
    SAFE_MAX_ROWS = 1000
    MAX_MEMORY_MB = 1000
    
    @staticmethod
    def _convert_with_comprehensive_fallback(df) -> pd.DataFrame:
        """Conversione con fallback comprensivo per tutti i tipi problematici"""
        
        st.info("🔧 Applicando conversione robusta...")
        
        # Prima, identifica TUTTE le colonne problematiche
        problematic_cols = RobustDataConverter._diagnose_all_columns(df, show_details=True)
        
        if problematic_cols:
            st.warning("🔧 Colonne convertite automaticamente:")
            for idx, name, dtype, error_type in problematic_cols:
                st.info(f"- #{idx}: '{name}' ({dtype}) → {error_type}")
        
        # Strategia 1: Converti tutto il possibile a tipi sicuri
        safe_cols = []
        
        for i, field in enumerate(df.schema.fields):
            col_name = field.name
            col_type = field.dataType
            
            try:
                if RobustDataConverter._is_problematic_column(i, col_name, problematic_cols):
                    # Applica conversione specifica basata sul tipo di problema
                    safe_col = RobustDataConverter._convert_problematic_column(
                        F.col(col_name), col_name, col_type, i, problematic_cols
                    )
                    safe_cols.append(safe_col)
                else:
                    # Colonna OK, mantieni com'è
                    safe_cols.append(F.col(col_name))
                    
            except Exception as e:
                logger.warning(f"Errore nel processare colonna {col_name}: {e}")
                # Fallback estremo: converti a stringa
                safe_cols.append(F.col(col_name).cast("string").alias(col_name))
        
        try:
            # Prova la conversione con le colonne "sicure"
            df_safe = df.select(*safe_cols)
            pandas_df = df_safe.toPandas()
            
            # Post-processing per gestire eventuali problemi residui
            pandas_df = RobustDataConverter._post_process_pandas_df(pandas_df)
            
            st.success("✅ Conversione robusta completata!")
            return pandas_df
            
        except Exception as e2:
            logger.error(f"Anche la conversione robusta è fallita: {e2}")
            st.error(f"❌ Conversione robusta fallita: {str(e2)[:100]}...")            
            return RobustDataConverter._force_all_to_string(df)
    
    @staticmethod
    def _diagnose_all_columns(df, sample_size: int = 50, show_details: bool = False) -> List[Tuple[int, str, str, str]]:
        """Identifica tutte le colonne problematiche"""
        problematic = []
        
        for i, field in enumerate(df.schema.fields):
            try:
                test_df = df.select(field.name).limit(sample_size)
                test_df.head()
                    
            except Exception as e:
                error_msg = str(e)
                error_type = "Conversione Generica"
                
                if "Expected bytes" in error_msg and "got a" in error_msg:
                    error_type = "Tipo Misto (bytes/object)"
                elif "struct" in field.dataType.simpleString().lower():
                    error_type = "Struttura Complessa"
                elif "array" in field.dataType.simpleString().lower():
                    error_type = "Array"
                elif "map" in field.dataType.simpleString().lower():
                    error_type = "Mappa"
                elif "binary" in field.dataType.simpleString().lower():
                    error_type = "Dati Binari"
                
                problematic.append((i, field.name, field.dataType.simpleString(), error_type))
        
        return problematic
    
    @staticmethod
    def _is_problematic_column(col_index: int, col_name: str, problematic_list: List) -> bool:
        """Controlla se una colonna è nella lista delle problematiche"""
        return any(p[0] == col_index for p in problematic_list)
    
    @staticmethod
    def _convert_problematic_column(col_expr, col_name: str, col_type, col_index: int, problematic_list: List):
        """Converte una colonna problematica usando la strategia appropriata"""
        
        error_type = "Conversione Generica"
        for p_idx, p_name, p_type, p_error in problematic_list:
            if p_idx == col_index:
                error_type = p_error
                break
        
        if error_type == "Struttura Complessa":
            return F.to_json(col_expr).alias(col_name)
        elif error_type == "Array":
            return F.to_json(col_expr).alias(col_name)
        elif error_type == "Mappa":
            return F.to_json(col_expr).alias(col_name)
        elif error_type == "Dati Binari":
            return F.base64(col_expr).alias(col_name)
        elif error_type == "Tipo Misto (bytes/object)":
            return F.col(col_name).cast("string").alias(col_name)
        else:
            return F.col(col_name).cast("string").alias(col_name)
    
    @staticmethod
    def _post_process_pandas_df(df: pd.DataFrame) -> pd.DataFrame:
        """Post-processing del DataFrame Pandas per gestire problemi residui"""
        
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = df[col].astype(str)
                except Exception as e:
                    logger.warning(f"Post-processing colonna {col}: {e}")
                    df[col] = df[col].astype(str)
        
        return df
    
    @staticmethod
    def _force_all_to_string(df) -> pd.DataFrame:
        """Ultima risorsa: forza TUTTE le colonne a stringa"""
        st.error("CONVERSIONE FORZATA: Tutte le colonne convertite in stringhe")
        
        try:
            string_cols = [F.col(field.name).cast("string").alias(field.name) for field in df.schema.fields]
            df_strings = df.select(*string_cols)
            return df_strings.toPandas()
        except Exception as e:
            logger.error(f"Anche la conversione forzata è fallita: {e}")
            st.error(f"Impossibile convertire il DataFrame: {e}")
            return pd.DataFrame({"error": ["Conversione fallita completamente"]})
    
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

        st.info("🔍 Validazione query in corso...")
        
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
            
            if limit_results:
                spark_df = spark_df.limit(limit_results)
            else:
                spark_df = spark_df.limit(RobustDataConverter.DEFAULT_MAX_ROWS)
            
            if self._is_empty_spark_df(spark_df):
                result['warning'] = "Query eseguita ma nessun risultato restituito"
                result['success'] = True
                result['data'] = pd.DataFrame()
                execution_time = time.time() - start_time
                self._add_to_history(query, 0, True, None, execution_time)
                return result
            
            try:
                total_rows_count = spark_df.count()
            except Exception as e:
                total_rows_count = "N/A"
                st.error(f"❌ Impossibile contare le righe totali: {e}")

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

def get_twitter_query_templates():
    """Carica i template delle query da un file JSON."""
    file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'custom_query.json')
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            templates = json.load(f)

        for category, queries in templates.items():
            for query_name, query_string in queries.items():
                templates[category][query_name] = query_string.format(
                    temp_view_name=DatabaseConfig.TEMP_VIEW_NAME
                )
        return templates
    
    except FileNotFoundError:
        print(f"Errore: file '{file_path}' non trovato.")
        return {}
    except json.JSONDecodeError:
        print(f"Errore: il file '{file_path}' non è un JSON valido.")
        return {}
    except Exception as e:
        print(f"Si è verificato un errore inatteso: {e}")
        return {}

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


def show_dataset_info_safe(dataset):
    """Mostra informazioni dataset con gestione errori"""
    with st.expander("Informazioni Dataset"):
        with st.spinner("Caricamento informazioni dataset..."):
            if dataset is not None:
                if 'row_count' not in st.session_state:
                    try:
                        st.session_state.row_count = dataset.count()
                    except Exception as e:
                        st.session_state.row_count = f"Errore: {e}"
                        st.error(f"❌ Errore nel conteggio: {e}")
                
                row_count = st.session_state.row_count

                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.subheader("Schema Colonne:")
                    try:
                        # Prendi il primo record per ispezionare i dati complessi
                        first_row = dataset.take(1)
                        if first_row:
                            first_row = first_row[0]
                        else:
                            first_row = None
                            
                        schema_list = []
                        for field in dataset.schema.fields:
                            col_name = field.name
                            col_type = field.dataType.simpleString()
                            is_complex = RobustDataConverter._is_complex_type(field.dataType)
                            
                            sample_value = ""
                            if first_row:
                                # Gestisce i tipi complessi per mostrare una singola occorrenza
                                if isinstance(field.dataType, StructType):
                                    sample_value = str(first_row.asDict()[col_name])
                                elif isinstance(field.dataType, ArrayType):
                                    array_values = first_row.asDict()[col_name]
                                    if array_values and len(array_values) > 0:
                                        sample_value = str(array_values[0])
                                else:
                                    sample_value = "" # Per tipi semplici
                                    
                            schema_list.append({
                                "Nome Colonna": col_name,
                                "Tipo di Dato": col_type
                            })
                        
                        schema_df = pd.DataFrame(schema_list)
                        st.dataframe(schema_df, width='stretch')
                        
                    except Exception as e:
                        st.error(f"Errore nella lettura schema: {e}")
                        st.write("Colonne disponibili:", ", ".join(dataset.columns[:10]))
                        
                with col2:
                    st.write("**Nome Tabella SQL:**", f"`{DatabaseConfig.TEMP_VIEW_NAME}`")
                    
                    if isinstance(row_count, int):
                        st.write("**Numero Righe:**", f"{row_count:,}")
                    else:
                        st.write("**Numero Righe:**", row_count)
                        
                    st.write("**Numero Colonne:**", len(dataset.columns))
    

def show_simplified_editor_tab(query_engine, dataset):
    """Tab editor semplificato con template integrati"""
    
    st.markdown("#### 📚 Query Predefinite")
    
    all_templates = get_twitter_query_templates()
    
    for category, queries in all_templates.items():
        with st.expander(category):
            for name, query in queries.items():
                with st.expander(name):
                    st.markdown(f"**{name}**")
                    col1, col2 = st.columns([4, 1])
                    
                    with col1:
                        st.code(query, language="sql")
                    
                    with col2:
                        if st.button("Usa", key=f"template_{category}_{name}"):
                            st.session_state.selected_template = query
                            st.success("✅ Template copiato!")
                            st.rerun()
    
    st.markdown("### ✏️ Editor SQL")
    
    default_query = f"SELECT * FROM {DatabaseConfig.TEMP_VIEW_NAME}"
    
    if 'selected_template' in st.session_state:
        default_query = st.session_state.selected_template
        del st.session_state.selected_template
        st.info("✅ Template caricato nell'editor!")
    
    query_text = st.text_area(
        "Query SQL:",
        value=default_query,
        height=150,
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
            show_save_template_popup(query_text)
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

    # Esecuzione query
    if execute_button:
        if not query_text.strip():
            st.error("⚠️ Inserisci una query SQL valida")
            return
        
        with st.spinner("⚡ Esecuzione query in corso..."):
            result = query_engine.execute_custom_query_safe(query_text, result_limit)
            
            if result['success']:
                data = result['data']
                stats = result['stats']
                st.session_state.last_query_result = data
                st.session_state.last_query_text = query_text
                st.session_state.last_query_stats = stats
                
                if result['warning']:
                    st.warning(f"⚠️ {result['warning']}")
    
    if 'last_query_result' in st.session_state:
        st.markdown("---")
        st.markdown("### 📊 Risultati")
        result_data = st.session_state.last_query_result
        stats = st.session_state.last_query_stats
        query_text = st.session_state.last_query_text

        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Righe Totali", f"{len(result_data):,}")
        with col2:
            st.metric("Colonne", len(result_data.columns))
        with col3:
            memory_mb = result_data.memory_usage(deep=True).sum() / 1024**2
            st.metric("Memoria Utilizzata", f"{memory_mb:.1f} MB")
        with col4:
            null_count = result_data.isnull().sum().sum()
            st.metric("Valori Null", null_count)
        
        # Controlli di visualizzazione
        column1, column2, column3 = st.columns(3)
        
        with column1:
            show_all = st.checkbox("Mostra tutte le righe", key="show_all_results", value=False)
        
        with column2:
            if not show_all:
                max_rows = min(10000, len(result_data))
                display_limit = st.slider("Righe da mostrare:", 10, max_rows, 100, key="results_slider")
            else:
                display_limit = len(result_data)
        with column3:
            search_term = st.text_input("🔍 Cerca nei risultati:", placeholder="Termine di ricerca...")
        
        display_data = result_data.copy()

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


def show_save_template_popup(query_text: str):
    """Mostra popup per salvare template personalizzato"""

    @st.dialog("💾 Salva Query")
    def save_template_dialog():
        current_query = query_text

        if not current_query.strip():
            st.error("❌ Nessuna query presente nell'editor!")
            if st.button("Chiudi"):
                st.rerun()
            return
        
        col1_out, col2_out = st.columns([1, 2])
        
        with col1_out:
            category_type = st.radio(
                "Tipo categoria:",
                ["Esistente", "Nuova"],
                help="Scegli se usare una categoria esistente o crearne una nuova",
                key="category_radio_choice"
            )

        category = ""
        with col2_out:
            all_templates = get_twitter_query_templates()
            existing_categories = list(all_templates.keys())

            if category_type == "Esistente":
                if existing_categories:
                    category = st.selectbox("Categoria:", existing_categories, key="existing_category_select")
                else:
                    st.info("Nessuna categoria esistente trovata. Sarà creata una nuova categoria.")
                    category = st.text_input("Nome della nuova categoria:", value="Custom", key="new_category_text_1")
            else:
                category = st.text_input("Nome della nuova categoria:", placeholder="es. Analisi Custom", key="new_category_text_2")

        with st.form("save_template_form"):
            template_name = st.text_input(
                "Nome query:", 
                placeholder="es. example query"
            )
            
            st.write("**Query da salvare:**")
            st.code(current_query, language="sql")

            col_save, col_cancel = st.columns(2)

            with col_save:
                save_submitted = st.form_submit_button("💾 Salva Query", type="primary")

            with col_cancel:
                cancel_submitted = st.form_submit_button("❌ Annulla")

            if save_submitted:
                if not category.strip() or not template_name.strip():
                    st.error("❌ Categoria e nome template sono obbligatori!")
                    return
                
                if 'custom_templates' not in st.session_state:
                    st.session_state.custom_templates = {}
                
                if category not in st.session_state.custom_templates:
                    st.session_state.custom_templates[category] = {}
                
                st.session_state.custom_templates[category][template_name] = {
                    'query': current_query,
                    'created_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                }

                file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'custom_query.json')
                templates_to_save = get_twitter_query_templates()
                if category not in templates_to_save:
                    templates_to_save[category] = {}
                templates_to_save[category][template_name] = format_query_for_json(current_query)
                try:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(templates_to_save, f, indent=4, ensure_ascii=False)
                    st.success(f"✅ Template '{template_name}' salvato nella categoria '{category}' nel file JSON!")
                except Exception as e:
                    st.error(f"❌ Errore durante il salvataggio del file: {e}")
                
                import time
                time.sleep(1)
                st.rerun()
            if cancel_submitted:
                st.rerun()

    save_template_dialog()

def format_query_for_json(query_string: str) -> str:
    formatted_query = query_string.replace('\t', '\\t').replace('\n', '\\n')
    return formatted_query

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