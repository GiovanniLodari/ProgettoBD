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
from typing import Optional, Dict, Any, List, Tuple
from streamlit_tree_select import tree_select


logger = logging.getLogger(__name__)

class RobustDataConverter:
    """Classe per gestire conversioni robuste da Spark a Pandas"""
    
    @staticmethod
    def safe_spark_to_pandas(df, max_rows: Optional[int] = None, force_string_complex: bool = True) -> pd.DataFrame:
        """
        Converte un DataFrame Spark in Pandas con gestione robusta degli errori
        """
        try:
            if max_rows:
                df = df.limit(max_rows)
            
            return df.toPandas()
            
        except Exception as e:
            logger.warning(f"Conversione diretta fallita: {e}")
            
            if force_string_complex:
                return RobustDataConverter._convert_with_comprehensive_fallback(df)
            else:
                raise e
    
    @staticmethod
    def _convert_with_comprehensive_fallback(df) -> pd.DataFrame:
        """Conversione con fallback comprensivo per tutti i tipi problematici"""
        
        problematic_cols = RobustDataConverter._diagnose_all_columns(df, show_details=False)
        
        safe_cols = []
        
        for i, field in enumerate(df.schema.fields):
            col_name = field.name
            col_type = field.dataType
            
            try:
                if RobustDataConverter._is_problematic_column(i, col_name, problematic_cols):
                    safe_col = RobustDataConverter._convert_problematic_column(
                        F.col(col_name), col_name, col_type, i, problematic_cols
                    )
                    safe_cols.append(safe_col)
                else:
                    safe_cols.append(F.col(col_name))
                    
            except Exception as e:
                logger.warning(f"Errore nel processare colonna {col_name}: {e}")
                safe_cols.append(F.col(col_name).cast("string").alias(col_name))
        
        try:
            df_safe = df.select(*safe_cols)
            pandas_df = df_safe.toPandas()
            pandas_df = RobustDataConverter._post_process_pandas_df(pandas_df)
            return pandas_df
            
        except Exception as e2:
            logger.error(f"Anche la conversione robusta è fallita: {e2}")
            return RobustDataConverter._force_all_to_string(df)
    
    @staticmethod
    def _diagnose_all_columns(df, sample_size: int = 50, show_details: bool = False) -> List[Tuple[int, str, str, str]]:
        """Identifica tutte le colonne problematiche"""
        problematic = []
        
        for i, field in enumerate(df.schema.fields):
            try:
                test_df = df.select(field.name).limit(sample_size)
                test_df.toPandas()
                    
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
        
        try:
            string_cols = [F.col(field.name).cast("string").alias(field.name) for field in df.schema.fields]
            df_strings = df.select(*string_cols)
            return df_strings.toPandas()
        except Exception as e:
            logger.error(f"Anche la conversione forzata è fallita: {e}")
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
            
            if limit_results:
                spark_df = spark_df.limit(limit_results)
            
            if self._is_empty_spark_df(spark_df):
                result['warning'] = "Query eseguita ma nessun risultato restituito"
                result['success'] = True
                result['data'] = pd.DataFrame()
                execution_time = time.time() - start_time
                self._add_to_history(query, 0, True, None, execution_time)
                return result
            
            pandas_df = RobustDataConverter.safe_spark_to_pandas(
                spark_df, 
                max_rows=limit_results, 
                force_string_complex=True
            )
            
            result['stats'] = self._calculate_stats(pandas_df, spark_df)
            result['success'] = True
            result['data'] = pandas_df
            
            execution_time = time.time() - start_time
            self._add_to_history(query, len(pandas_df), True, None, execution_time)
            
            return result
            
        except Exception as e:
            logger.error(f"Errore nell'esecuzione query: {str(e)}")
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
        validation = {'valid': True}
        
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

        # Sostituisci il segnaposto con il nome della vista temporanea
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
                row_count = dataset.count()
                col1, col2 = st.columns([2, 1])
                with st.spinner("Caricamento informazioni dataset..."):
                    with col1:
                        st.subheader("Schema Colonne:")
                        try:
                            # Mostra schema in modo sicuro
                            for i, field in enumerate(dataset.schema.fields):
                                col_type = field.dataType.simpleString()
                                is_complex = RobustDataConverter._is_complex_type(field.dataType)
                                
                                if is_complex:
                                    st.write(f"`{field.name}` ({col_type}) ⚠️")
                                else:
                                    st.write(f"`{field.name}` ({col_type})")
                                
                                if i > 15:  # Limita visualizzazione
                                    remaining = len(dataset.schema.fields) - i - 1
                                    if remaining > 0:
                                        st.write(f"... e altre {remaining} colonne")
                                    break
                        except Exception as e:
                            st.write(f"Errore nella lettura schema: {e}")
                            st.write("Colonne disponibili:", ", ".join(dataset.columns[:10]))
                    
                    with col2:
                        st.write("**Nome Tabella SQL:**", f"`{DatabaseConfig.TEMP_VIEW_NAME}`")
                        try:
                            st.write("**Numero Righe:**", f"{row_count:,}")
                        except Exception as e:
                            st.write("**Numero Righe:**", f"Errore nel conteggio: {e}")
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

    # 2. Template personalizzati
    st.markdown("#### 💾 I Tuoi Template")
    
    if 'custom_templates' not in st.session_state:
        st.session_state.custom_templates = {}
    
    # Form per aggiungere template
    with st.form("add_custom_template"):
        st.write("**Salva Query come Template:**")
        
        col1, col2 = st.columns([2, 3])
        with col1:
            template_name = st.text_input("Nome template:")
        with col2:
            template_description = st.text_input("Descrizione (opzionale):")
        
        template_query = st.text_area("Query SQL:", height=100)
        
        submitted = st.form_submit_button("💾 Salva Template")
        
        if submitted:
            if template_name and template_query:
                st.session_state.custom_templates[template_name] = {
                    'query': template_query,
                    'description': template_description,
                    'created_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                st.success(f"✅ Template '{template_name}' salvato!")
                st.rerun()
            else:
                st.error("❌ Inserisci almeno nome e query")
    
    # Mostra template salvati
    if st.session_state.custom_templates:
        st.write("**Template Salvati:**")
        
        for name, template_info in st.session_state.custom_templates.items():
            with st.expander(f"🔖 {name}"):
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    if template_info.get('description'):
                        st.write(f"*{template_info['description']}*")
                    st.code(template_info['query'], language="sql")
                    st.caption(f"Creato: {template_info.get('created_at', 'N/A')}")
                
                with col2:
                    if st.button("Usa", key=f"custom_use_{name}"):
                        st.session_state.selected_template = template_info['query']
                        st.success("✅ Template caricato!")
                        st.rerun()
                
                with col3:
                    if st.button("🗑️", key=f"custom_delete_{name}", help="Elimina template"):
                        del st.session_state.custom_templates[name]
                        st.success("🗑️ Template eliminato!")
                        st.rerun()

    st.divider()
    
    # 3. DOPO: Editor per scrivere query
    st.markdown("### ✏️ Editor SQL")
    
    # Query di default
    default_query = f"SELECT * FROM {DatabaseConfig.TEMP_VIEW_NAME} LIMIT 10"
    
    # Recupera template selezionato se presente
    if 'selected_template' in st.session_state:
        default_query = st.session_state.selected_template
        del st.session_state.selected_template
        st.info("✅ Template caricato nell'editor!")
    
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
            result_limit = st.number_input("Limite:", min_value=10, max_value=10000, value=1000)
        else:
            result_limit = None
    with col4:
        validate_button = st.button("✅ Valida Query")
    
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
                
                if result['warning']:
                    st.warning(f"⚠️ {result['warning']}")
                
                if data is not None and len(data) > 0:
                    st.success(f"✅ Query eseguita con successo! {stats['rows']} righe restituite.")
                    
                    # MOSTRA TABELLA COMPLETA (non anteprima)
                    st.markdown("### 📊 Risultati Completi")
                    
                    # Metriche
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("📊 Righe", f"{stats['rows']:,}")
                    
                    with col2:
                        st.metric("🗂️ Colonne", stats['columns'])
                    
                    with col3:
                        st.metric("💾 Memoria", f"{stats['memory_mb']:.1f} MB")
                    
                    with col4:
                        st.metric("❓ Valori Null", stats['null_values'])
                    
                    # Controlli visualizzazione
                    show_complete_results(data, query_text)
                    
                else:
                    st.info("ℹ️ Query eseguita ma nessun dato restituito")
            else:
                st.error(f"❌ {result['error']}")
                show_error_suggestions(result['error'], query_text)

def show_complete_results(result: pd.DataFrame, query_text: str):
    """Mostra i risultati completi con controlli"""
    
    # Controlli per visualizzazione
    col1, col2 = st.columns(2)
    
    with col1:
        search_term = st.text_input("🔍 Cerca nei risultati:", placeholder="Termine di ricerca...")
    
    with col2:
        show_all = st.checkbox("Mostra tutte le righe", value=True)
    
    # Applica filtri
    display_data = result.copy()
    
    if search_term:
        text_cols = display_data.select_dtypes(include=['object', 'string']).columns
        if len(text_cols) > 0:
            mask = display_data[text_cols].astype(str).apply(
                lambda x: x.str.contains(search_term, case=False, na=False)
            ).any(axis=1)
            display_data = display_data[mask]
            st.info(f"🔍 Trovate {len(display_data)} righe contenenti '{search_term}'")
    
    # Mostra tabella completa
    if show_all:
        st.dataframe(display_data, use_container_width=True, height=600)
    else:
        display_limit = st.slider("Righe da mostrare:", 10, min(1000, len(display_data)), 100)
        st.dataframe(display_data.head(display_limit), use_container_width=True, height=600)
        
        if len(display_data) > display_limit:
            st.info(f"Mostrando {display_limit} di {len(display_data)} righe.")
    
    # Opzioni di export
    show_export_options(display_data, query_text)

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