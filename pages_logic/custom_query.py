"""
Pagina per query SQL personalizzate - Versione robusta con gestione errori avanzata
"""

import streamlit as st
import pandas as pd
from src.analytics import QueryEngine
from src.visualizations import GeneralVisualizations
from src.config import Config, DatabaseConfig
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging
import traceback
from typing import Optional, Dict, Any, List, Tuple

logger = logging.getLogger(__name__)

class RobustDataConverter:
    """Classe per gestire conversioni robuste da Spark a Pandas"""
    
    @staticmethod
    def safe_spark_to_pandas(df, max_rows: Optional[int] = None, force_string_complex: bool = True) -> pd.DataFrame:
        """
        Converte un DataFrame Spark in Pandas con gestione robusta degli errori
        
        Args:
            df: DataFrame Spark
            max_rows: Numero massimo di righe da convertire (None = tutte)
            force_string_complex: Se True, converte strutture complesse in stringhe
        
        Returns:
            DataFrame Pandas
        """
        try:
            # Limita righe se specificato
            if max_rows:
                df = df.limit(max_rows)
            
            # DEBUG: Mostra info sul DataFrame
            logger.info(f"Tentativo conversione DataFrame con {len(df.schema.fields)} colonne")
            
            # Tentativo di conversione diretta
            return df.toPandas()
            
        except Exception as e:
            logger.warning(f"Conversione diretta fallita: {e}")
            st.warning(f"⚙️ Conversione diretta fallita: {str(e)[:100]}...")
            
            if force_string_complex:
                return RobustDataConverter._convert_with_comprehensive_fallback(df)
            else:
                raise e
    
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
            
            # Ultima risorsa: forza TUTTO a stringa
            return RobustDataConverter._force_all_to_string(df)
    
    @staticmethod
    def _diagnose_all_columns(df, sample_size: int = 50, show_details: bool = False) -> List[Tuple[int, str, str, str]]:
        """Identifica tutte le colonne problematiche con dettagli sul tipo di errore"""
        problematic = []
        
        if show_details:
            st.info("🔍 Diagnosi colonne in corso...")
        
        for i, field in enumerate(df.schema.fields):
            try:
                # Test di conversione su campione piccolo
                test_df = df.select(field.name).limit(sample_size)
                test_df.toPandas()
                
                if show_details:
                    st.success(f"✅ #{i}: {field.name} ({field.dataType.simpleString()}) - OK")
                    
            except Exception as e:
                error_msg = str(e)
                error_type = "Conversione Generica"
                
                # Classifica il tipo di errore
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
                
                if show_details:
                    st.error(f"❌ #{i}: {field.name} ({field.dataType.simpleString()}) - {error_type}")
        
        return problematic
    
    @staticmethod
    def _is_problematic_column(col_index: int, col_name: str, problematic_list: List) -> bool:
        """Controlla se una colonna è nella lista delle problematiche"""
        return any(p[0] == col_index for p in problematic_list)
    
    @staticmethod
    def _convert_problematic_column(col_expr, col_name: str, col_type, col_index: int, problematic_list: List):
        """Converte una colonna problematica usando la strategia appropriata"""
        
        # Trova il tipo di errore per questa colonna
        error_type = "Conversione Generica"
        for p_idx, p_name, p_type, p_error in problematic_list:
            if p_idx == col_index:
                error_type = p_error
                break
        
        # Applica conversione basata sul tipo di errore
        if error_type == "Struttura Complessa":
            # Converti struct in JSON
            return F.to_json(col_expr).alias(col_name)
        
        elif error_type == "Array":
            # Converti array in JSON
            return F.to_json(col_expr).alias(col_name)
        
        elif error_type == "Mappa":
            # Converti map in JSON
            return F.to_json(col_expr).alias(col_name)
        
        elif error_type == "Dati Binari":
            # Converti binary in base64
            return F.base64(col_expr).alias(col_name)
        
        elif error_type == "Tipo Misto (bytes/object)":
            # Questo è il problema specifico che hai - forza a stringa
            return F.col(col_name).cast("string").alias(col_name)
        
        else:
            # Fallback generico: cast a stringa
            return F.col(col_name).cast("string").alias(col_name)
    
    @staticmethod
    def _post_process_pandas_df(df: pd.DataFrame) -> pd.DataFrame:
        """Post-processing del DataFrame Pandas per gestire problemi residui"""
        
        # Controlla e corregge colonne 'object' problematiche
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    # Prova a inferire il tipo corretto
                    # Ma se fallisce, forza a stringa
                    df[col] = df[col].astype(str)
                except Exception as e:
                    logger.warning(f"Post-processing colonna {col}: {e}")
                    df[col] = df[col].astype(str)
        
        return df
    
    @staticmethod
    def _force_all_to_string(df) -> pd.DataFrame:
        """Ultima risorsa: forza TUTTE le colonne a stringa"""
        st.error("⚠️ CONVERSIONE FORZATA: Tutte le colonne convertite in stringhe")
        
        try:
            string_cols = [F.col(field.name).cast("string").alias(field.name) for field in df.schema.fields]
            df_strings = df.select(*string_cols)
            return df_strings.toPandas()
        except Exception as e:
            logger.error(f"Anche la conversione forzata è fallita: {e}")
            # Crea DataFrame vuoto come ultima risorsa
            st.error(f"❌ Impossibile convertire il DataFrame: {e}")
            return pd.DataFrame({"error": ["Conversione fallita completamente"]})
    
    @staticmethod
    def _is_complex_type(data_type) -> bool:
        """Controlla se un tipo di dato è complesso (struct, array, map)"""
        return isinstance(data_type, (StructType, ArrayType, MapType))

class EnhancedQueryEngine(QueryEngine):
    """QueryEngine migliorato con gestione errori robusta"""
    
    def __init__(self):
        super().__init__()
        # Inizializza cronologia se non esiste
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
            # Validazione query
            validation = self.validate_query(query)
            if not validation['valid']:
                result['error'] = f"Query non valida: {validation['error']}"
                self._add_to_history(query, 0, False, result['error'])
                return result
            
            # Esecuzione query
            spark_session = st.session_state.spark_manager.get_spark_session()
            spark_df = spark_session.sql(query)
            
            # Applica limite se specificato
            if limit_results:
                spark_df = spark_df.limit(limit_results)
            
            # Controllo se risultato vuoto
            if self._is_empty_spark_df(spark_df):
                result['warning'] = "Query eseguita ma nessun risultato restituito"
                result['success'] = True
                result['data'] = pd.DataFrame()
                execution_time = time.time() - start_time
                self._add_to_history(query, 0, True, None, execution_time)
                return result
            
            # Conversione a Pandas con gestione robusta SEMPRE attiva
            st.info("🔄 Conversione risultati in corso...")
            pandas_df = RobustDataConverter.safe_spark_to_pandas(
                spark_df, 
                max_rows=limit_results, 
                force_string_complex=True  # SEMPRE attivo
            )
            
            # Statistiche
            result['stats'] = self._calculate_stats(pandas_df, spark_df)
            
            result['success'] = True
            result['data'] = pandas_df
            
            # Salva nella cronologia
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
        
        # Mantieni solo le ultime 50 query per evitare accumulo eccessivo
        if len(self.query_history) > 50:
            self.query_history = self.query_history[-50:]
    
    def get_query_history(self) -> List[Dict[str, Any]]:
        """Restituisce la cronologia delle query"""
        return getattr(self, 'query_history', [])
    
    def clear_history(self):
        """Pulisce la cronologia delle query"""
        self.query_history = []
    
    def get_query_suggestions(self, columns: List[str]) -> List[str]:
        """Genera suggerimenti di query basati sulle colonne del dataset"""
        suggestions = []
        
        # Suggerimenti base sempre validi
        suggestions.append(f"SELECT COUNT(*) FROM {DatabaseConfig.TEMP_VIEW_NAME}")
        suggestions.append(f"SELECT * FROM {DatabaseConfig.TEMP_VIEW_NAME} LIMIT 10")
        
        # Suggerimenti basati sulle colonne
        if columns:
            # Prima colonna per raggruppamenti
            first_col = columns[0]
            suggestions.append(f"SELECT {first_col}, COUNT(*) FROM {DatabaseConfig.TEMP_VIEW_NAME} GROUP BY {first_col} ORDER BY COUNT(*) DESC LIMIT 10")
            
            # Se ci sono colonne numeriche (basato sui nomi comuni)
            numeric_candidates = [col for col in columns if any(word in col.lower() 
                                for word in ['count', 'number', 'amount', 'size', 'length', 'favorite', 'retweet'])]
            
            if numeric_candidates:
                num_col = numeric_candidates[0]
                suggestions.append(f"SELECT AVG({num_col}), MIN({num_col}), MAX({num_col}) FROM {DatabaseConfig.TEMP_VIEW_NAME}")
        
        return suggestions[:5]  # Limita a 5 suggerimenti
    
    def validate_query(self, query: str) -> Dict[str, Any]:
        """Valida una query SQL - implementazione base"""
        validation = {'valid': True}
        
        if not query.strip():
            return {'valid': False, 'error': 'Query vuota'}
        
        # Controlli di base
        query_upper = query.upper().strip()
        
        if not any(query_upper.startswith(cmd) for cmd in ['SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'EXPLAIN']):
            return {'valid': False, 'error': 'Query deve iniziare con SELECT, WITH, SHOW, DESCRIBE o EXPLAIN'}
        
        # Controlla che non ci siano comandi pericolosi
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(keyword in query_upper for keyword in dangerous_keywords):
            return {'valid': False, 'error': 'Comandi di modifica non sono permessi'}
        
        return validation
    
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
        
        # Aggiungi info sui tipi di dato
        numeric_cols = pandas_df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = pandas_df.select_dtypes(include=['object', 'string']).columns.tolist()
        
        stats.update({
            'numeric_columns': len(numeric_cols),
            'categorical_columns': len(categorical_cols),
            'complex_columns_converted': any('json' in str(col).lower() for col in pandas_df.columns)
        })
        
        return stats

def show_custom_query_page():
    """Mostra la pagina delle query personalizzate con gestione errori migliorata"""
    
    st.title("🔍 Query SQL Personalizzate")
    st.markdown("Esegui query SQL personalizzate sui tuoi dati usando Apache Spark SQL")
    
    # Controlla se il dataset è caricato
    if not hasattr(st.session_state, 'datasets_loaded') or not st.session_state.datasets_loaded:
        st.warning("⚠️ Nessun dataset caricato. Vai alla sezione 'Esplora Dataset' per caricare i dati.")
        
        if st.button("📊 Vai al Caricamento Dataset"):
            st.switch_page("pages_logic/data_explorer.py")
        return
    
    dataset = st.session_state.data
    
    # Inizializza query engine migliorato
    if 'enhanced_query_engine' not in st.session_state:
        st.session_state.enhanced_query_engine = EnhancedQueryEngine()
    query_engine = st.session_state.enhanced_query_engine

    # Setup vista temporanea con gestione errori
    try:
        view_name = DatabaseConfig.TEMP_VIEW_NAME
        dataset.createOrReplaceTempView(view_name)
        st.success(f"✅ Vista temporanea '{view_name}' registrata con successo!")
        
        # Mostra info dataset con gestione errori
        with st.expander("📊 Informazioni Dataset"):
            show_dataset_info_safe(dataset)
        
    except Exception as e:
        st.error(f"❌ Errore critico: impossibile registrare la vista temporanea: {e}")
        st.code(f"Dettagli: DataFrame tipo {type(dataset)}")
        return
    
    general_viz = GeneralVisualizations()
    
    # Layout principale con gestione errori
    show_enhanced_query_interface(query_engine, general_viz, dataset)

def show_dataset_info_safe(dataset):
    """Mostra informazioni dataset con gestione errori"""
    try:
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Nome Tabella SQL:**", f"`{DatabaseConfig.TEMP_VIEW_NAME}`")
            try:
                row_count = dataset.count()
                st.write("**Numero Righe:**", f"{row_count:,}")
            except Exception as e:
                st.write("**Numero Righe:**", f"Errore nel conteggio: {e}")
            
            st.write("**Numero Colonne:**", len(dataset.columns))
        
        with col2:
            st.write("**Schema Colonne:**")
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
    
    except Exception as e:
        st.error(f"Errore nel mostrare informazioni dataset: {e}")

def show_enhanced_query_interface(query_engine, general_viz, dataset):
    """Interfaccia query migliorata"""
    
    # Tabs per diverse sezioni
    tab1, tab2, tab3, tab4 = st.tabs([
        "✍️ Editor Query", "📚 Query Template", "📊 Risultati", "📜 Cronologia"
    ])
    
    with tab1:
        show_enhanced_query_editor_tab(query_engine, general_viz, dataset)
    
    with tab2:
        show_query_templates_tab(query_engine, dataset)
    
    with tab3:
        show_enhanced_results_tab()
    
    with tab4:
        show_history_tab(query_engine)

def show_enhanced_query_editor_tab(query_engine, general_viz, dataset):
    """Tab editor migliorato con gestione errori robusta"""
    
    st.markdown("### ✍️ Editor SQL Avanzato")
    
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
    
    # Controlli esecuzione migliorati
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
    
    # Opzioni avanzate
    with st.expander("⚙️ Opzioni Avanzate"):
        col1, col2 = st.columns(2)
        
        # with col1:
        #     force_string_conversion = st.checkbox(
        #         "Converti strutture complesse in stringhe", 
        #         value=True,
        #         help="Converte automaticamente colonne complesse (struct, array) in stringhe JSON per evitare errori"
        #     )
        
        with col2:
            show_execution_details = st.checkbox(
                "Mostra dettagli esecuzione", 
                value=False,
                help="Mostra informazioni dettagliate sull'esecuzione della query"
            )
    
    # Validazione query
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
    
    # Esecuzione query migliorata
    if execute_button:
        if not query_text.strip():
            st.error("⚠️ Inserisci una query SQL valida")
            return
        
        # Mostra info su conversione forzata se abilitata
        #if force_string_conversion:
        #    st.info("🔧 Conversione robusta attiva - strutture complesse saranno convertite automaticamente")
        
        with st.spinner("⚡ Esecuzione query in corso..."):
            # Debug: mostra info dataset prima dell'esecuzione
            with st.expander("🔍 Debug Info (clicca per vedere)", expanded=False):
                st.write("**Colonne dataset:**", len(dataset.columns))
                st.write("**Schema colonne critiche:**")
                for i, field in enumerate(dataset.schema.fields[:15]):  # Prime 15
                    is_complex = RobustDataConverter._is_complex_type(field.dataType)
                    icon = "🔴" if is_complex else "🟢"
                    st.write(f"{icon} #{i}: `{field.name}` ({field.dataType.simpleString()})")
            
            # Esegui query con gestione errori robusta
            result = query_engine.execute_custom_query_safe(query_text, result_limit)
            
            if result['success']:
                data = result['data']
                stats = result['stats']
                
                if result['warning']:
                    st.warning(f"⚠️ {result['warning']}")
                
                if data is not None and len(data) > 0:
                    st.success(f"✅ Query eseguita con successo! {stats['rows']} righe restituite.")
                    
                    # Salva risultati
                    st.session_state.last_query_result = data
                    st.session_state.last_query_text = query_text
                    st.session_state.last_query_stats = stats
                    
                    # Mostra anteprima
                    show_query_preview(data, stats, show_execution_details)
                    
                    # Suggerisci visualizzazioni
                    if len(data) > 1 and len(data.columns) >= 2:
                        if st.checkbox("📊 Crea visualizzazione automatica"):
                            show_auto_visualization(data, general_viz)
                else:
                    st.info("ℹ️ Query eseguita ma nessun dato restituito")
            else:
                st.error(f"❌ {result['error']}")
                
                # Suggerimenti per errori comuni
                show_error_suggestions(result['error'], query_text)
    
    # Suggerimenti SQL
    show_sql_help_section()

def show_query_preview(data: pd.DataFrame, stats: Dict[str, Any], show_details: bool):
    """Mostra anteprima risultati query"""
    
    st.markdown("#### 👀 Anteprima Risultati")
    
    # Mostra dati
    display_rows = min(20, len(data))
    st.dataframe(data.head(display_rows), use_container_width=True)
    
    if len(data) > display_rows:
        st.info(f"Mostrando {display_rows} di {len(data)} righe. Vai al tab 'Risultati' per vedere tutto.")
    
    # Statistiche in formato cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Righe", f"{stats['rows']:,}")
    
    with col2:
        st.metric("🗂️ Colonne", stats['columns'])
    
    with col3:
        st.metric("💾 Memoria", f"{stats['memory_mb']:.1f} MB")
    
    with col4:
        st.metric("❓ Valori Null", stats['null_values'])
    
    # Dettagli avanzati se richiesto
    if show_details:
        st.markdown("#### 🔍 Dettagli Esecuzione")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Tipi di Colonne:**")
            st.write(f"- Numeriche: {stats['numeric_columns']}")
            st.write(f"- Categoriche: {stats['categorical_columns']}")
            
        with col2:
            if stats.get('complex_columns_converted'):
                st.warning("⚙️ Alcune colonne complesse sono state convertite")
            else:
                st.success("✅ Tutte le colonne convertite senza problemi")

def show_error_suggestions(error: str, query: str):
    """Mostra suggerimenti basati sugli errori comuni"""
    
    st.markdown("#### 💡 Suggerimenti per la Risoluzione")
    
    error_lower = error.lower()
    
    if "table or view not found" in error_lower:
        st.info(f"🔍 Assicurati di usare il nome tabella corretto: `{DatabaseConfig.TEMP_VIEW_NAME}`")
    
    elif "column" in error_lower and "not found" in error_lower:
        st.info("🔍 Controlla che i nomi delle colonne siano corretti. Usa le informazioni nel dataset espanso sopra.")
    
    elif "syntax error" in error_lower:
        st.info("📝 Controlla la sintassi SQL. Assicurati che tutte le parentesi siano bilanciate e le virgole al posto giusto.")
    
    elif "type" in error_lower and "conversion" in error_lower:
        st.info("⚙️ Prova ad abilitare 'Converti strutture complesse in stringhe' nelle opzioni avanzate.")
    
    elif "memory" in error_lower or "out of" in error_lower:
        st.info("💾 Prova a limitare i risultati o usare filtri WHERE più restrittivi.")
    
    else:
        st.info("📖 Controlla la sintassi SQL e assicurati che la query sia compatibile con Spark SQL.")
    
    # Suggerimenti generici
    with st.expander("📚 Risorse Utili"):
        st.markdown("""
        **Link Utili:**
        - [Documentazione Spark SQL](https://spark.apache.org/docs/latest/sql-ref.html)
        - [Funzioni Spark SQL](https://spark.apache.org/docs/latest/api/sql/)
        
        **Errori Comuni:**
        - Usa apici singoli per le stringhe: `'valore'`
        - I nomi con spazi vanno in backtick: `` `nome colonna` ``
        - Le date vanno nel formato: `'2023-01-01'`
        """)

def show_enhanced_results_tab():
    """Tab risultati migliorato"""
    
    st.markdown("### 📊 Risultati Dettagliati")
    
    if 'last_query_result' not in st.session_state:
        st.info("📝 Nessun risultato disponibile. Esegui una query nel tab 'Editor Query'.")
        return
    
    result = st.session_state.last_query_result
    query_text = st.session_state.get('last_query_text', 'N/A')
    stats = st.session_state.get('last_query_stats', {})
    
    # Query eseguita
    st.markdown("#### 🔍 Query Eseguita")
    st.code(query_text, language="sql")
    
    # Statistiche dettagliate
    show_detailed_stats(result, stats)
    
    # Controlli visualizzazione
    show_result_controls(result)
    
    # Export opzioni
    show_export_options(result, query_text)

def show_detailed_stats(result: pd.DataFrame, stats: Dict[str, Any]):
    """Mostra statistiche dettagliate sui risultati"""
    
    st.markdown("#### 📈 Statistiche Dettagliate")
    
    # Metriche principali
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Righe Totali", f"{len(result):,}")
    
    with col2:
        st.metric("🗂️ Colonne", len(result.columns))
    
    with col3:
        memory_mb = result.memory_usage(deep=True).sum() / 1024**2
        st.metric("💾 Memoria", f"{memory_mb:.1f} MB")
    
    with col4:
        null_count = result.isnull().sum().sum()
        st.metric("❓ Valori Null", null_count)
    
    # Analisi per tipo di colonna
    if st.checkbox("🔍 Analisi Avanzata Colonne"):
        show_column_analysis(result)

def show_column_analysis(result: pd.DataFrame):
    """Mostra analisi dettagliata delle colonne"""
    
    st.markdown("##### 📊 Analisi per Colonna")
    
    analysis_data = []
    
    for col in result.columns:
        col_data = result[col]
        
        base_info = {
            'Colonna': col,
            'Tipo': str(col_data.dtype),
            'Valori Null': col_data.isnull().sum(),
            'Valori Unici': col_data.nunique(),
            '% Completezza': f"{((len(col_data) - col_data.isnull().sum()) / len(col_data) * 100):.1f}%"
        }
        
        # Aggiungi statistiche specifiche per tipo
        if col_data.dtype in ['int64', 'float64']:
            if not col_data.empty:
                base_info.update({
                    'Media': f"{col_data.mean():.2f}" if pd.notnull(col_data.mean()) else "N/A",
                    'Min': col_data.min(),
                    'Max': col_data.max(),
                    'Std Dev': f"{col_data.std():.2f}" if pd.notnull(col_data.std()) else "N/A"
                })
        elif col_data.dtype == 'object':
            base_info.update({
                'Lunghezza Media': f"{col_data.astype(str).str.len().mean():.1f}" if not col_data.empty else "N/A",
                'Valore Più Comune': col_data.mode().iloc[0] if not col_data.mode().empty else "N/A"
            })
        
        analysis_data.append(base_info)
    
    analysis_df = pd.DataFrame(analysis_data)
    st.dataframe(analysis_df, use_container_width=True)

def show_result_controls(result: pd.DataFrame):
    """Controlli per visualizzazione risultati"""
    
    st.markdown("#### 📋 Visualizza Risultati")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        show_all = st.checkbox("Mostra tutte le righe", value=False)
    
    with col2:
        if not show_all:
            display_limit = st.slider("Righe da mostrare:", 10, min(1000, len(result)), 100)
        else:
            display_limit = len(result)
    
    with col3:
        search_term = st.text_input("🔍 Cerca nei risultati:", placeholder="Termine di ricerca...")
    
    # Applica filtri
    display_data = result.copy()
    
    if search_term:
        # Ricerca in tutte le colonne di testo
        text_cols = display_data.select_dtypes(include=['object', 'string']).columns
        if len(text_cols) > 0:
            mask = display_data[text_cols].astype(str).apply(
                lambda x: x.str.contains(search_term, case=False, na=False)
            ).any(axis=1)
            display_data = display_data[mask]
            st.info(f"🔍 Trovate {len(display_data)} righe contenenti '{search_term}'")
    
    # Mostra risultati
    if show_all:
        st.dataframe(display_data, use_container_width=True)
    else:
        st.dataframe(display_data.head(display_limit), use_container_width=True)
        
        if len(display_data) > display_limit:
            st.info(f"Mostrando {display_limit} di {len(display_data)} righe.")

def show_export_options(result: pd.DataFrame, query_text: str):
    """Opzioni di export dei risultati"""
    
    st.markdown("#### 💾 Export Risultati")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # CSV Export
        csv_data = result.to_csv(index=False)
        st.download_button(
            "📥 CSV",
            csv_data,
            file_name=f"query_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            help="Scarica i risultati in formato CSV"
        )
    
    with col2:
        # JSON Export
        json_data = result.to_json(orient='records', indent=2)
        st.download_button(
            "📥 JSON",
            json_data,
            file_name=f"query_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            help="Scarica i risultati in formato JSON"
        )
    
    with col3:
        # SQL Export
        sql_export = f"-- Query eseguita il {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n-- Righe restituite: {len(result)}\n\n{query_text};"
        st.download_button(
            "📥 SQL",
            sql_export,
            file_name=f"query_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain",
            help="Scarica la query SQL"
        )
    
    with col4:
        # Excel Export (se disponibile)
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

def show_sql_help_section():
    """Sezione di aiuto SQL"""
    
    with st.expander("💡 Guida SQL per Spark"):
        st.markdown(f"""
        **Esempi Query Comuni:**
        
        ```sql
        -- Selezione base
        SELECT * FROM {DatabaseConfig.TEMP_VIEW_NAME} LIMIT 10;
        
        -- Conteggio e raggruppamento
        SELECT colonna, COUNT(*) as count 
        FROM {DatabaseConfig.TEMP_VIEW_NAME} 
        GROUP BY colonna 
        ORDER BY count DESC;
        
        -- Filtri e condizioni
        SELECT * FROM {DatabaseConfig.TEMP_VIEW_NAME} 
        WHERE colonna_numerica > 100 
        AND colonna_testo LIKE '%parola%';
        
        -- Statistiche aggregate
        SELECT 
            AVG(colonna_numerica) as media,
            MIN(colonna_numerica) as minimo,
            MAX(colonna_numerica) as massimo,
            COUNT(*) as totale
        FROM {DatabaseConfig.TEMP_VIEW_NAME};
        
        -- Gestione strutture complesse (per dati Twitter)
        SELECT 
            user.screen_name,
            user.followers_count,
            text,
            favorite_count
        FROM {DatabaseConfig.TEMP_VIEW_NAME}
        WHERE user.verified = true;
        
        -- Estrazione da JSON (se colonne convertite)
        SELECT 
            get_json_object(user, '$.screen_name') as username,
            get_json_object(user, '$.followers_count') as followers
        FROM {DatabaseConfig.TEMP_VIEW_NAME};
        ```
        
        **Funzioni Spark SQL Utili:**
        - `COALESCE(col1, col2)` - Gestisce valori null
        - `CAST(colonna AS INT)` - Conversione tipi
        - `SUBSTRING(testo, 1, 10)` - Sottostringhe
        - `UPPER(testo)`, `LOWER(testo)` - Maiuscole/minuscole
        - `DATE_FORMAT(data, 'yyyy-MM-dd')` - Formattazione date
        - `YEAR(data)`, `MONTH(data)` - Estrazione parti data
        
        **Per Strutture Complesse:**
        - `user.screen_name` - Accesso a campi struct
        - `get_json_object(col, '$.field')` - Estrazione da JSON
        - `explode(array_col)` - Espande array in righe
        """)

def show_auto_visualization(result: pd.DataFrame, general_viz):
    """Crea visualizzazioni automatiche migliorata"""
    
    st.markdown("#### 📊 Visualizzazione Automatica")
    
    # Analizza tipi di colonne
    numeric_cols = result.select_dtypes(include=['int64', 'float64']).columns.tolist()
    categorical_cols = result.select_dtypes(include=['object', 'string']).columns.tolist()
    datetime_cols = result.select_dtypes(include=['datetime64']).columns.tolist()
    
    if len(result) <= 1:
        st.info("📊 Servono almeno 2 righe per creare visualizzazioni")
        return
    
    # Opzioni di visualizzazione
    viz_options = []
    
    if len(categorical_cols) >= 1 and len(numeric_cols) >= 1:
        viz_options.extend(["Grafico a Barre", "Grafico a Torta", "Box Plot"])
    
    if len(numeric_cols) >= 2:
        viz_options.extend(["Scatter Plot", "Correlazione"])
    
    if len(numeric_cols) >= 1:
        viz_options.append("Istogramma")
    
    if len(datetime_cols) >= 1 and len(numeric_cols) >= 1:
        viz_options.append("Serie Temporale")
    
    if not viz_options:
        st.info("📊 Tipi di dati non ottimali per visualizzazioni automatiche")
        return
    
    # Selezione tipo di grafico
    chart_type = st.selectbox("Seleziona tipo di visualizzazione:", viz_options)
    
    try:
        if chart_type == "Grafico a Barre":
            cat_col = st.selectbox("Colonna categorica:", categorical_cols)
            num_col = st.selectbox("Colonna numerica:", numeric_cols)
            
            # Limita a top 20 per leggibilità
            plot_data = result.nlargest(20, num_col) if len(result) > 20 else result
            fig = general_viz.create_horizontal_bar_chart(
                plot_data, cat_col, num_col, f"{num_col} per {cat_col}"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Grafico a Torta":
            cat_col = st.selectbox("Colonna categorica:", categorical_cols)
            num_col = st.selectbox("Colonna numerica:", numeric_cols)
            
            # Aggrega e limita a top 10
            pie_data = result.groupby(cat_col)[num_col].sum().nlargest(10).reset_index()
            fig = general_viz.create_pie_chart(
                pie_data, cat_col, num_col, f"Distribuzione {cat_col}"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Scatter Plot":
            col1, col2 = st.columns(2)
            with col1:
                x_col = st.selectbox("Asse X:", numeric_cols)
            with col2:
                y_col = st.selectbox("Asse Y:", [c for c in numeric_cols if c != x_col])
            
            color_col = st.selectbox("Colore per:", ["Nessuno"] + categorical_cols)
            color_col = color_col if color_col != "Nessuno" else None
            
            # Campiona se troppi dati
            plot_data = result.sample(n=min(1000, len(result)))
            fig = general_viz.create_scatter_plot(
                plot_data, x_col, y_col, f"Relazione {x_col} vs {y_col}", color_col
            )
            st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Istogramma":
            num_col = st.selectbox("Colonna numerica:", numeric_cols)
            bins = st.slider("Numero di bin:", 10, 100, 30)
            
            fig = general_viz.create_histogram(
                result, num_col, f"Distribuzione di {num_col}", bins
            )
            st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Serie Temporale":
            date_col = st.selectbox("Colonna data:", datetime_cols)
            num_col = st.selectbox("Colonna numerica:", numeric_cols)
            
            # Ordina per data
            time_data = result.sort_values(date_col)
            fig = general_viz.create_time_series(
                time_data, date_col, num_col, f"Trend di {num_col} nel tempo"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Box Plot":
            cat_col = st.selectbox("Colonna categorica:", categorical_cols)
            num_col = st.selectbox("Colonna numerica:", numeric_cols)
            
            fig = general_viz.create_box_plot(
                result, cat_col, num_col, f"Distribuzione {num_col} per {cat_col}"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Correlazione":
            # Matrice di correlazione
            corr_cols = st.multiselect(
                "Seleziona colonne numeriche:", 
                numeric_cols,
                default=numeric_cols[:5]  # Prime 5 per default
            )
            
            if len(corr_cols) >= 2:
                fig = general_viz.create_correlation_heatmap(
                    result[corr_cols], "Matrice di Correlazione"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Seleziona almeno 2 colonne per la correlazione")
    
    except Exception as e:
        st.error(f"❌ Errore nella creazione del grafico: {str(e)}")
        logger.error(f"Errore visualizzazione: {str(e)}")

# Template queries specifici per Twitter
def get_twitter_query_templates():
    """Template specifici per dati Twitter"""
    
    return {
        "📊 Analisi Tweet": {
            "Top 20 utenti per follower": f"""
                SELECT 
                    user.screen_name as username,
                    user.followers_count as followers,
                    user.friends_count as following,
                    user.verified as verified,
                    COUNT(*) as tweet_count
                FROM {DatabaseConfig.TEMP_VIEW_NAME}
                GROUP BY user.screen_name, user.followers_count, user.friends_count, user.verified
                ORDER BY user.followers_count DESC
                LIMIT 20
            """,
            
            "Tweet più popolari (like + retweet)": f"""
                SELECT 
                    user.screen_name as author,
                    text,
                    favorite_count as likes,
                    retweet_count as retweets,
                    (favorite_count + retweet_count) as total_engagement
                FROM {DatabaseConfig.TEMP_VIEW_NAME}
                WHERE retweeted_status IS NULL  -- Solo tweet originali
                ORDER BY total_engagement DESC
                LIMIT 20
            """,
            
            "Distribuzione tipi di tweet": f"""
                SELECT
                    CASE
                        WHEN retweeted_status IS NOT NULL THEN 'Retweet'
                        WHEN quoted_status_id_str IS NOT NULL THEN 'Quote Tweet'
                        WHEN reply_count > 0 THEN 'Tweet con Reply'
                        ELSE 'Tweet Originale'
                    END AS tweet_type,
                    COUNT(*) as count,
                    AVG(favorite_count) as avg_likes,
                    AVG(retweet_count) as avg_retweets
                FROM {DatabaseConfig.TEMP_VIEW_NAME}
                GROUP BY 1
                ORDER BY count DESC
            """,
            
            "Hashtag più usati": f"""
                SELECT 
                    hashtag,
                    COUNT(*) as frequency
                FROM (
                    SELECT explode(
                        split(
                            regexp_replace(text, '[^#\\w\\s]', ''), 
                            ' '
                        )
                    ) as hashtag
                    FROM {DatabaseConfig.TEMP_VIEW_NAME}
                    WHERE text LIKE '%#%'
                )
                WHERE hashtag LIKE '#%' AND length(hashtag) > 1
                GROUP BY hashtag
                ORDER BY frequency DESC
                LIMIT 30
            """
        }
    }

def show_query_templates_tab(query_engine, dataset):
    """Tab template migliorato con template Twitter"""
    
    st.markdown("### 📚 Template Query Predefiniti")
    
    # Template generali + Twitter specifici
    all_templates = {
        **get_twitter_query_templates(),
        "🔍 Query di Base": {
            "Prime 10 righe": f"SELECT * FROM {DatabaseConfig.TEMP_VIEW_NAME} LIMIT 10",
            "Conteggio totale": f"SELECT COUNT(*) as total_records FROM {DatabaseConfig.TEMP_VIEW_NAME}",
            "Schema tabella": f"DESCRIBE {DatabaseConfig.TEMP_VIEW_NAME}",
        },
        
        "📊 Aggregazioni Generali": {
            "Statistiche colonna numerica": f"""
                SELECT 
                    COUNT(*) as count,
                    AVG(favorite_count) as media,
                    MIN(favorite_count) as minimo,
                    MAX(favorite_count) as massimo,
                    STDDEV(favorite_count) as std_dev
                FROM {DatabaseConfig.TEMP_VIEW_NAME}
                WHERE favorite_count IS NOT NULL
            """,
            
            "Percentili": f"""
                SELECT 
                    PERCENTILE_APPROX(favorite_count, 0.25) as Q1,
                    PERCENTILE_APPROX(favorite_count, 0.5) as mediana,
                    PERCENTILE_APPROX(favorite_count, 0.75) as Q3,
                    PERCENTILE_APPROX(favorite_count, 0.9) as P90
                FROM {DatabaseConfig.TEMP_VIEW_NAME}
                WHERE favorite_count IS NOT NULL
            """,
        }
    }
    
    # Mostra template per categoria
    for category, queries in all_templates.items():
        with st.expander(category):
            for name, query in queries.items():
                col1, col2 = st.columns([4, 1])
                
                with col1:
                    st.code(query, language="sql")
                
                with col2:
                    if st.button("Usa", key=f"template_{category}_{name}"):
                        st.session_state.selected_template = query
                        st.success("✅ Template copiato!")
                        st.rerun()
    
    # Template personalizzati (mantenuto dal codice originale)
    show_custom_templates_section()

def show_custom_templates_section():
    """Sezione per template personalizzati"""
    
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
            with st.expander(f"📝 {name}"):
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

def show_history_tab(query_engine):
    """Tab cronologia migliorato"""
    
    st.markdown("### 📜 Cronologia Query")
    
    # Usa il metodo della classe per ottenere la cronologia
    history = query_engine.get_query_history()
    
    if not history:
        st.info("📝 Nessuna query eseguita ancora. Inizia dal tab 'Editor Query'!")
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
        
        # Status icon
        status_icon = "✅" if query_info.get('success', True) else "❌"
        rows = query_info.get('row_count', 'N/A')
        exec_time = query_info.get('execution_time', 0)
        
        with st.expander(f"{status_icon} Query #{index} - {rows} righe" + 
                        (f" - {exec_time:.2f}s" if exec_time > 0 else "")):
            
            col1, col2 = st.columns([4, 1])
            
            with col1:
                st.code(query_info['query'], language="sql")
                
                # Info aggiuntive
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
                
                # Copia query
                if st.button("📋 Copia", key=f"history_copy_{index}", help="Copia negli appunti"):
                    st.code(query_info['query'])
    
    # Azioni cronologia
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🗑️ Pulisci Cronologia", type="secondary"):
            if st.button("⚠️ Conferma Eliminazione", type="secondary"):
                query_engine.clear_history()
                st.success("🗑️ Cronologia pulita!")
                st.rerun()
    
    with col2:
        if history:
            # Statistiche cronologia
            total_queries = len(history)
            successful = len([h for h in history if h.get('success', True)])
            avg_time = sum(h.get('execution_time', 0) for h in history) / total_queries
            
            st.info(f"📊 Successo: {successful}/{total_queries} ({successful/total_queries*100:.1f}%)")
            if avg_time > 0:
                st.info(f"⏱️ Tempo medio: {avg_time:.2f}s")
    
    with col3:
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

# Mantieni le funzioni esistenti per compatibilità
show_query_templates_tab = show_query_templates_tab
show_results_tab = show_enhanced_results_tab