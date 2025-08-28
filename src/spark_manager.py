"""
Utilità per integrazione Spark nell'app di analisi disastri naturali
"""
from typing import Dict, Any, List, Optional
import streamlit as st
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import pandas as pd
import tempfile
import os
import logging
import shutil
from src.config import Config, SparkConfig

logger = logging.getLogger(__name__)

class SparkManager:
    """Gestore sessione Spark per Streamlit"""
    
    def __init__(self):
        self.spark = None
        self._session_initialized = False
        self.temp_dir = None
    
    def get_spark_session(self, dataset_size_mb=None):
        """Crea o recupera sessione Spark (cached)"""
        if self.spark is None or not self._session_initialized:

            try:
                hadoop_home = os.path.join(os.path.expanduser('~'), 'hadoop-3.3.6')
                os.environ['HADOOP_HOME'] = hadoop_home
                
                os.environ['PATH'] = f"{os.environ.get('PATH')};{os.path.join(hadoop_home, 'bin')}"
                logger.info(f"HADOOP_HOME impostato a: {os.environ['HADOOP_HOME']}")
            except Exception as e:
                logger.error(f"Errore nell'impostazione di HADOOP_HOME: {e}")
                st.warning("Potrebbero verificarsi errori di Hadoop/winutils su Windows.")
            
            # Configurazione base
            builder = SparkSession.builder \
                .appName(Config.SPARK_APP_NAME) \
                .master("local[*]") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .config("spark.driver.maxResultSize", "2g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.memory.fraction", "0.8")

            self.spark = builder.getOrCreate()
            
            # Configurazioni memoria dinamiche
            if dataset_size_mb:
                memory_configs = SparkConfig.get_memory_settings(dataset_size_mb)
                for key, value in memory_configs.items():
                    builder = builder.config(key, value)
            
            # Per sviluppo locale - modalità standalone
            try:
                self.spark = builder.master("local[*]").getOrCreate()
                self.spark.sparkContext.setLogLevel("WARN")  # Riduci logging
                self._session_initialized = True
                
                #st.success(f"✅ Spark initialized - Using {self.spark.sparkContext.defaultParallelism} cores")
                
            except Exception as e:
                st.error(f"❌ Spark initialization failed: {str(e)}")
                self.spark = None
        
        return self.spark
    
    @staticmethod
    @st.cache_resource
    def get_spark_manager():
        """Restituisce un'istanza singola e cached di SparkManager."""
        return SparkManager()
    
    def read_files_with_spark(self, uploaded_files, schema_type='auto'):
        """
        Carica file usando Spark con gestione schema compatibile per Twitter
        
        Args:
            uploaded_files: Lista di file uploadati
            schema_type: Tipo di schema ('auto', 'twitter', 'generic')
            
        Returns:
            tuple: (DataFrame combinato, informazioni di caricamento)
        """
        if not self.spark:
            raise Exception("Spark session not initialized")
        
        dataframes = []
        load_info = []
        temp_files = []
        
        # Crea directory temporanea
        if not self.temp_dir:
            self.temp_dir = tempfile.mkdtemp()
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total_size_mb = sum(f.size for f in uploaded_files) / 1024 / 1024
        
        # Determina se usare gestione Twitter
        is_twitter_data = (schema_type == 'twitter' or 
                          (schema_type == 'auto' and self._detect_twitter_files(uploaded_files)))
        
        if is_twitter_data:
            st.info("🐦 Rilevati dati Twitter - Applicando normalizzazione schema compatibile")
        
        for idx, uploaded_file in enumerate(uploaded_files):
            try:
                status_text.text(f"Loading {uploaded_file.name} with Spark ({idx+1}/{len(uploaded_files)})")
                progress_bar.progress((idx + 1) / len(uploaded_files))
                
                # Salva file temporaneamente
                temp_path = os.path.join(self.temp_dir, uploaded_file.name)
                temp_files.append(temp_path)
                
                with open(temp_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                # Leggi con Spark - SENZA schema predefinito per evitare conflitti
                spark_df = None
                
                if uploaded_file.name.endswith('.csv'):
                    spark_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(temp_path)
                
                elif uploaded_file.name.endswith(('.json', '.jsonl')):
                    spark_df = self.spark.read.option("multiline", "true").json(temp_path)
                
                elif uploaded_file.name.endswith('.parquet'):
                    spark_df = self.spark.read.parquet(temp_path)
                
                elif uploaded_file.name.endswith('.json.gz'):
                    spark_df = self.spark.read.option("compression", "gzip").json(temp_path)
                
                if spark_df is not None:
                    # Aggiungi metadati
                    spark_df = spark_df.withColumn("source_file", F.lit(uploaded_file.name)) \
                                     .withColumn("source_id", F.lit(idx + 1))
                    
                    # Normalizza per Twitter se necessario
                    if is_twitter_data:
                        spark_df = self._normalize_twitter_schema(spark_df)
                    
                    dataframes.append(spark_df)
                    
                    # Info per il report
                    row_count = spark_df.count()
                    col_count = len(spark_df.columns)
                    
                    load_info.append({
                        'file': uploaded_file.name,
                        'rows': row_count,
                        'columns': col_count,
                        'size_mb': uploaded_file.size / 1024 / 1024,
                        'engine': 'Spark',
                        'schema_applied': 'Twitter Normalized' if is_twitter_data else 'Auto'
                    })
                
            except Exception as e:
                logger.error(f"Error loading {uploaded_file.name}: {str(e)}")
                st.error(f"Error loading {uploaded_file.name}: {str(e)}")
        
        progress_bar.empty()
        status_text.empty()
        
        # Cleanup file temporanei
        # for temp_file in temp_files:
        #     try:
        #         if os.path.exists(temp_file):
        #             os.remove(temp_file)
        #     except Exception as e:
        #         logger.warning(f"Impossibile rimuovere file temporaneo {temp_file}: {e}")
        
        # Unisci tutti i DataFrame Spark
        if dataframes:
            try:
                st.info(f"Unendo {len(dataframes)} DataFrame...")
                
                # Usa sempre unionByName con allowMissingColumns per massima flessibilità
                combined_spark_df = dataframes[0]
                for i, df in enumerate(dataframes[1:], 1):
                    try:
                        combined_spark_df = combined_spark_df.unionByName(df, allowMissingColumns=True)
                        st.info(f"Unito DataFrame {i+1}/{len(dataframes)}")
                    except Exception as e:
                        logger.error(f"Errore unione DataFrame {i+1}: {str(e)}")
                        # Fallback: converti tutto a stringhe e riprova
                        st.warning(f"Fallback per DataFrame {i+1}: conversione campi complessi a stringa")
                        df_flattened = self._flatten_complex_types(df)
                        combined_flattened = self._flatten_complex_types(combined_spark_df)
                        combined_spark_df = combined_flattened.unionByName(df_flattened, allowMissingColumns=True)
                
                st.success(f"✅ Successfully combined {len(dataframes)} files into single dataset")
                return combined_spark_df, load_info
                
            except Exception as e:
                logger.error(f"Errore critico nell'unione dei DataFrame: {str(e)}")
                st.error(f"Errore nell'unione dei DataFrame: {str(e)}")
                
                # Ultimo tentativo: flattening completo
                try:
                    st.warning("Tentativo di recupero: conversione tutti i campi complessi a stringhe...")
                    flattened_dfs = [self._flatten_complex_types(df) for df in dataframes]
                    
                    combined_spark_df = flattened_dfs[0]
                    for df in flattened_dfs[1:]:
                        combined_spark_df = combined_spark_df.unionByName(df, allowMissingColumns=True)
                    
                    st.success("✅ Recupero riuscito con flattening completo")
                    return combined_spark_df, load_info
                    
                except Exception as final_error:
                    logger.error(f"Errore finale: {str(final_error)}")
                    st.error(f"Impossibile unire i file: {str(final_error)}")
                    return None, []
        
        return None, []
    
    def _normalize_twitter_schema(self, df: DataFrame) -> DataFrame:
        """
        Normalizza schema Twitter convertendo campi problematici a stringhe
        per garantire compatibilità nell'union
        
        Args:
            df: DataFrame da normalizzare
            
        Returns:
            DataFrame: DataFrame normalizzato
        """
        try:
            # Lista dei campi complessi che causano problemi
            complex_fields_to_stringify = [
                "entities", "extended_entities", "extended_tweet", 
                "quoted_status", "retweeted_status", "user", "place"
            ]
            
            # Converti campi complessi a JSON string
            for field_name in complex_fields_to_stringify:
                if field_name in df.columns:
                    df = df.withColumn(field_name, F.to_json(F.col(field_name)))
            
            return df
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione Twitter: {str(e)}")
            return df
    
    def _flatten_complex_types(self, df: DataFrame) -> DataFrame:
        """
        Converte tutti i tipi complessi (struct, array) a stringhe JSON
        
        Args:
            df: DataFrame da appiattire
            
        Returns:
            DataFrame: DataFrame con tipi semplificati
        """
        try:
            for field in df.schema.fields:
                if isinstance(field.dataType, (StructType, ArrayType)):
                    df = df.withColumn(field.name, F.to_json(F.col(field.name)))
            
            return df
            
        except Exception as e:
            logger.error(f"Errore nel flattening: {str(e)}")
            return df
    
    def _detect_twitter_files(self, uploaded_files) -> bool:
        """
        Rileva se i file caricati contengono dati Twitter
        
        Args:
            uploaded_files: Lista di file uploadati
            
        Returns:
            bool: True se sembrano file Twitter
        """
        twitter_indicators = 0
        
        for uploaded_file in uploaded_files:
            # Controlla il nome del file
            filename_lower = uploaded_file.name.lower()
            if any(keyword in filename_lower for keyword in ['tweet', 'twitter', 'x_', 'social']):
                twitter_indicators += 1
            
            # Se è JSON, prova a controllare il contenuto del primo file
            if uploaded_file.name.endswith(('.json', '.jsonl')) and twitter_indicators == 0:
                try:
                    # Leggi le prime righe per identificare la struttura
                    uploaded_file.seek(0)
                    sample = uploaded_file.read(1024).decode('utf-8', errors='ignore')
                    uploaded_file.seek(0)  # Reset position
                    
                    twitter_fields = ['created_at', 'user', 'entities', 'text', 'id']
                    found_fields = sum(1 for field in twitter_fields if field in sample)
                    
                    if found_fields >= 3:
                        twitter_indicators += 2
                        break
                        
                except Exception:
                    pass
        
        return twitter_indicators >= 2
    
    def spark_df_to_pandas_sample(self, spark_df, sample_size=10000):
        """Converte un campione del DataFrame Spark in Pandas per visualizzazione"""
        if spark_df is None:
            return pd.DataFrame()
            
        try:
            total_count = spark_df.count()
            if total_count > sample_size:
                # Prendi campione casuale
                fraction = sample_size / total_count
                sampled_df = spark_df.sample(fraction=fraction, seed=42)
                return sampled_df.toPandas()
            else:
                return spark_df.toPandas()
        except Exception as e:
            logger.error(f"Errore nella conversione a Pandas: {str(e)}")
            return pd.DataFrame()
    
    def get_basic_stats(self, spark_df):
        """Statistiche base usando Spark SQL"""
        if spark_df is None:
            return {}
            
        stats = {}
        
        try:
            # Count totale
            stats['total_rows'] = spark_df.count()
            stats['total_columns'] = len(spark_df.columns)
            
            # Colonne numeriche per statistiche
            numeric_cols = [field.name for field in spark_df.schema.fields 
                           if field.dataType in [IntegerType(), LongType(), FloatType(), DoubleType()]]
            
            if numeric_cols:
                # Usa describe() di Spark per statistiche aggregate
                describe_df = spark_df.describe(numeric_cols)
                stats['describe'] = describe_df.toPandas()
            
            # Null counts per tutte le colonne
            null_counts = {}
            for col_name in spark_df.columns:
                null_count = spark_df.filter(F.col(col_name).isNull()).count()
                null_counts[col_name] = null_count
            
            stats['null_counts'] = null_counts
            
        except Exception as e:
            logger.error(f"Errore nel calcolo delle statistiche: {str(e)}")
            stats = {'error': str(e)}
        
        return stats
    
    def optimize_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Applica ottimizzazioni di base a un DataFrame, come il caching.
        Il caching memorizza il DataFrame in memoria per un accesso più rapido.
        """
        if df is None:
            return df
            
        logger.info("Ottimizzazione del DataFrame (caching)...")
        return df.cache()

    def create_temp_view(self, df: DataFrame, view_name: str) -> None:
        """
        Crea o rimpiazza una vista temporanea SQL dal DataFrame.
        Questo permette di eseguire query SQL direttamente sul DataFrame.
        """
        if df is None:
            logger.warning("Tentativo di creare vista da DataFrame None")
            return
            
        logger.info(f"Creazione della vista temporanea: {view_name}")
        df.createOrReplaceTempView(view_name)

    def get_dataframe_stats(self, df: DataFrame) -> Dict[str, Any]:
        """
        Estrae statistiche di base da un DataFrame.
        """
        if df is None:
            return {'error': 'DataFrame is None'}
            
        logger.info("Calcolo delle statistiche del DataFrame...")
        try:
            stats = {
                'count': df.count(),
                'columns': len(df.columns),
                'partitions': df.rdd.getNumPartitions(),
                'schema': df.dtypes  # dtypes restituisce una lista di tuple (nome_colonna, tipo_dato)
            }
            return stats
        except Exception as e:
            logger.error(f"Errore nel calcolo statistiche: {str(e)}")
            return {'error': str(e)}
    
    def create_aggregation_spark(self, spark_df, group_col, agg_col=None, agg_func='count'):
        """Aggregazione usando Spark SQL"""
        if spark_df is None:
            return pd.DataFrame()
            
        try:
            if agg_func == 'count':
                result = spark_df.groupBy(group_col).count().orderBy(F.desc('count')).limit(20)
            else:
                if agg_col:
                    if agg_func == 'sum':
                        result = spark_df.groupBy(group_col).agg(F.sum(agg_col).alias(f'sum_{agg_col}')) \
                                       .orderBy(F.desc(f'sum_{agg_col}')).limit(20)
                    elif agg_func == 'avg':
                        result = spark_df.groupBy(group_col).agg(F.avg(agg_col).alias(f'avg_{agg_col}')) \
                                       .orderBy(F.desc(f'avg_{agg_col}')).limit(20)
                    elif agg_func == 'max':
                        result = spark_df.groupBy(group_col).agg(F.max(agg_col).alias(f'max_{agg_col}')) \
                                       .orderBy(F.desc(f'max_{agg_col}')).limit(20)
                    elif agg_func == 'min':
                        result = spark_df.groupBy(group_col).agg(F.min(agg_col).alias(f'min_{agg_col}')) \
                                       .orderBy(F.desc(f'min_{agg_col}')).limit(20)
                    else:
                        result = spark_df.groupBy(group_col).count().orderBy(F.desc('count')).limit(20)
                else:
                    result = spark_df.groupBy(group_col).count().orderBy(F.desc('count')).limit(20)
            
            return result.toPandas()  # Converti per display in Streamlit
            
        except Exception as e:
            logger.error(f"Errore nell'aggregazione: {str(e)}")
            return pd.DataFrame()
    
    def execute_sql_query(self, query: str, view_name: str = None) -> Optional[DataFrame]:
        """
        Esegue una query SQL sulla vista temporanea
        
        Args:
            query: Query SQL da eseguire
            view_name: Nome della vista (opzionale, usa quella di default)
            
        Returns:
            DataFrame: Risultato della query o None se errore
        """
        try:
            if not self.spark:
                raise Exception("Spark session not initialized")
            
            result_df = self.spark.sql(query)
            return result_df
            
        except Exception as e:
            logger.error(f"Errore nell'esecuzione della query SQL: {str(e)}")
            st.error(f"Errore nella query SQL: {str(e)}")
            return None
    
    def cleanup(self):
        """Chiudi sessione Spark"""
        if self.spark:
            self.spark.stop()
            self.spark = None
            self._session_initialized = False
        
def cleanup_spark():
    """Funzione di utilità esterna per chiudere la sessione Spark."""
    manager = SparkManager.get_spark_manager()
    manager.cleanup()

def get_file_size_mb(uploaded_files):
    """Calcola dimensione totale file caricati"""
    return sum(f.size for f in uploaded_files) / 1024 / 1024

def should_use_spark(file_size_mb, num_files, uploaded_files=None):
    """Decide se usare Spark basandosi su dimensione e numero file"""
    # Usa Spark se:
    # - File > 500MB totali 
    # - Più di 5 file
    # - File singolo > 100MB
    if uploaded_files is not None and hasattr(uploaded_files, '__iter__'):
        return file_size_mb > 500 or num_files > 5 or any(f.size > 100*1024*1024 for f in uploaded_files)
    else:
        return file_size_mb > 100

@st.cache_data
def detect_data_schema(sample_df):
    """Rileva schema dati per ottimizzazioni"""
    schema_info = {}
    
    for col in sample_df.columns:
        col_type = str(sample_df[col].dtype)
        unique_count = sample_df[col].nunique()
        null_pct = sample_df[col].isnull().sum() / len(sample_df)
        
        schema_info[col] = {
            'type': col_type,
            'unique_values': unique_count,
            'null_percentage': null_pct,
            'is_categorical': unique_count < len(sample_df) * 0.05,  # < 5% valori unici
            'potential_date': 'date' in col.lower() or 'time' in col.lower()
        }
    
    return schema_info