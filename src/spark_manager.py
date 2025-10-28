"""
Utilità per integrazione Spark nell'app di analisi disastri naturali
"""
from typing import Dict, Any
import streamlit as st
import os, sys
import logging
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)

class SparkManager:
    """Gestore sessione Spark per Streamlit"""
    
    def __init__(self):
        self.spark = None
        self._session_initialized = False
        self.temp_dir = None
    
    def get_spark_session(self, dataset_size_mb=None):
        """Crea o recupera sessione Spark"""
        if self.spark is None or not self._session_initialized:

            try:
                hadoop_home = os.path.join(os.path.expanduser('~'), 'hadoop-3.3.6')
                os.environ['HADOOP_HOME'] = hadoop_home
                
                os.environ['PATH'] = f"{os.environ.get('PATH')};{os.path.join(hadoop_home, 'bin')}"
                logger.info(f"HADOOP_HOME impostato a: {os.environ['HADOOP_HOME']}")
                os.environ['PYSPARK_PYTHON'] = sys.executable
                os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
            except Exception as e:
                logger.error(f"Errore nell'impostazione di HADOOP_HOME: {e}")
                st.warning("Potrebbero verificarsi errori di Hadoop/winutils su Windows.")

            log_dir_base = "C:\\tmp"
            event_log_dir = os.path.join(log_dir_base, "spark-events")

            try:
                os.makedirs(event_log_dir, exist_ok=True)
                logger.info(f"Cartella per i log di Spark assicurata: {event_log_dir}")
            except OSError as e:
                logger.error(f"Impossibile creare la directory per i log di Spark: {e}")
                st.error(f"Errore di permessi: non è stato possibile creare la cartella {event_log_dir}")
                return None
            
            num_cores = 14

            builder = SparkSession.builder \
                .appName("DisasterAnalysis") \
                .master(f"local[{num_cores}]") \
                .config("spark.driver.memory", "8g") \
                .config("spark.executor.memory", "8g") \
                .config("spark.driver.maxResultSize", "8g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.eventLog.enabled", "true") \
                .config("spark.eventLog.dir", "/tmp/spark-events") \
                .config("spark.sql.shuffle.partitions", num_cores * 3) \
                .config("spark.memory.fraction", "0.8")

            self.spark = builder.getOrCreate()
            
            try:
                self.spark = builder.master("local[*]").getOrCreate()
                self.spark.sparkContext.setLogLevel("WARN")
                self._session_initialized = True
                                
            except Exception as e:
                st.error(f"❌ Spark initialization failed: {str(e)}")
                self.spark = None
        
        return self.spark
    
    @staticmethod
    @st.cache_resource
    def get_spark_manager():
        """Restituisce un'istanza singola (cached) di SparkManager."""
        return SparkManager()
    
    def get_dataframe_stats(self, df: DataFrame) -> Dict[str, Any]:
        """Estrae informazioni di base da un DataFrame."""
        if df is None:
            return {'error': 'DataFrame is None'}
            
        logger.info("Calcolo delle statistiche del DataFrame...")
        try:
            stats = {
                'count': df.count(),
                'columns': len(df.columns),
                'partitions': df.rdd.getNumPartitions(),
                'schema': df.dtypes
            }
            return stats
        except Exception as e:
            logger.error(f"Errore nel calcolo statistiche: {str(e)}")
            return {'error': str(e)}
    
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
    if uploaded_files is not None and hasattr(uploaded_files, '__iter__'):
        return file_size_mb > 500 or num_files > 5 or any(f.size > 100*1024*1024 for f in uploaded_files)
    else:
        return file_size_mb > 100