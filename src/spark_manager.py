"""
Gestione della sessione Apache Spark
"""

import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import logging

from .config import Config, SparkConfig

logger = logging.getLogger(__name__)

class SparkManager:
    """Classe per gestire la sessione Spark"""
    
    _instance = None
    _spark_session = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkManager, cls).__new__(cls)
        return cls._instance
    
    @classmethod
    def get_spark_session(cls, dataset_size_mb=None):
        """
        Ottieni o crea una sessione Spark
        
        Args:
            dataset_size_mb (float, optional): Dimensione del dataset in MB per ottimizzare la configurazione
        
        Returns:
            SparkSession: Sessione Spark configurata
        """
        if cls._spark_session is None:
            cls._spark_session = cls._create_spark_session(dataset_size_mb)
        
        return cls._spark_session
    
    @classmethod
    def _create_spark_session(cls, dataset_size_mb=None):
        """Crea una nuova sessione Spark con configurazioni ottimizzate"""
        try:
            builder = SparkSession.builder.appName(Config.SPARK_APP_NAME)
            
            # Applica configurazioni base
            for key, value in SparkConfig.get_spark_conf().items():
                builder = builder.config(key, value)
            
            # Applica configurazioni memoria se specificata la dimensione del dataset
            if dataset_size_mb:
                memory_config = SparkConfig.get_memory_settings(dataset_size_mb)
                for key, value in memory_config.items():
                    builder = builder.config(key, value)
            
            # Crea la sessione
            spark = builder.getOrCreate()
            
            # Configura il livello di log
            spark.sparkContext.setLogLevel("WARN")
            
            logger.info("Sessione Spark creata con successo")
            return spark
            
        except Exception as e:
            logger.error(f"Errore nella creazione della sessione Spark: {str(e)}")
            st.error(f"Errore nell'inizializzazione di Spark: {str(e)}")
            raise
    
    @classmethod
    def stop_session(cls):
        """Ferma la sessione Spark"""
        if cls._spark_session:
            cls._spark_session.stop()
            cls._spark_session = None
            logger.info("Sessione Spark terminata")
    
    @classmethod
    def get_session_info(cls):
        """Ottieni informazioni sulla sessione Spark corrente"""
        if cls._spark_session is None:
            return None
        
        spark = cls._spark_session
        return {
            'version': spark.version,
            'master': spark.sparkContext.master,
            'app_name': spark.sparkContext.appName,
            'app_id': spark.sparkContext.applicationId,
            'ui_web_url': spark.sparkContext.uiWebUrl,
            'default_parallelism': spark.sparkContext.defaultParallelism
        }
    
    @classmethod
    def execute_query(cls, query, use_cache=True):
        """
        Esegue una query SQL
        
        Args:
            query (str): Query SQL da eseguire
            use_cache (bool): Se utilizzare la cache per la query
        
        Returns:
            DataFrame: Risultato della query
        """
        try:
            spark = cls.get_spark_session()
            
            if use_cache:
                result = spark.sql(query).cache()
            else:
                result = spark.sql(query)
            
            return result
            
        except Exception as e:
            logger.error(f"Errore nell'esecuzione della query: {str(e)}")
            raise
    
    @classmethod
    def create_temp_view(cls, df, view_name):
        """
        Crea una vista temporanea da un DataFrame
        
        Args:
            df: DataFrame Spark
            view_name (str): Nome della vista temporanea
        """
        try:
            df.createOrReplaceTempView(view_name)
            logger.info(f"Vista temporanea '{view_name}' creata con successo")
        except Exception as e:
            logger.error(f"Errore nella creazione della vista temporanea: {str(e)}")
            raise
    
    @classmethod
    def optimize_dataframe(cls, df, partition_by=None, cache=True):
        """
        Ottimizza un DataFrame per le performance
        
        Args:
            df: DataFrame Spark
            partition_by (str, optional): Colonna per il partizionamento
            cache (bool): Se cacheare il DataFrame
        
        Returns:
            DataFrame: DataFrame ottimizzato
        """
        try:
            # Ripartiziona se specificato
            if partition_by and partition_by in df.columns:
                df = df.repartition(col(partition_by))
            
            # Cache se richiesto
            if cache:
                df = df.cache()
            
            # Forza la materializzazione per valutare le ottimizzazioni
            df.count()
            
            logger.info("DataFrame ottimizzato con successo")
            return df
            
        except Exception as e:
            logger.error(f"Errore nell'ottimizzazione del DataFrame: {str(e)}")
            return df
    
    @classmethod
    def get_dataframe_stats(cls, df):
        """
        Ottieni statistiche su un DataFrame
        
        Args:
            df: DataFrame Spark
        
        Returns:
            dict: Statistiche del DataFrame
        """
        try:
            return {
                'count': df.count(),
                'columns': len(df.columns),
                'partitions': df.rdd.getNumPartitions(),
                'schema': [(field.name, str(field.dataType)) for field in df.schema.fields]
            }
        except Exception as e:
            logger.error(f"Errore nel calcolo delle statistiche: {str(e)}")
            return {}

@st.cache_resource
def get_cached_spark_session():
    """Versione cached della sessione Spark per Streamlit"""
    #return SparkManager.get_spark_session()

def cleanup_spark():
    """Funzione per pulire le risorse Spark alla chiusura dell'app"""
    #SparkManager.stop_session()