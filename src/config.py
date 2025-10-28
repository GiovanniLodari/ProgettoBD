"""
Configurazione3 dell'applicazione
"""

import os

class Config:
    PAGE_TITLE = "Analisi Disastri Naturali"
    PAGE_ICON = "🌪️"
    LAYOUT = "wide"
    
    SPARK_APP_NAME = "DisasterAnalysis"
    SPARK_CONFIGS = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
    
    DATA_DIR = "data"
    TEMP_DIR = "/tmp"
    SUPPORTED_FORMATS = ['csv', 'json', 'parquet', 'gz']    

    CHART_CONFIG = {
        'height': 500,
        'use_container_width': True,
        'theme': 'streamlit'
    }    

    COLOR_PALETTE = [
        '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4',
        '#FFEAA7', '#DDA0DD', '#98D8C8', '#F7DC6F'
    ]    

    DEFAULT_QUERY_LIMIT = 1000
    MAX_DISPLAY_ROWS = 100

    MESSAGES = {
        'loading': "Caricamento dati in corso...",
        'success': "✅ Dataset caricato con successo!",
        'error_loading': "❌ Errore nel caricamento del file",
        'no_data': "👆 Carica un dataset per iniziare l'analisi",
        'processing': "Elaborazione con Spark in corso..."
    }
    
    AGGREGATION_TYPES = {
        'count': 'Conteggio',
        'sum': 'Somma',
        'avg': 'Media',
        'max': 'Massimo',
        'min': 'Minimo',
        'stddev': 'Deviazione Standard'
    }
    
    TIME_FORMATS = [
        'yyyy-MM-dd',
        'dd/MM/yyyy',
        'MM/dd/yyyy',
        'yyyy-MM-dd HH:mm:ss'
    ]
    
class SparkConfig:
    """Configurazione per Spark"""
    
    @staticmethod
    def get_spark_conf():
        """Ritorna la configurazione Spark"""
        return Config.SPARK_CONFIGS
    
    @staticmethod
    def get_memory_settings(dataset_size_mb=None):
        """Calcola le impostazioni di memoria basate sulla dimensione del dataset"""
        if dataset_size_mb is None:
            return {}
        
        if dataset_size_mb < 100:
            return {
                "spark.executor.memory": "2g",
                "spark.driver.memory": "2g"
            }
        elif dataset_size_mb < 1000:
            return {
                "spark.executor.memory": "4g",
                "spark.driver.memory": "4g"
            }
        else:
            return {
                "spark.executor.memory": "8g",
                "spark.driver.memory": "8g"
            }

class DatabaseConfig:
    """Configurazioni per il database"""
    
    TEMP_VIEW_NAME = "disasters"