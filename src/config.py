"""
Configurazioni dell'applicazione per l'analisi dei disastri naturali
"""

import os

class Config:
    # Configurazioni Streamlit
    PAGE_TITLE = "Analisi Disastri Naturali"
    PAGE_ICON = "🌪️"
    LAYOUT = "wide"
    
    # Configurazioni Spark
    SPARK_APP_NAME = "DisasterAnalysis"
    SPARK_CONFIGS = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
    
    # Percorsi
    DATA_DIR = "data"
    TEMP_DIR = "/tmp"
    
    # Formati file supportati
    SUPPORTED_FORMATS = ['csv', 'json', 'parquet', 'gz']
    
    # Configurazioni per i grafici
    CHART_CONFIG = {
        'height': 500,
        'use_container_width': True,
        'theme': 'streamlit'
    }
    
    # Colori per i grafici
    COLOR_PALETTE = [
        '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4',
        '#FFEAA7', '#DDA0DD', '#98D8C8', '#F7DC6F'
    ]
    
    # Configurazioni per le query
    DEFAULT_QUERY_LIMIT = 1000
    MAX_DISPLAY_ROWS = 100
    
    # Messaggi
    MESSAGES = {
        'loading': "Caricamento dati in corso...",
        'success': "✅ Dataset caricato con successo!",
        'error_loading': "❌ Errore nel caricamento del file",
        'no_data': "👆 Carica un dataset per iniziare l'analisi",
        'processing': "Elaborazione con Spark in corso..."
    }
    
    # Tipi di aggregazione disponibili
    AGGREGATION_TYPES = {
        'count': 'Conteggio',
        'sum': 'Somma',
        'avg': 'Media',
        'max': 'Massimo',
        'min': 'Minimo',
        'stddev': 'Deviazione Standard'
    }
    
    # Configurazioni per l'analisi temporale
    TIME_FORMATS = [
        'yyyy-MM-dd',
        'dd/MM/yyyy',
        'MM/dd/yyyy',
        'yyyy-MM-dd HH:mm:ss'
    ]
    
class SparkConfig:
    """Configurazioni specifiche per Spark"""
    
    @staticmethod
    def get_spark_conf():
        """Ritorna la configurazione Spark"""
        return Config.SPARK_CONFIGS
    
    @staticmethod
    def get_memory_settings(dataset_size_mb=None):
        """Calcola le impostazioni di memoria basate sulla dimensione del dataset"""
        if dataset_size_mb is None:
            return {}
        
        if dataset_size_mb < 100:  # Dataset piccolo
            return {
                "spark.executor.memory": "1g",
                "spark.driver.memory": "1g"
            }
        elif dataset_size_mb < 1000:  # Dataset medio
            return {
                "spark.executor.memory": "2g",
                "spark.driver.memory": "2g"
            }
        else:  # Dataset grande
            return {
                "spark.executor.memory": "4g",
                "spark.driver.memory": "4g"
            }

class DatabaseConfig:
    """Configurazioni per i database (se necessario)"""
    
    # Nome della vista temporanea Spark
    TEMP_VIEW_NAME = "disasters"
    
    # Query di esempio
    SAMPLE_QUERIES = {
        'basic': "SELECT * FROM disasters LIMIT 10",
        'count_by_type': "SELECT disaster_type, COUNT(*) as count FROM disasters GROUP BY disaster_type ORDER BY count DESC",
        'yearly_trend': "SELECT YEAR(date) as year, COUNT(*) as disasters FROM disasters GROUP BY YEAR(date) ORDER BY year",
        'top_countries': "SELECT country, COUNT(*) as disasters FROM disasters GROUP BY country ORDER BY disasters DESC LIMIT 10"
    }