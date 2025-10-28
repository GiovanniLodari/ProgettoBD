"""
Modulo per le analisi e aggregazioni sui dati
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple, Any
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as functions

from src.config import Config, DatabaseConfig
from src.spark_manager import SparkManager

logger = logging.getLogger(__name__)

class DisasterAnalytics:
    """Classe per analisi"""
    
    def __init__(self, dataframe: SparkDataFrame):
        self.df = dataframe
        self.spark_manager = SparkManager()

class GeneralAnalytics:
    """Classe per analisi generali su qualsiasi dataset"""
    
    def __init__(self, dataframe: SparkDataFrame):
        self.df = dataframe
        self.spark_manager = SparkManager()
    
class QueryEngine:
    """Motore per l'esecuzione di query personalizzate"""
    
    def __init__(self):
        self.spark_manager = SparkManager()
        self.query_history = []