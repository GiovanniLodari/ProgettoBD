"""
Modulo per il caricamento e la gestione dei dati
"""

import streamlit as st
import pandas as pd
import gzip
import os
import logging
from typing import Optional, Union
from pyspark.sql import DataFrame as SparkDataFrame

from .config import Config, DatabaseConfig
from .spark_manager import SparkManager

logger = logging.getLogger(__name__)

class DataLoader:
    """Classe per il caricamento dei dati con vari formati"""
    
    def __init__(self):
        self.spark_manager = SparkManager()
        self.current_dataset = None
        self.dataset_metadata = {}
    
    def load_file(self, file_path: str, file_format: str = None) -> Optional[SparkDataFrame]:
        """
        Carica un file in un DataFrame Spark
        
        Args:
            file_path (str): Percorso del file
            file_format (str, optional): Formato del file ('csv', 'json', 'parquet')
        
        Returns:
            SparkDataFrame: DataFrame caricato o None se errore
        """
        try:
            spark = self.spark_manager.get_spark_session()
            
            # Determina il formato automaticamente se non specificato
            if file_format is None:
                file_format = self._detect_format(file_path)
            
            logger.info(f"Caricamento file: {file_path} (formato: {file_format})")
            
            # Carica in base al formato
            if file_format == 'csv':
                df = self._load_csv(spark, file_path)
            elif file_format == 'json':
                df = self._load_json(spark, file_path)
            elif file_format == 'parquet':
                df = self._load_parquet(spark, file_path)
            else:
                # Tentativo con CSV come default
                df = self._load_csv(spark, file_path)
            
            if df is not None:
                # Ottimizza il DataFrame
                df = self.spark_manager.optimize_dataframe(df)
                
                # Crea vista temporanea
                self.spark_manager.create_temp_view(df, DatabaseConfig.TEMP_VIEW_NAME)
                
                # Salva metadata
                self._update_metadata(df, file_path)
                
                self.current_dataset = df
                logger.info("Dataset caricato con successo")
                
            return df
            
        except Exception as e:
            logger.error(f"Errore nel caricamento del file: {str(e)}")
            st.error(f"Errore nel caricamento: {str(e)}")
            return None
    
    def _load_csv(self, spark, file_path: str) -> SparkDataFrame:
        """Carica un file CSV"""
        return spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .option("multiline", "true") \
            .option("escape", '"') \
            .csv(file_path)
    
    def _load_json(self, spark, file_path: str) -> SparkDataFrame:
        """Carica un file JSON"""
        return spark.read \
            .option("multiline", "true") \
            .json(file_path)
    
    def _load_parquet(self, spark, file_path: str) -> SparkDataFrame:
        """Carica un file Parquet"""
        return spark.read.parquet(file_path)
    
    def _detect_format(self, file_path: str) -> str:
        """Rileva automaticamente il formato del file"""
        extension = file_path.lower().split('.')[-1]
        
        format_mapping = {
            'csv': 'csv',
            'json': 'json',
            'jsonl': 'json',
            'parquet': 'parquet',
            'pq': 'parquet'
        }
        
        return format_mapping.get(extension, 'csv')
    
    def _update_metadata(self, df: SparkDataFrame, file_path: str):
        """Aggiorna i metadata del dataset"""
        stats = self.spark_manager.get_dataframe_stats(df)
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        
        self.dataset_metadata = {
            'file_path': file_path,
            'file_size_bytes': file_size,
            'file_size_mb': file_size / (1024 * 1024),
            'record_count': stats.get('count', 0),
            'column_count': stats.get('columns', 0),
            'partitions': stats.get('partitions', 0),
            'schema': stats.get('schema', [])
        }
    
    def get_sample_data(self, limit: int = 100) -> pd.DataFrame:
        """
        Ottieni un campione del dataset come Pandas DataFrame
        
        Args:
            limit (int): Numero massimo di righe da restituire
        
        Returns:
            pd.DataFrame: Campione dei dati
        """
        if self.current_dataset is None:
            return pd.DataFrame()
        
        try:
            return self.current_dataset.limit(limit).toPandas()
        except Exception as e:
            logger.error(f"Errore nel campionamento dei dati: {str(e)}")
            return pd.DataFrame()
    
    def get_column_info(self) -> pd.DataFrame:
        """
        Ottieni informazioni sulle colonne del dataset
        
        Returns:
            pd.DataFrame: Informazioni sulle colonne
        """
        if not self.dataset_metadata.get('schema'):
            return pd.DataFrame()
        
        schema_info = []
        for col_name, col_type in self.dataset_metadata['schema']:
            # Calcola statistiche base per colonne numeriche
            try:
                if self.current_dataset and 'int' in col_type.lower() or 'double' in col_type.lower() or 'float' in col_type.lower():
                    stats = self.current_dataset.select(col_name).describe().collect()
                    null_count = self.current_dataset.filter(f"`{col_name}` IS NULL").count()
                else:
                    stats = None
                    null_count = self.current_dataset.filter(f"`{col_name}` IS NULL").count() if self.current_dataset else 0
                
                schema_info.append({
                    'Colonna': col_name,
                    'Tipo': col_type,
                    'Valori Null': null_count,
                    'Statistiche': 'Disponibili' if stats else 'N/A'
                })
            except Exception:
                schema_info.append({
                    'Colonna': col_name,
                    'Tipo': col_type,
                    'Valori Null': 'N/A',
                    'Statistiche': 'N/A'
                })
        
        return pd.DataFrame(schema_info)
    
    def get_metadata(self) -> dict:
        """Ottieni i metadata del dataset corrente"""
        return self.dataset_metadata.copy()
    
    def validate_dataset(self) -> dict:
        """
        Valida il dataset e restituisce un report di qualità
        
        Returns:
            dict: Report di validazione
        """
        if self.current_dataset is None:
            return {'status': 'error', 'message': 'Nessun dataset caricato'}
        
        try:
            df = self.current_dataset
            total_records = df.count()
            
            if total_records == 0:
                return {'status': 'warning', 'message': 'Dataset vuoto'}
            
            # Controlla colonne con tutti valori null
            null_columns = []
            for col_name in df.columns:
                null_count = df.filter(f"`{col_name}` IS NULL").count()
                if null_count == total_records:
                    null_columns.append(col_name)
            
            # Controlla duplicati (su tutte le colonne)
            duplicate_count = df.count() - df.dropDuplicates().count()
            
            validation_report = {
                'status': 'success',
                'total_records': total_records,
                'duplicate_records': duplicate_count,
                'columns_all_null': null_columns,
                'quality_score': self._calculate_quality_score(total_records, duplicate_count, len(null_columns), len(df.columns))
            }
            
            return validation_report
            
        except Exception as e:
            logger.error(f"Errore nella validazione: {str(e)}")
            return {'status': 'error', 'message': f'Errore durante la validazione: {str(e)}'}
    
    def _calculate_quality_score(self, total_records: int, duplicates: int, null_cols: int, total_cols: int) -> float:
        """Calcola un punteggio di qualità del dataset (0-100)"""
        if total_records == 0:
            return 0.0
        
        # Penalità per duplicati
        duplicate_penalty = min(50, (duplicates / total_records) * 100)
        
        # Penalità per colonne completamente null
        null_col_penalty = (null_cols / total_cols) * 30 if total_cols > 0 else 0
        
        # Punteggio base
        score = 100 - duplicate_penalty - null_col_penalty
        
        return max(0.0, min(100.0, score))

class FileHandler:
    """Utility per gestire file uploadati"""
    
    @staticmethod
    def handle_uploaded_file(uploaded_file) -> Optional[str]:
        """
        Gestisce un file uploadato e lo salva temporaneamente
        
        Args:
            uploaded_file: File uploadato da Streamlit
        
        Returns:
            str: Percorso del file salvato o None se errore
        """
        try:
            # Crea directory temporanea se non esiste
            temp_dir = Config.TEMP_DIR
            os.makedirs(temp_dir, exist_ok=True)
            
            # Percorso del file temporaneo
            temp_path = os.path.join(temp_dir, uploaded_file.name)
            
            # Gestisci file compressi
            if uploaded_file.name.endswith('.gz'):
                # Decomprimi file gz
                with gzip.open(uploaded_file, 'rt') as gz_file:
                    content = gz_file.read()
                
                # Salva il contenuto decompresso
                decompressed_path = temp_path[:-3]  # Rimuovi .gz
                with open(decompressed_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                return decompressed_path
            else:
                # Salva file normale
                with open(temp_path, 'wb') as f:
                    f.write(uploaded_file.getbuffer())
                
                return temp_path
                
        except Exception as e:
            logger.error(f"Errore nella gestione del file: {str(e)}")
            return None
    
    @staticmethod
    def cleanup_temp_files(file_paths: list):
        """Rimuove file temporanei"""
        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"File temporaneo rimosso: {file_path}")
            except Exception as e:
                logger.warning(f"Impossibile rimuovere il file {file_path}: {str(e)}")