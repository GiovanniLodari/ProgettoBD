"""
Modulo per le analisi statistiche e aggregazioni sui dati
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
    """Classe per analisi specifiche sui disastri naturali"""
    
    def __init__(self, dataframe: SparkDataFrame):
        self.df = dataframe
        self.spark_manager = SparkManager()
    
    def get_disaster_summary(self) -> Dict[str, Any]:
        """Ottieni un riassunto generale dei disastri"""
        try:
            total_disasters = self.df.count()
            
            # Conta per tipo di disastro (cerca colonne comuni)
            disaster_type_col = self._find_column(['disaster_type', 'type', 'disaster', 'event_type'])
            country_col = self._find_column(['country', 'nation', 'state', 'region'])
            year_col = self._find_column(['year', 'date', 'time'])
            
            summary = {
                'total_disasters': total_disasters,
                'total_columns': len(self.df.columns),
                'available_analyses': []
            }
            
            if disaster_type_col:
                type_counts = self.df.groupBy(disaster_type_col).count().collect()
                summary['disaster_types'] = [(row[disaster_type_col], row['count']) for row in type_counts]
                summary['available_analyses'].append('Analisi per Tipo di Disastro')
            
            if country_col:
                summary['available_analyses'].append('Analisi Geografica')
            
            if year_col:
                summary['available_analyses'].append('Analisi Temporale')
            
            return summary
            
        except Exception as e:
            logger.error(f"Errore nel calcolo del summary: {str(e)}")
            return {'error': str(e)}
    
    def analyze_by_disaster_type(self) -> Optional[pd.DataFrame]:
        """Analizza i dati per tipo di disastro"""
        disaster_type_col = self._find_column(['disaster_type', 'type', 'disaster', 'event_type'])
        
        if not disaster_type_col:
            return None
        
        try:
            # Conteggio per tipo
            type_analysis = self.df.groupBy(disaster_type_col).agg(
                count("*").alias("count"),
                countDistinct("*").alias("unique_events")
            ).orderBy(desc("count"))
            
            # Aggiungi percentuale
            total = self.df.count()
            type_analysis = type_analysis.withColumn(
                "percentage", 
                round((col("count") / total * 100), 2)
            )
            
            return type_analysis.toPandas()
            
        except Exception as e:
            logger.error(f"Errore nell'analisi per tipo: {str(e)}")
            return None
    
    def analyze_geographical_distribution(self) -> Optional[pd.DataFrame]:
        """Analizza la distribuzione geografica dei disastri"""
        country_col = self._find_column(['country', 'nation', 'state', 'region'])
        
        if not country_col:
            return None
        
        try:
            geo_analysis = self.df.groupBy(country_col).agg(
                count("*").alias("disaster_count")
            ).orderBy(desc("disaster_count"))
            
            # Top 20 paesi
            top_countries = geo_analysis.limit(20)
            
            return top_countries.toPandas()
            
        except Exception as e:
            logger.error(f"Errore nell'analisi geografica: {str(e)}")
            return None
    
    def analyze_temporal_trends(self) -> Optional[pd.DataFrame]:
        """Analizza i trend temporali dei disastri"""
        date_col = self._find_column(['date', 'year', 'time', 'occurred_date'])
        
        if not date_col:
            return None
        
        try:
            # Prova a estrarre l'anno
            df_with_year = self.df.withColumn("year", year(col(date_col)))
            
            yearly_trend = df_with_year.groupBy("year").agg(
                count("*").alias("disaster_count")
            ).orderBy("year")
            
            return yearly_trend.toPandas()
            
        except Exception as e:
            # Se la colonna non è di tipo data, prova approccio alternativo
            try:
                # Se la colonna contiene già l'anno come intero
                if date_col and self._is_numeric_column(date_col):
                    yearly_trend = self.df.groupBy(date_col).agg(
                        count("*").alias("disaster_count")
                    ).orderBy(date_col)
                    
                    return yearly_trend.toPandas().rename(columns={date_col: 'year'})
            except:
                pass
            
            logger.error(f"Errore nell'analisi temporale: {str(e)}")
            return None
    
    def analyze_severity_impact(self) -> Optional[Dict[str, pd.DataFrame]]:
        """Analizza l'impatto e la severità dei disastri"""
        impact_cols = self._find_columns(['casualties', 'deaths', 'injured', 'affected', 'damage', 'loss'])
        
        if not impact_cols:
            return None
        
        results = {}
        
        try:
            impact_cols = list(col for col in self.df.columns if self._is_numeric_column(col))
            for c in impact_cols:
                if self._is_numeric_column(c):
                    # Statistiche descrittive
                    stats = self.df.select(c).describe().toPandas()
                    stats['metric'] = c
                    results[f"{c}_statistics"] = stats
                    
                    # Top eventi per impatto
                    disaster_type_col = self._find_column(['disaster_type', 'type', 'disaster'])
                    if disaster_type_col:
                        top_impact = self.df.select(disaster_type_col, c) \
                            .filter(col(c).isNotNull()) \
                            .orderBy(desc(c)) \
                            .limit(10)
                        results[f"top_{c}"] = top_impact.toPandas()
            
            return results if results else None
            
        except Exception as e:
            logger.error(f"Errore nell'analisi di impatto: {str(e)}")
            return None
    
    def create_correlation_matrix(self) -> Optional[pd.DataFrame]:
        """Crea una matrice di correlazione per variabili numeriche"""
        numeric_cols = [col for col, dtype in self.df.dtypes 
                       if dtype in ['int', 'bigint', 'double', 'float']]
        
        if len(numeric_cols) < 2:
            return None
        
        try:
            # Converti in Pandas per calcolare la correlazione
            pandas_df = self.df.select(*numeric_cols).toPandas()
            correlation_matrix = pandas_df.corr()
            
            return correlation_matrix
            
        except Exception as e:
            logger.error(f"Errore nel calcolo della correlazione: {str(e)}")
            return None
    
    def _find_column(self, possible_names: List[str]) -> Optional[str]:
        """Trova una colonna che corrisponde a uno dei nomi possibili"""
        df_columns_lower = [col.lower() for col in self.df.columns]
        
        for name in possible_names:
            if name.lower() in df_columns_lower:
                # Trova il nome originale della colonna
                original_name = self.df.columns[df_columns_lower.index(name.lower())]
                return original_name
        
        return None
    
    def _find_columns(self, possible_names: List[str]) -> List[str]:
        """Trova tutte le colonne che contengono uno dei nomi possibili"""
        found_columns = []
        df_columns_lower = [col.lower() for col in self.df.columns]
        
        for col_name in self.df.columns:
            col_lower = col_name.lower()
            for possible_name in possible_names:
                if possible_name.lower() in col_lower:
                    found_columns.append(col_name)
                    break
        
        return found_columns
    
    def _is_numeric_column(self, col_name: str) -> bool:
        """Verifica se una colonna è numerica"""
        try:
            col_type = dict(self.df.dtypes)[col_name]
            return col_type in ['int', 'bigint', 'double', 'float', 'decimal']
        except:
            return False

class GeneralAnalytics:
    """Classe per analisi generali su qualsiasi dataset"""
    
    def __init__(self, dataframe: SparkDataFrame):
        self.df = dataframe
        self.spark_manager = SparkManager()
    
    def perform_aggregation(self, group_by_col: str, agg_col: str, agg_type: str) -> Optional[pd.DataFrame]:
        """
        Esegue un'aggregazione personalizzata
        
        Args:
            group_by_col (str): Colonna per il raggruppamento
            agg_col (str): Colonna da aggregare
            agg_type (str): Tipo di aggregazione
        
        Returns:
            pd.DataFrame: Risultato dell'aggregazione
        """
        try:
            if agg_type == "count":
                result = self.df.groupBy(group_by_col).count().orderBy(desc("count"))
            else:
                agg_func = getattr(functions, agg_type)
                result = self.df.groupBy(group_by_col).agg(
                    agg_func(agg_col).alias(f"{agg_type}_{agg_col}")
                ).orderBy(desc(f"{agg_type}_{agg_col}"))
            
            return result.toPandas()
            
        except Exception as e:
            logger.error(f"Errore nell'aggregazione: {str(e)}")
            return None
    
    def get_column_statistics(self, column_name: str) -> Optional[Dict[str, Any]]:
        """Ottieni statistiche dettagliate per una colonna"""
        try:
            col_type = dict(self.df.dtypes)[column_name]
            
            stats = {
                'column_name': column_name,
                'data_type': col_type,
                'total_count': self.df.count(),
                'null_count': self.df.filter(col(column_name).isNull()).count()
            }
            
            # Aggiungi statistiche specifiche per tipo
            if col_type in ['int', 'bigint', 'double', 'float']:
                # Statistiche numeriche
                numeric_stats = self.df.select(column_name).describe().collect()
                for row in numeric_stats:
                    stats[row['summary']] = float(row[column_name]) if row[column_name] else None
                
                # Percentili
                percentiles = self.df.select(column_name).approxQuantile(column_name, [0.25, 0.5, 0.75], 0.05)
                stats['25th_percentile'] = percentiles[0] if len(percentiles) > 0 else None
                stats['median'] = percentiles[1] if len(percentiles) > 1 else None
                stats['75th_percentile'] = percentiles[2] if len(percentiles) > 2 else None
                
            else:
                # Statistiche categoriche
                unique_count = self.df.select(column_name).distinct().count()
                stats['unique_values'] = unique_count
                
                # Top valori più frequenti
                top_values = self.df.groupBy(column_name).count() \
                    .orderBy(desc("count")).limit(5).collect()
                stats['top_values'] = [(row[column_name], row['count']) for row in top_values]
            
            return stats
            
        except Exception as e:
            logger.error(f"Errore nel calcolo delle statistiche per {column_name}: {str(e)}")
            return None
    
    def detect_outliers(self, column_name: str, method: str = 'iqr') -> Optional[pd.DataFrame]:
        """
        Rileva outlier in una colonna numerica
        
        Args:
            column_name (str): Nome della colonna
            method (str): Metodo di rilevamento ('iqr' o 'zscore')
        
        Returns:
            pd.DataFrame: Righe contenenti outlier
        """
        try:
            col_type = dict(self.df.dtypes)[column_name]
            if col_type not in ['int', 'bigint', 'double', 'float']:
                return None
            
            if method == 'iqr':
                # Metodo IQR (Interquartile Range)
                percentiles = self.df.select(column_name).approxQuantile(column_name, [0.25, 0.75], 0.05)
                if len(percentiles) < 2:
                    return None
                
                q1, q3 = percentiles[0], percentiles[1]
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                outliers = self.df.filter(
                    (col(column_name) < lower_bound) | (col(column_name) > upper_bound)
                )
                
            else:  # zscore method
                # Calcola media e deviazione standard
                stats = self.df.select(column_name).describe().collect()
                mean_val = float([row[column_name] for row in stats if row['summary'] == 'mean'][0])
                std_val = float([row[column_name] for row in stats if row['summary'] == 'stddev'][0])
                
                # Z-score > 3 o < -3 sono considerati outlier
                outliers = self.df.withColumn('zscore', abs((col(column_name) - mean_val) / std_val)) \
                    .filter(col('zscore') > 3) \
                    .drop('zscore')
            
            return outliers.toPandas()
            
        except Exception as e:
            logger.error(f"Errore nella rilevazione di outlier: {str(e)}")
            return None
    
    def generate_data_quality_report(self) -> Dict[str, Any]:
        """Genera un report completo sulla qualità dei dati"""
        try:
            total_rows = self.df.count()
            total_cols = len(self.df.columns)
            
            quality_report = {
                'overview': {
                    'total_rows': total_rows,
                    'total_columns': total_cols,
                    'dataset_size_mb': 0  # Calcolato approssimativamente
                },
                'completeness': {},
                'consistency': {},
                'validity': {}
            }
            
            # Analisi di completezza (valori null)
            for col_name in self.df.columns:
                null_count = self.df.filter(col(col_name).isNull()).count()
                null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
                
                quality_report['completeness'][col_name] = {
                    'null_count': null_count,
                    'null_percentage': round(null_percentage, 2),
                    'complete_percentage': round(100 - null_percentage, 2)
                }
            
            # Analisi di consistenza (duplicati)
            duplicate_count = self.df.count() - self.df.dropDuplicates().count()
            quality_report['consistency']['duplicates'] = {
                'duplicate_rows': duplicate_count,
                'duplicate_percentage': round((duplicate_count / total_rows) * 100, 2) if total_rows > 0 else 0
            }
            
            # Calcola punteggio qualità generale
            avg_completeness = sum(col_info['complete_percentage'] 
                                 for col_info in quality_report['completeness'].values()) / total_cols
            duplicate_penalty = quality_report['consistency']['duplicates']['duplicate_percentage']
            
            quality_score = max([0, avg_completeness - duplicate_penalty])
            quality_report['overall_quality_score'] = round(quality_score, 1)
            
            return quality_report
            
        except Exception as e:
            logger.error(f"Errore nella generazione del report di qualità: {str(e)}")
            return {'error': str(e)}

class QueryEngine:
    """Motore per l'esecuzione di query personalizzate"""
    
    def __init__(self):
        self.spark_manager = SparkManager()
        self.query_history = []
    
    def execute_custom_query(self, query: str, limit_results: bool = True) -> Optional[pd.DataFrame]:
        """
        Esegue una query SQL personalizzata
        
        Args:
            query (str): Query SQL da eseguire
            limit_results (bool): Se limitare i risultati per performance
        
        Returns:
            pd.DataFrame: Risultati della query
        """
        try:
            # Aggiungi LIMIT se non presente e richiesto
            if limit_results and 'LIMIT' not in query.upper():
                query = f"{query} LIMIT {Config.DEFAULT_QUERY_LIMIT}"
            
            # Esegui la query SQL usando la SparkSession
            result = self.spark_manager.spark.sql(query)
            pandas_result = result.toPandas()
            
            # Aggiungi alla cronologia
            self.query_history.append({
                'query': query,
                'row_count': len(pandas_result),
                'execution_time': None  # Potresti aggiungere timing se necessario
            })
            
            return pandas_result
            
        except Exception as e:
            logger.error(f"Errore nell'esecuzione della query: {str(e)}")
            return None
    
    def validate_query(self, query: str) -> Dict[str, Any]:
        """
        Valida una query SQL prima dell'esecuzione
        
        Args:
            query (str): Query da validare
        
        Returns:
            dict: Risultato della validazione
        """
        try:
            # Controlli di base
            query_upper = query.upper().strip()
            
            # Verifica che non contenga operazioni pericolose
            dangerous_operations = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE']
            for operation in dangerous_operations:
                if operation in query_upper:
                    return {
                        'valid': False,
                        'error': f"Operazione non consentita: {operation}",
                        'suggestion': "Usa solo query SELECT per l'analisi dei dati"
                    }
            
            # Verifica sintassi basic
            if not query_upper.startswith('SELECT'):
                return {
                    'valid': False,
                    'error': "La query deve iniziare con SELECT",
                    'suggestion': "Esempio: SELECT * FROM disasters WHERE ..."
                }
            
            # Verifica che contenga FROM
            if 'FROM' not in query_upper:
                return {
                    'valid': False,
                    'error': "La query deve contenere la clausola FROM",
                    'suggestion': f"Usa FROM {DatabaseConfig.TEMP_VIEW_NAME} per referenziare il dataset"
                }
            
            return {'valid': True, 'message': 'Query valida'}
            
        except Exception as e:
            return {'valid': False, 'error': f'Errore nella validazione: {str(e)}'}
    
    def get_query_suggestions(self, dataset_columns: List[str]) -> List[str]:
        """Genera suggerimenti di query basati sulle colonne del dataset"""
        suggestions = []
        
        # Query di base
        suggestions.append(f"SELECT * FROM {DatabaseConfig.TEMP_VIEW_NAME} LIMIT 10")
        
        # Se ci sono colonne specifiche, genera suggerimenti mirati
        for col in dataset_columns:
            col_lower = col.lower()
            
            if any(keyword in col_lower for keyword in ['type', 'category', 'kind']):
                suggestions.append(f"SELECT {col}, COUNT(*) as count FROM {DatabaseConfig.TEMP_VIEW_NAME} GROUP BY {col} ORDER BY count DESC")
            
            if any(keyword in col_lower for keyword in ['date', 'year', 'time']):
                suggestions.append(f"SELECT {col}, COUNT(*) as disasters FROM {DatabaseConfig.TEMP_VIEW_NAME} GROUP BY {col} ORDER BY {col}")
            
            if any(keyword in col_lower for keyword in ['country', 'region', 'location']):
                suggestions.append(f"SELECT {col}, COUNT(*) as count FROM {DatabaseConfig.TEMP_VIEW_NAME} GROUP BY {col} ORDER BY count DESC LIMIT 10")
        
        return suggestions
    
    def get_query_history(self) -> List[Dict]:
        """Ottieni la cronologia delle query eseguite"""
        return self.query_history.copy()