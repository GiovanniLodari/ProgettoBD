"""
Pagina Streamlit per l'analisi visiva automatica e intelligente dei dati.
Include supporto completo per grafici e algoritmi Spark MLlib predefiniti dai metadati JSON.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, Tuple, Optional, List
import warnings
import re

# Import specifici per Spark MLlib
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder, FeatureHasher
from pyspark.ml.clustering import KMeans, BisectingKMeans
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.regression import LinearRegression as SparkLinearRegression
from pyspark.ml.evaluation import ClusteringEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql.functions import col, when, isnan, isnull
from pyspark.sql.types import DoubleType

# Algoritmi di ML non supportati da Spark
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler as SklearnStandardScaler

# Import per utils
from utils.utils import get_twitter_query_templates

# Gestione dipendenza opzionale (SciPy)
try:
    from scipy.signal import find_peaks
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

warnings.filterwarnings('ignore')

### 1. CLASSE DI ANALISI PRINCIPALE
# ==============================================================================

class GeneralAnalytics:
    """
    Classe helper per eseguire aggregazioni sui dati utilizzando Spark SQL.
    """
    def __init__(self, dataframe: pd.DataFrame, spark_session: SparkSession = None):
        self.df = dataframe
        self.spark = spark_session or SparkSession.getActiveSession()
        if self.spark is None:
            raise ValueError("Spark session non trovata. Assicurati che Spark sia inizializzato.")

    def perform_aggregation(self, group_by_col: str, agg_col: str, agg_type: str, sort_desc: bool = True) -> Optional[pd.DataFrame]:
        """
        Esegue un'aggregazione utilizzando Spark SQL per migliori performance.
        """
        try:
            # Converti pandas DataFrame in Spark DataFrame
            spark_df = self.spark.createDataFrame(self.df)
            spark_df.createOrReplaceTempView("temp_table")
            
            # Gestisce il caso speciale del conteggio
            if agg_col == "count" or agg_type == "count":
                query = f"""
                SELECT {group_by_col}, COUNT(*) as count
                FROM temp_table
                GROUP BY {group_by_col}
                ORDER BY count {'DESC' if sort_desc else 'ASC'}
                """
            else:
                # Gestisce le aggregazioni numeriche standard
                query = f"""
                SELECT {group_by_col}, {agg_type}({agg_col}) as {agg_type}_{agg_col}
                FROM temp_table
                WHERE {agg_col} IS NOT NULL
                GROUP BY {group_by_col}
                ORDER BY {agg_type}_{agg_col} {'DESC' if sort_desc else 'ASC'}
                """
            
            result_spark_df = self.spark.sql(query)
            result = result_spark_df.toPandas()
            
            return result
        except Exception as e:
            st.error(f"Errore durante l'aggregazione Spark: {e}")
            return None

### 2. CLASSE PER LA VISUALIZZAZIONE DEI GRAFICI (invariata)
# ==============================================================================

class ChartVisualizer:
    """
    Classe per gestire la visualizzazione automatica dei grafici basata sui metadati JSON.
    """
    def __init__(self):
        self.CHART_COLOR = '#89CFF0'
        self.color_palettes = {
            'default': px.colors.qualitative.Set3,
            'viridis': px.colors.sequential.Viridis,
            'plasma': px.colors.sequential.Plasma,
        }

    def validate_chart_config(self, chart_config: Dict[str, Any], df_columns: List[str]) -> Tuple[bool, str]:
        """Valida la configurazione del grafico."""
        if not isinstance(chart_config, dict):
            return False, "Configurazione non valida"
        
        chart_type = chart_config.get('type', '').lower()
        x_col = chart_config.get('x')
        y_col = chart_config.get('y')
        
        if chart_type in ['barre', 'linee', 'heatmap', 'serie temporale con picchi']:
            if not x_col or x_col not in df_columns:
                return False, f"Colonna X '{x_col}' non trovata nel dataset"
            if chart_type in ['linee', 'heatmap', 'serie temporale con picchi'] and (not y_col or y_col not in df_columns):
                return False, f"Colonna Y '{y_col}' richiesta per {chart_type}"
        
        elif chart_type == 'torta':
            if not x_col or x_col not in df_columns:
                return False, f"Colonna etichetta '{x_col}' non trovata"
            if y_col:
                if not y_col in df_columns:
                    return False, f"Colonna etichetta '{y_col}' non trovata"
        
        return True, ""
    
    def _detect_peaks(self, df: pd.DataFrame, x_col: str, y_col: str, prominence_factor: float = 0.1) -> Tuple[List[int], float]:
        """Rileva picchi in una serie temporale."""
        if not SCIPY_AVAILABLE:
            st.warning("Libreria 'scipy' non trovata. Rilevamento picchi limitato.")
            df_sorted = df.sort_values(x_col).dropna(subset=[x_col, y_col])
            y_values = df_sorted[y_col].values
            
            peaks = []
            threshold = np.mean(y_values) + np.std(y_values)
            
            for i in range(1, len(y_values) - 1):
                if (y_values[i] > y_values[i-1] and 
                    y_values[i] > y_values[i+1] and 
                    y_values[i] > threshold):
                    peaks.append(i)
            return peaks, threshold

        try:
            df_sorted = df.sort_values(x_col).dropna(subset=[x_col, y_col])
            y_values = df_sorted[y_col].values
            
            prominence_threshold = prominence_factor * np.std(y_values)
            
            peaks, properties = find_peaks(y_values, prominence=prominence_threshold)
            
            return peaks.tolist(), prominence_threshold
            
        except Exception as e:
            st.error(f"Errore nel rilevamento picchi: {e}")
            return [], 0.0

    def create_chart(self, df: pd.DataFrame, chart_config: Dict[str, Any]) -> Optional[go.Figure]:
        """Crea un grafico basato sulla configurazione con gestione centralizzata dei dati."""
        if df.empty:
            return None
        
        is_valid, error_msg = self.validate_chart_config(chart_config, df.columns.tolist())
        if not is_valid:
            st.error(f"Errore configurazione grafico: {error_msg}")
            return None
        
        chart_type = chart_config.get('type', '').lower()
        x_col = chart_config.get('x')
        y_col = chart_config.get('y')
        z_col = chart_config.get('z')
        agg_func = chart_config.get('agg', 'max')
        
        try:
            df_final = df
            x_col_final = x_col
            y_col_final = y_col
            
            # Le heatmap usano i dati originali, quindi bypassiamo la preparazione
            if chart_type != 'heatmap':
                prepared_data = self._prepare_chart_data(df, x_col, y_col, chart_type)
                
                if prepared_data is None or prepared_data['df'].empty:
                    st.warning("Nessun dato valido dopo la preparazione.")
                    return None
                
                # Usiamo i dati preparati per tutti gli altri grafici
                df_final = prepared_data['df']
                x_col_final = prepared_data['x_col']
                y_col_final = prepared_data['y_col']
            
            fig = self._create_chart_by_type(df_final, chart_type, x_col_final, y_col_final, x_col, z_col, agg_func)
            
            if fig:
                fig.update_layout(height=500, template='plotly_white', 
                                margin=dict(t=60, b=60, l=60, r=60))
            
            return fig
            
        except Exception as e:
            st.error(f"Errore nella creazione del grafico {chart_type}: {e}")
            return None

    def _prepare_chart_data(self, df: pd.DataFrame, x_col: str, y_col: str, chart_type: str) -> Optional[Dict]:
        """Prepara i dati in modo centralizzato per tutti i tipi di grafico."""
        df_clean = df.copy()
        df_clean = df_clean.dropna(subset=[x_col])
        
        if df_clean.empty:
            return None
        
        is_temporal = pd.api.types.is_datetime64_any_dtype(df_clean[x_col])
        
        if is_temporal and chart_type != 'serie temporale con picchi':
            st.info("Rilevata serie temporale, dati aggregati per giorno.")
            df_clean[x_col] = pd.to_datetime(df_clean[x_col], errors='coerce').dt.date
            df_clean = df_clean.dropna(subset=[x_col])

            if not y_col:
                agg_df = df_clean.groupby(x_col).size().reset_index(name='Conteggio')
                y_col_final = 'Conteggio'
            elif pd.api.types.is_numeric_dtype(df_clean[y_col]):
                agg_df = df_clean.groupby(x_col)[y_col].sum().reset_index()
                y_col_final = y_col
            else:
                agg_df = df_clean.groupby(x_col).size().reset_index(name=f'Conteggio_{y_col}')
                y_col_final = f'Conteggio_{y_col}'
            
            df_final = agg_df.sort_values(x_col)
            x_col_final = x_col
        else:
            if not y_col:
                counts = df_clean[x_col].value_counts()
                df_final = pd.DataFrame({x_col: counts.index, 'Conteggio': counts.values})
                x_col_final, y_col_final = x_col, 'Conteggio'
            else:
                df_clean = df_clean.dropna(subset=[y_col])
                if df_clean[x_col].dtype == 'object' and pd.api.types.is_numeric_dtype(df_clean[y_col]):
                    df_final = df_clean.groupby(x_col)[y_col].sum().reset_index()
                else:
                    df_final = df_clean[[x_col, y_col]]
                x_col_final, y_col_final = x_col, y_col
        
        return {'df': df_final, 'x_col': x_col_final, 'y_col': y_col_final}

    def _create_chart_by_type(self, df: pd.DataFrame, chart_type: str, x_col: str, y_col: str, original_x_col: str, z_col: str, agg_func: str) -> Optional[go.Figure]:
        """Crea il grafico specifico usando dati già preparati e unificati."""
        fig = None
        
        n_unique_x = df[x_col].nunique()
        needs_scrolling = n_unique_x > 20

        MAX_DATAPOINTS_FOR_CHART = 500
        
        if chart_type == 'barre':
            if len(df) > MAX_DATAPOINTS_FOR_CHART:
                df_for_chart = df.sort_values(by=y_col, ascending=False)
                df = df_for_chart.head(MAX_DATAPOINTS_FOR_CHART)
            fig = px.bar(df, x=x_col, y=y_col, title=f"Grafico a Barre - {y_col} per {original_x_col}")
            fig.update_traces(width=0.6)
            fig.update_xaxes(title=original_x_col, type='category')
            
            if needs_scrolling:
                fig.update_layout(
                    xaxis=dict(
                        rangeslider=dict(visible=True),
                        type="category",
                        title=original_x_col
                    ),
                    height=600,
                    title=f"Grafico a Barre - {y_col} per {original_x_col}"
                )
                if n_unique_x > 50:
                    fig.update_xaxes(range=[-0.5, 19.5])
        
        elif chart_type == 'linee':
            if len(df) > MAX_DATAPOINTS_FOR_CHART:
                df_for_chart = df.sort_values(by=x_col, ascending=False)
                df = df_for_chart.head(MAX_DATAPOINTS_FOR_CHART)
            fig = px.line(df, x=x_col, y=y_col, markers=True, title=f"Grafico a Linee - {y_col} rispetto a {original_x_col}")
            fig.update_xaxes(title=original_x_col, type='category')
            
            if needs_scrolling:
                fig.update_layout(
                    xaxis=dict(
                        rangeslider=dict(visible=True),
                        type="category",
                        title=original_x_col
                    ),
                    height=600,
                    title=f"Grafico a Linee - {y_col} rispetto a {original_x_col}"
                )
                if n_unique_x > 50:
                    fig.update_xaxes(range=[-0.5, 19.5])
        
        elif chart_type == 'serie temporale con picchi':
            st.warning("Serie temporale con picchi richiede dati originali - implementazione speciale necessaria.")
            return None
        
        elif chart_type == 'torta':
            if y_col is None or y_col == '':
                value_counts = df[x_col].value_counts()
                df_pie = pd.DataFrame({
                    'categories': value_counts.index,
                    'counts': value_counts.values
                })
                fig = px.pie(df_pie, values='counts', names='categories', 
                        title=f"Distribuzione per {original_x_col} (Conteggio)")
            else:
                df_agg = df.groupby(x_col)[y_col].sum().reset_index()
                df_pie = df_agg.nlargest(10, y_col) if len(df_agg) > 10 else df_agg
                fig = px.pie(df_pie, values=y_col, names=x_col, 
                        title=f"Distribuzione - {y_col} per {original_x_col}")
        
        elif chart_type == 'heatmap':
            if z_col:
                try:
                    pivot_table = pd.pivot_table(
                        df, 
                        values=z_col, 
                        index=y_col, 
                        columns=x_col, 
                        aggfunc=agg_func
                    )
                    
                    fig = px.imshow(
                        pivot_table,
                        title=f"Heatmap di {z_col} tra {y_col} e {x_col}",
                        labels=dict(x=x_col, y=y_col, color=f"{agg_func} di {z_col}"),
                        color_continuous_scale='Plasma'
                    )
                except Exception as e:
                    st.error(f"Impossibile creare la pivot table: {e}")
                    fig = go.Figure() # Restituisce un grafico vuoto in caso di errore

            else:
                if len(df[x_col].unique()) <= 50 and len(df[y_col].unique()) <= 50:
                    crosstab = pd.crosstab(df[y_col], df[x_col])
                    fig = px.imshow(
                        crosstab, 
                        title=f"Heatmap: {y_col} vs {x_col}",
                        color_continuous_scale='Plasma'
                        )
                else:
                    fig = px.density_heatmap(
                        df,
                        x=x_col,
                        y=y_col,
                        title=f"Density Heatmap: {y_col} vs {x_col}",
                        nbinsx=50,
                        nbinsy=50,
                        color_continuous_scale='Plasma'
                    )

        if fig:
            fig.update_layout(
                dragmode='pan',
                showlegend=True
            )

        return fig
    
    def display_charts_from_config(self, df: pd.DataFrame, charts_config: List[Dict[str, Any]]):
        """Visualizza tutti i grafici dalla configurazione JSON."""
        if not charts_config:
            st.info("Nessun grafico configurato per questa query")
            return
        
        if len(charts_config) == 1:
            fig = self.create_chart(df, charts_config[0])
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        else:
            tab_names = [f"📊 {cfg.get('type', f'Grafico {i+1}').title()}" for i, cfg in enumerate(charts_config)]
            tabs = st.tabs(tab_names)
            for tab, chart_config in zip(tabs, charts_config):
                with tab:
                    fig = self.create_chart(df, chart_config)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)

### 3. CLASSE PER SPARK MLLIB
# ==============================================================================

# Aggiornamento per SparkMLProcessor per supportare algoritmi non-Spark

class SparkMLProcessor:
    """
    Classe per gestire l'esecuzione di algoritmi sia Spark MLlib che scikit-learn.
    """
    def __init__(self, dataframe: pd.DataFrame, spark_session: SparkSession = None):
        self.df = dataframe.copy()
        
        if spark_session is not None:
            self.spark = spark_session
        else:
            try:
                self.spark = st.session_state.spark_manager.get_spark_session()
                if self.spark is None:
                    raise ValueError("Nessuna sessione Spark attiva trovata")
            except Exception:
                raise ValueError("Impossibile ottenere una sessione Spark valida")        
        
        self.spark_df = None
        self._connection_retries = 3
        self.label_encoders = {}
        self.scaler = SklearnStandardScaler()
        
    def get_algorithm_requirements(self) -> Dict[str, Dict[str, Any]]:
        """Restituisce i requisiti per ogni algoritmo (sia Spark che scikit-learn)."""
        return {
            # Spark MLlib - Clustering
            'K-Means': {
                'type': 'clustering', 
                'requires_target': False, 
                'min_features': 1, 
                'engine': 'spark',
                'description': 'Clustering partizionale che raggruppa i dati in k cluster'
            },
            'Bisecting K-Means': {
                'type': 'clustering', 
                'requires_target': False, 
                'min_features': 1, 
                'engine': 'spark',
                'description': 'Variante di K-Means che usa divisione gerarchica'
            },
            
            # Spark MLlib - Classification
            'Random Forest Classifier': {
                'type': 'classification', 
                'requires_target': True, 
                'min_features': 1, 
                'engine': 'spark',
                'description': 'Ensemble di alberi decisionali per classificazione'
            },
            'GBT Classifier': {
                'type': 'classification', 
                'requires_target': True, 
                'min_features': 1, 
                'engine': 'spark',
                'description': 'Gradient Boosted Trees per classificazione'
            },
            
            # Spark MLlib - Regression
            'Linear Regression': {
                'type': 'regression', 
                'requires_target': True, 
                'min_features': 1, 
                'engine': 'spark',
                'description': 'Regressione lineare con regolarizzazione'
            },
            
            # Scikit-learn - Clustering e Anomaly Detection
            'DBSCAN': {
                'type': 'clustering', 
                'requires_target': False, 
                'min_features': 1, 
                'engine': 'sklearn',
                'description': 'Clustering basato su densità che identifica cluster di forma arbitraria'
            },
            'Isolation Forest': {
                'type': 'anomaly', 
                'requires_target': False, 
                'min_features': 1, 
                'engine': 'sklearn',
                'description': 'Rilevamento anomalie tramite isolamento in foreste di alberi'
            }
        }
    
    def _ensure_spark_session_active(self):
        """Verifica e ripristina la sessione Spark se necessario."""
        for attempt in range(self._connection_retries):
            try:
                if self.spark is None or self.spark.sparkContext._jsc is None:
                    raise RuntimeError("Sessione Spark non attiva")
                
                # Test rapido della connessione
                self.spark.sparkContext.getConf().get("spark.app.name")
                return True
                
            except Exception as e:
                st.warning(f"Tentativo {attempt + 1}: Sessione Spark non disponibile - {e}")
                
                if attempt < self._connection_retries - 1:
                    try:
                        # Tenta di ricreare la sessione
                        self.spark = st.session_state.spark_manager.get_spark_session()
                        if self.spark is None:
                            self.spark = st.session_state.spark_manager.restart_spark()
                    except Exception:
                        continue
                else:
                    raise RuntimeError(f"Impossibile ripristinare la sessione Spark dopo {self._connection_retries} tentativi")

    def _prepare_spark_data_robust(self, features: List[str], target: Optional[str] = None) -> Tuple[SparkDataFrame, List[str]]:
        """Versione ultra-robusta di preparazione dati Spark con FeatureHasher."""
        try:
            self._ensure_spark_session_active()
            
            MAX_ROWS_FOR_ML = 5000
            if len(self.df) > MAX_ROWS_FOR_ML:
                sample_df = self.df.sample(n=MAX_ROWS_FOR_ML, random_state=42)
            else:
                sample_df = self.df
            
            if self.spark_df is None:
                self.spark_df = self.spark.createDataFrame(sample_df)
                self.spark_df.cache()

            spark_columns = self.spark_df.columns
            missing_features = [f for f in features if f not in spark_columns]
            if missing_features:
                raise ValueError(f"Colonne mancanti: {missing_features}")
            
            cols_needed = features + ([target] if target else [])
            df_clean = self.spark_df.select(*cols_needed).na.drop("any")
            
            row_count = df_clean.count()
            if row_count == 0:
                raise ValueError("Nessuna riga valida dopo pulizia")
            elif row_count < 50:
                raise ValueError(f"Troppo poche righe ({row_count}) per ML affidabile")
            
            stages = []
            
            string_cols = [f for f in features if dict(df_clean.dtypes).get(f) == 'string']
            other_cols = [f for f in features if f not in string_cols]

            cleaned_other_cols = []
            for feature in other_cols:
                col_type = dict(df_clean.dtypes).get(feature)
                output_col = f"{feature}_cleaned"
                
                if col_type == 'boolean':
                    df_clean = df_clean.withColumn(output_col, col(feature).cast(DoubleType()))
                else: # si assume che sia numerico
                    df_clean = df_clean.withColumn(output_col, 
                        when(col(feature).isNull() | isnan(col(feature)) | 
                             (col(feature) == float('inf')) | (col(feature) == float('-inf')), 
                             0.0).otherwise(col(feature).cast(DoubleType()))
                    )
                cleaned_other_cols.append(output_col)
            
            final_assembler_cols = cleaned_other_cols
            if string_cols:
                hasher = FeatureHasher(
                    inputCols=string_cols,
                    outputCol="hashed_features",
                    numFeatures=256  # Un buon punto di partenza, puoi aumentarlo se necessario (es. 512, 1024)
                )
                stages.append(hasher)
                final_assembler_cols.append("hashed_features")

            if not final_assembler_cols:
                raise ValueError("Nessuna feature valida dopo il preprocessing")

            # 4. Assembla tutte le feature processate in un unico vettore
            assembler = VectorAssembler(
                inputCols=final_assembler_cols,
                outputCol="features",
                handleInvalid="keep"  # 'keep' è più robusto di 'skip' o 'error'
            )
            stages.append(assembler)

            # 5. Crea ed esegui la pipeline
            pipeline = Pipeline(stages=stages)
            pipeline_model = pipeline.fit(df_clean)
            df_final = pipeline_model.transform(df_clean)

            final_count = df_final.count()
            if final_count == 0:
                raise ValueError("Nessuna riga dopo l'assemblaggio delle feature")

            return df_final, final_assembler_cols
            
        except Exception as e:
            if hasattr(self, 'spark_df') and self.spark_df is not None:
                try:
                    self.spark_df.unpersist()
                    self.spark_df = None
                except:
                    pass
            raise ValueError(f"Errore preparazione dati Spark: {e}")
    
    def _prepare_spark_data(self, features: List[str], target: Optional[str] = None) -> Tuple[SparkDataFrame, List[str]]:
        """Prepara i dati Spark con encoding e pulizia - versione sicura."""
        try:
            # Verifica sessione prima di iniziare
            self._ensure_spark_session_active()
            
            # Crea Spark DataFrame con gestione errori
            if self.spark_df is None:
                # Converti con chunks più piccoli per evitare problemi di memoria
                chunk_size = min(10000, len(self.df))
                if len(self.df) > chunk_size:
                    st.warning(f"Dataset grande ({len(self.df)} righe), usando campione di {chunk_size}")
                    sample_df = self.df.sample(n=chunk_size, random_state=42)
                    self.spark_df = self.spark.createDataFrame(sample_df)
                else:
                    self.spark_df = self.spark.createDataFrame(self.df)
            
            # Verifica che le colonne esistano
            spark_columns = self.spark_df.columns
            missing_features = [f for f in features if f not in spark_columns]
            if missing_features:
                raise ValueError(f"Colonne mancanti: {missing_features}")
            
            # Seleziona colonne necessarie e rimuovi valori nulli
            cols_needed = features + ([target] if target else [])
            df_clean = self.spark_df.select(*cols_needed).na.drop()
            
            # Controlla che ci siano dati dopo la pulizia
            row_count = df_clean.count()
            if row_count == 0:
                raise ValueError("Nessuna riga valida dopo pulizia")
            elif row_count < 10:
                raise ValueError(f"Troppo poche righe ({row_count}) per ML")
            
            # Encoding delle colonne categoriche
            stages = []
            feature_cols = []
            
            for feature in features:
                try:
                    col_type = dict(self.spark_df.dtypes)[feature]
                    
                    if col_type == 'string':
                        indexer = StringIndexer(
                            inputCol=feature, 
                            outputCol=f"{feature}_indexed",
                            handleInvalid="keep"
                        )
                        stages.append(indexer)
                        feature_cols.append(f"{feature}_indexed")
                    else:
                        df_clean = df_clean.withColumn(feature, col(feature).cast(DoubleType()))
                        feature_cols.append(feature)
                except Exception as e:
                    st.warning(f"Errore feature '{feature}': {e}")
                    continue
            
            if not feature_cols:
                raise ValueError("Nessuna feature valida dopo preprocessing")
            
            # Applica le trasformazioni
            if stages:
                pipeline = Pipeline(stages=stages)
                pipeline_model = pipeline.fit(df_clean)
                df_clean = pipeline_model.transform(df_clean)
            
            # Assembla le features
            assembler = VectorAssembler(
                inputCols=feature_cols,
                outputCol="features",
                handleInvalid="keep"
            )
            
            df_final = assembler.transform(df_clean)
            
            # Verifica finale
            final_count = df_final.count()
            if final_count == 0:
                raise ValueError("Nessuna riga dopo assemblaggio features")
            
            return df_final, feature_cols
            
        except Exception as e:
            # Cleanup in caso di errore
            if hasattr(self, 'spark_df') and self.spark_df is not None:
                try:
                    self.spark_df.unpersist()
                except:
                    pass
            raise ValueError(f"Errore preparazione dati Spark: {e}")

    def run_clustering_optimized(self, algorithm: str, features: List[str]) -> Dict[str, Any]:
        """Esegue algoritmi di clustering Spark MLlib con ottimizzazione degli iperparametri."""
        try:
            df_prepared, feature_cols = self._prepare_spark_data(features)
            
            if algorithm == "K-Means":
                return self._run_kmeans_optimized(df_prepared, features)
            elif algorithm == "Bisecting K-Means":
                return self._run_bisecting_kmeans_optimized(df_prepared, features)
            else:
                return {'error': f"Algoritmo di clustering '{algorithm}' non supportato", 'success': False}
                
        except Exception as e:
            return {'error': f"Errore in {algorithm}: {e}", 'success': False}

    def _run_kmeans_optimized(self, df_prepared: SparkDataFrame, original_features: List[str]) -> Dict[str, Any]:
        """Versione sicura di K-Means con gestione robusta degli errori."""
        try:
            self._ensure_spark_session_active()
            
            # Persisti il DataFrame per performance
            df_prepared.cache()
            
            # Verifica dati sufficienti
            total_rows = df_prepared.count()
            if total_rows < 4:
                df_prepared.unpersist()
                return {'error': f'Troppo poche righe ({total_rows}) per K-Means', 'success': False}
            
            # Limita i valori di k
            max_k = min(10, total_rows // 2)
            k_values = list(range(2, max_k + 1))
            
            if not k_values:
                df_prepared.unpersist()
                return {'error': 'Impossibile determinare valori k validi', 'success': False}
            
            best_k = 2
            best_score = -1
            best_model = None
            successful_runs = 0
            
            progress_bar = st.progress(0)
            
            for i, k in enumerate(k_values):
                progress = (i + 1) / len(k_values)
                progress_bar.progress(progress)
                
                try:
                    kmeans = KMeans(
                        k=k, 
                        featuresCol="features", 
                        predictionCol="prediction", 
                        seed=42,
                        maxIter=20,  # Limita iterazioni
                        tol=1e-4
                    )
                    
                    model = kmeans.fit(df_prepared)
                    predictions = model.transform(df_prepared)
                    
                    # Usa campione per valutazione se dataset grande
                    if total_rows > 1000:
                        eval_sample = predictions.sample(fraction=0.1, seed=42)
                    else:
                        eval_sample = predictions
                    
                    evaluator = ClusteringEvaluator(
                        predictionCol="prediction", 
                        featuresCol="features",
                        metricName="silhouette"
                    )
                    score = evaluator.evaluate(eval_sample)
                    
                    if score > best_score:
                        best_score = score
                        best_k = k
                        best_model = model
                    
                    successful_runs += 1
                    
                except Exception as e:
                    st.warning(f"Errore con k={k}: {str(e)[:100]}...")
                    continue
            
            progress_bar.empty()
            
            if best_model is None or successful_runs == 0:
                df_prepared.unpersist()
                return {'error': 'K-Means fallito su tutti i k testati', 'success': False}
            
            # Risultati finali - collect() sicuro
            try:
                final_predictions = best_model.transform(df_prepared)
                
                # Limita righe per collect()
                if total_rows > 5000:
                    sample_predictions = final_predictions.sample(fraction=5000/total_rows, seed=42)
                    labels_data = sample_predictions.select("prediction").collect()
                    st.warning(f"Visualizzazione su campione di {len(labels_data)} righe")
                else:
                    labels_data = final_predictions.select("prediction").collect()
                
                labels = [row.prediction for row in labels_data]
                
            except Exception as e:
                df_prepared.unpersist()
                return {'error': f'Errore raccolta risultati: {e}', 'success': False}
            
            # Cleanup
            df_prepared.unpersist()
                        
            return {
                'algorithm': 'K-Means',
                'labels': labels,
                'n_clusters': best_k,
                'silhouette_score': best_score,
                'best_params': {'k': best_k},
                'success': True
            }
            
        except Exception as e:
            # Cleanup in caso di errore
            try:
                df_prepared.unpersist()
            except:
                pass
            return {'error': f'Errore critico K-Means: {e}', 'success': False}

    def run_supervised_optimized(self, algorithm: str, features: List[str], target: str) -> Dict[str, Any]:
        """Versione robusta per algoritmi supervisionati."""
        try:
            df_prepared, feature_cols = self._prepare_spark_data_robust(features, target)
            
            target_unique = self.df[target].nunique()
            target_is_numeric = pd.api.types.is_numeric_dtype(self.df[target])
            
            is_classification = (not target_is_numeric) or (target_unique <= 10 and target_unique >= 2)
            
            if is_classification:
                if target_is_numeric:
                    df_prepared = df_prepared.withColumn("label", col(target).cast("string"))
                
                target_indexer = StringIndexer(
                    inputCol="label" if target_is_numeric else target, 
                    outputCol="label",
                    handleInvalid="keep"
                )
                df_prepared = target_indexer.fit(df_prepared).transform(df_prepared)
            else:
                df_prepared = df_prepared.withColumn("label", col(target).cast(DoubleType()))
            
            total_rows = df_prepared.count()
            if total_rows < 100:
                train_ratio = 0.7
            else:
                train_ratio = 0.8
                
            train_df, test_df = df_prepared.randomSplit([train_ratio, 1-train_ratio], seed=42)
            
            train_count = train_df.count()
            test_count = test_df.count()
            
            if train_count < 10 or test_count < 5:
                raise ValueError(f"Split inadeguato: train={train_count}, test={test_count}")
            
            if is_classification and algorithm in ['Random Forest Classifier', 'GBT Classifier']:
                return self._run_classification_optimized(algorithm, train_df, test_df)
            elif not is_classification and algorithm == 'Linear Regression':
                return self._run_regression_optimized(algorithm, train_df, test_df)
            else:
                if is_classification:
                    suggested = algorithm.replace('Regressor', 'Classifier')
                else:
                    suggested = algorithm.replace('Classifier', 'Regressor')
                
                return {
                    'error': f"Mismatch algoritmo-problema. Per questo target, usa '{suggested}' invece di '{algorithm}'",
                    'success': False
                }
                
        except Exception as e:
            return {'error': f"Errore in {algorithm}: {e}", 'success': False}
        finally:
            if hasattr(self, 'spark_df') and self.spark_df is not None:
                try:
                    self.spark_df.unpersist()
                    self.spark_df = None
                except:
                    pass

    def _run_classification_optimized(self, algorithm: str, train_df: SparkDataFrame, test_df: SparkDataFrame) -> Dict[str, Any]:
        """Esegue algoritmi di classificazione con ottimizzazione."""
        
        if algorithm == "Random Forest Classifier":
            model_class = RandomForestClassifier
            param_grid = ParamGridBuilder() \
                .addGrid(model_class.numTrees, [10, 20, 50]) \
                .addGrid(model_class.maxDepth, [5, 10, 15]) \
                .build()
        elif algorithm == "GBT Classifier":
            model_class = GBTClassifier
            param_grid = ParamGridBuilder() \
                .addGrid(model_class.maxIter, [10, 20]) \
                .addGrid(model_class.maxDepth, [5, 10]) \
                .build()
            
        base_model = model_class(featuresCol="features", labelCol="label")
        
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label", 
            predictionCol="prediction", 
            metricName="accuracy"
        )
        
        cv = CrossValidator(
            estimator=base_model,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )
        
        with st.spinner(f"Ottimizzazione {algorithm} con Cross-Validation..."):
            cv_model = cv.fit(train_df)
        
        predictions = cv_model.transform(test_df)
        accuracy = evaluator.evaluate(predictions)
        
        test_actual = [row.label for row in test_df.select("label").collect()]
        test_pred = [row.prediction for row in predictions.select("prediction").collect()]
        
        best_params = cv_model.bestModel.extractParamMap()
        best_params_dict = {str(param): value for param, value in best_params.items()}
                
        return {
            'algorithm': algorithm,
            'accuracy': accuracy,
            'test_actual': test_actual,
            'predictions': test_pred,
            'is_classification': True,
            'best_params': best_params_dict,
            'success': True
        }

    def _run_regression_optimized(self, algorithm: str, train_df: SparkDataFrame, test_df: SparkDataFrame) -> Dict[str, Any]:
        """Versione robusta per regressione."""
        try:
            self._ensure_spark_session_active()
            
            if algorithm == "Linear Regression":
                model = SparkLinearRegression(
                    featuresCol="features", 
                    labelCol="label",
                    maxIter=10,
                    regParam=0.1
                )
            
            trained_model = model.fit(train_df)
            predictions = trained_model.transform(test_df)
            
            evaluator = RegressionEvaluator(
                labelCol="label", 
                predictionCol="prediction", 
                metricName="mse"
            )
            mse = evaluator.evaluate(predictions) # Mean Squared Error
            
            test_size = test_df.count()
            if test_size > 1000:
                sample_test = test_df.sample(fraction=1000/test_size, seed=42)
                sample_pred = trained_model.transform(sample_test)
                test_actual = [row.label for row in sample_test.select("label").collect()]
                test_pred = [row.prediction for row in sample_pred.select("prediction").collect()]
            else:
                test_actual = [row.label for row in test_df.select("label").collect()]
                test_pred = [row.prediction for row in predictions.select("prediction").collect()]
            
            return {
                'algorithm': algorithm,
                'mse': mse,
                'test_actual': test_actual,
                'predictions': test_pred,
                'is_classification': False,
                'best_params': {'model': 'simplified'},
                'success': True
            }
            
        except Exception as e:
            return {'error': f'Errore {algorithm}: {e}', 'success': False}
    
    def _run_dbscan_optimized(self, features: List[str]) -> Dict[str, Any]:
        """Esegue DBSCAN con ottimizzazione degli iperparametri."""
        from sklearn.cluster import DBSCAN
        from sklearn.metrics import silhouette_score
        
        X, _, _ = self._prepare_data_robust(features)
        
        eps_values = [0.3, 0.5, 0.7, 1.0]
        min_samples_values = [5, 10]
        
        best_score = -1
        best_params = {}
        best_labels = None
        
        progress_bar = st.progress(0)
        
        total_combinations = len(eps_values) * len(min_samples_values)
        current_combination = 0
        
        for eps in eps_values:
            for min_samples in min_samples_values:
                current_combination += 1
                progress = current_combination / total_combinations
                progress_bar.progress(progress)
                
                try:
                    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
                    labels = dbscan.fit_predict(X)
                    
                    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
                    if n_clusters >= 2:
                        score = silhouette_score(X, labels)
                        if score > best_score:
                            best_score = score
                            best_params = {'eps': eps, 'min_samples': min_samples}
                            best_labels = labels
                
                except Exception:
                    continue
        
        progress_bar.empty()
        
        if best_labels is not None:
            n_clusters = len(set(best_labels)) - (1 if -1 in best_labels else 0)
            n_noise = list(best_labels).count(-1)
            
            return {
                'algorithm': 'DBSCAN',
                'labels': best_labels,
                'n_clusters': n_clusters,
                'n_noise': n_noise,
                'silhouette_score': best_score,
                'best_params': best_params,
                'success': True
            }
        else:
            return {
                'error': 'DBSCAN non è riuscito a trovare cluster validi con i parametri testati',
                'success': False
            }
    
    def _run_isolation_forest_optimized(self, features: List[str]) -> Dict[str, Any]:
        """Esegue Isolation Forest ottimizzato."""
        from sklearn.ensemble import IsolationForest
        
        X, _, _ = self._prepare_data_robust(features)
        
        best_params = {}
        with st.spinner("Ottimizzazione Isolation Forest..."):
            contamination_levels = [0.05, 0.1, 0.15]
            best_contamination = 0.1
            min_anomalies = float('inf')
            
            for cont in contamination_levels:
                iso = IsolationForest(contamination=cont, random_state=42)
                iso.fit(X)
                predictions = iso.predict(X)
                n_anomalies = (predictions == -1).sum()
                
                if 0 < n_anomalies < min_anomalies:
                    min_anomalies = n_anomalies
                    best_contamination = cont
            
            best_params['contamination'] = best_contamination
        
        final_model = IsolationForest(**best_params, random_state=42)
        final_model.fit(X)
        is_anomaly = final_model.predict(X) == -1
        anomaly_scores = final_model.score_samples(X)
                
        return {
            'algorithm': 'Isolation Forest',
            'is_anomaly': is_anomaly,
            'n_anomalies': is_anomaly.sum(),
            'best_params': best_params,
            'anomaly_scores': anomaly_scores,
            'success': True
        }
    
    def run_anomaly_detection_optimized(self, algorithm: str, features: List[str]) -> Dict[str, Any]:
        """Esegue algoritmi di anomaly detection con ottimizzazione."""
        try:
            if algorithm == "Isolation Forest":
                return self._run_isolation_forest_optimized(features)
            else:
                return {'error': f"Algoritmo {algorithm} non supportato", 'success': False}
        except Exception as e:
            return {'error': f"Errore in {algorithm}: {e}", 'success': False}
        
    def validate_ml_config(self, ml_config: Dict[str, Any]) -> Tuple[bool, str]:
        """Valida la configurazione ML con controlli robusti."""
        algorithm = ml_config.get('algorithm')
        features = ml_config.get('features', [])
        target = ml_config.get('target')
        
        if not algorithm or algorithm not in self.get_algorithm_requirements():
            return False, f"Algoritmo '{algorithm}' non valido o non supportato"
        
        alg_req = self.get_algorithm_requirements()[algorithm]
        
        if len(features) < alg_req['min_features']:
            return False, f"{algorithm} richiede almeno {alg_req['min_features']} feature(s)"
        
        if alg_req['requires_target']:
            if not target or target not in self.df.columns:
                return False, f"Target '{target}' richiesto e non trovato per {algorithm}"
            if target in features:
                return False, f"La colonna target '{target}' non può essere inclusa nelle features"
        
        return True, ""
    
    def _prepare_data_robust(self, features: List[str], target: Optional[str] = None) -> Tuple[np.ndarray, Optional[np.ndarray], Dict[str, Any]]:
        """Prepara i dati in modo robusto."""
        cols_to_use = features + ([target] if target else [])
        df_clean = self.df[cols_to_use].dropna().copy()

        if df_clean.empty:
            raise ValueError("Nessun dato valido dopo la rimozione dei valori mancanti.")

        X_processed = df_clean[features]
        for col in features:
            if X_processed[col].dtype == 'object' or X_processed[col].dtype.name == 'category':
                le = LabelEncoder()
                X_processed[col] = le.fit_transform(X_processed[col].astype(str))
                self.label_encoders[col] = le

        X_scaled = self.scaler.fit_transform(X_processed)

        y_processed = None
        target_type = None
        if target:
            y_series = df_clean[target]
            if y_series.dtype == 'object' or y_series.nunique() <= 10:
                target_type = 'classification'
                le = LabelEncoder()
                y_processed = le.fit_transform(y_series.astype(str))
                self.label_encoders[target] = le
            else:
                target_type = 'regression'
                y_processed = y_series.values

        metadata = {'target_type': target_type}
        return X_scaled, y_processed, metadata
    
    def execute_ml_config(self, ml_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Esegue una configurazione ML completa (Spark o scikit-learn).
        """
        is_valid, error_msg = self.validate_ml_config(ml_config)
        if not is_valid:
            return {'error': error_msg, 'success': False}
        
        algorithm = ml_config['algorithm']
        features = ml_config.get('features', [])
        target = ml_config.get('target')
        
        try:
            alg_req = self.get_algorithm_requirements()[algorithm]
            alg_type = alg_req['type']
            engine = alg_req['engine']
            
            if engine == 'spark':
                if self.spark_df is None:
                    self.spark_df = self.spark.createDataFrame(self.df)
                
                if alg_type == 'clustering':
                    return self.run_clustering_optimized(algorithm, features)
                elif alg_type in ['classification', 'regression']:
                    return self.run_supervised_optimized(algorithm, features, target)
            
            elif engine == 'sklearn':
                if alg_type == 'clustering':
                    if algorithm == 'DBSCAN':
                        return self._run_dbscan_optimized(features)
                elif alg_type == 'anomaly':
                    return self.run_anomaly_detection_optimized(algorithm, features)
            
            return {'error': f"Combinazione algoritmo/engine '{algorithm}/{engine}' non gestita.", 'success': False}
                
        except Exception as e:
            return {'error': f"Errore nell'esecuzione di {algorithm}: {e}", 'success': False}

### 4. CLASSE PER VISUALIZZAZIONE ML CON SPARK
# ==============================================================================

class SparkMLVisualizer:
    """Classe per visualizzare i risultati degli algoritmi Spark MLlib e scikit-learn."""
    
    def __init__(self, ml_processor: SparkMLProcessor):
        self.ml_processor = ml_processor

    def display_ml_results(self, ml_results: List[Dict[str, Any]], ml_configs: List[Dict[str, Any]]):
        """Visualizza tutti i risultati ML usando la logica unificata per Spark e scikit-learn."""
        if not ml_results:
            st.info("Nessun algoritmo ML configurato")
            return

        st.divider()
        st.markdown("### 🤖 Algoritmi di ML")

        if len(ml_results) == 1:
            st.markdown(f"#### {ml_configs[0].get('algorithm', 'Algoritmo Sconosciuto')}")
            self.visualize_results(ml_results[0], ml_configs[0])
            return

        tab_names = []
        for i, cfg in enumerate(ml_configs):
            algorithm = cfg.get('algorithm', f'Algoritmo {i+1}')
            # Aggiungi indicatore engine se disponibile
            engine_info = self._get_engine_indicator(algorithm)
            tab_names.append(f"{algorithm} {engine_info}")

        tabs = st.tabs(tab_names)

        for tab, result, config in zip(tabs, ml_results, ml_configs):
            with tab:
                self.visualize_results(result, config)
    
    def _get_engine_indicator(self, algorithm: str) -> str:
        """Restituisce un indicatore visivo per l'engine utilizzato."""
        spark_algorithms = [
            'K-Means', 'Bisecting K-Means', 'Random Forest Classifier', 
            'GBT Classifier', 'Linear Regression'
        ]
        
        if algorithm in spark_algorithms:
            return "⚡"  # Spark
        else:
            return "🐍"  # Scikit-learn
    
    def visualize_results(self, results: Dict[str, Any], config: Dict[str, Any]):
        """Visualizza i risultati ML con logica unificata per Spark e scikit-learn."""
        if not results.get('success', False):
            st.error(f"❌ {results.get('error', 'Errore sconosciuto')}")
            return
        
        algorithm = results['algorithm']
        
        if 'best_params' in results and results['best_params']:
            with st.expander("🔧 Parametri Ottimizzati", expanded=False):
                st.json(results['best_params'])
        
        if algorithm in ['K-Means', 'Bisecting K-Means', 'DBSCAN']:
            self._visualize_clustering(results, config)
        elif algorithm in ['Random Forest Classifier', 'GBT Classifier', 'Linear Regression']:
            self._visualize_supervised(results)
        elif algorithm == 'Isolation Forest':
            self._visualize_anomaly_detection(results, config)
    
    def _visualize_clustering(self, results: Dict, config: Dict):
        """Visualizza risultati clustering (Spark e scikit-learn)."""
        algorithm = results['algorithm']
        
        if algorithm == 'DBSCAN':
            col1, col2, col3 = st.columns(3)
            col1.metric("Cluster Trovati", results.get('n_clusters', 'N/A'))
            col2.metric("Punti Noise", results.get('n_noise', 'N/A'))
            col3.metric("Silhouette Score", f"{results.get('silhouette_score', 0):.3f}")
        else:
            col1, col2 = st.columns(2)
            col1.metric("Cluster Trovati", results.get('n_clusters', 'N/A'))
            col2.metric("Silhouette Score", f"{results.get('silhouette_score', 0):.3f}")
        
        features = config.get('features', [])
        if len(features) >= 2 and 'labels' in results:
            original_df = self.ml_processor.df.copy()
            df_clean = original_df[features].dropna()
            
            if len(df_clean) == len(results['labels']):
                df_plot = df_clean.copy()
                df_plot['Cluster'] = results['labels']
                
                if algorithm == 'DBSCAN':
                    df_plot['Cluster'] = df_plot['Cluster'].astype(str)
                    df_plot.loc[df_plot['Cluster'] == '-1', 'Cluster'] = 'Noise'
                else:
                    df_plot['Cluster'] = df_plot['Cluster'].astype(str)

                hover_col = next((col for col in original_df.columns 
                                if col not in features and original_df[col].dtype == 'object'), None)
                
                if hover_col:
                    df_plot[hover_col] = original_df.loc[df_clean.index, hover_col]

                if algorithm == 'DBSCAN':
                    color_map = {}
                    unique_clusters = df_plot['Cluster'].unique()
                    colors = px.colors.qualitative.Set3
                    
                    for i, cluster in enumerate(unique_clusters):
                        if cluster == 'Noise':
                            color_map[cluster] = '#808080'  # Grigio per noise
                        else:
                            color_map[cluster] = colors[i % len(colors)]
                else:
                    color_map = None

                fig = px.scatter(
                    df_plot, 
                    x=features[0], 
                    y=features[1], 
                    color='Cluster', 
                    title=f"Visualizzazione Cluster {algorithm}",
                    hover_name=hover_col,
                    color_discrete_map=color_map
                )
                
                st.plotly_chart(fig, use_container_width=True)
    
    def _visualize_anomaly_detection(self, results: Dict, config: Dict):
        """Visualizza risultati anomaly detection."""
        st.metric("Anomalie Rilevate", results.get('n_anomalies', 'N/A'))
        
        if results['algorithm'] == 'Isolation Forest':
            scores_df = st.session_state.get('last_query_result').copy()
            scores_df["Score"] = results['anomaly_scores']
            scores_df["Anomalia"] = results['is_anomaly']

            fig = px.scatter(
                scores_df,
                x=scores_df.index,
                y="Score",
                color="Anomalia",
                title="Isolation Forest - Anomaly Scores",
                hover_data=scores_df.columns
            )
            st.plotly_chart(fig, use_container_width=True)
    
    def _visualize_supervised(self, results: Dict):
        """Visualizza risultati algoritmi supervisionati (invariata)."""
        is_classification = results.get('is_classification', False)
        target_type = results.get('target_type', 'classification' if is_classification else 'regression')
        
        if is_classification:
            metric_val = results.get('accuracy', 0)
            st.metric("Accuracy", f"{metric_val:.3f}")
        else:
            metric_val = results.get('mse', 0)
            st.metric("MSE", f"{metric_val:.3f}")
        
        y_test, y_pred = results['test_actual'], results['predictions']
        
        if is_classification:
            conf_matrix = pd.crosstab(
                pd.Series(y_test, name='Reali'), 
                pd.Series(y_pred, name='Predizioni')
            )
            fig = px.imshow(conf_matrix, text_auto=True, 
                           title="Matrice di Confusione",
                           color_continuous_scale='Blues')
        else:
            fig = px.scatter(
                x=y_test, y=y_pred, 
                labels={'x': 'Valori Reali', 'y': 'Predizioni'}, 
                title="Predizioni vs. Valori Reali"
            )
            fig.add_shape(
                type="line", 
                x0=min(y_test), y0=min(y_test), 
                x1=max(y_test), y1=max(y_test), 
                line=dict(dash="dash", color="red")
            )
        
        st.plotly_chart(fig, use_container_width=True)

### 5. FUNZIONI DI SUPPORTO (invariate)
# ==============================================================================
def normalize_query_for_comparison(query: str) -> str:
    """Normalizza una query per il confronto."""
    if not query: 
        return ""    
    query = re.sub(r"--.*?,'", "", query, flags=re.MULTILINE)
    query = re.sub(r'\s+', ' ', query.strip(), flags=re.MULTILINE)
    return query.lower()

def find_predefined_config(query_text: str, config_key: str) -> List[Dict[str, Any]]:
    """Trova configurazioni predefinite (grafici o ML) per una query."""
    if not query_text.strip(): return []
    query_normalized = normalize_query_for_comparison(query_text)
    
    try:
        all_templates = get_twitter_query_templates()
        for category, queries in all_templates.items():
            for name, query_data in queries.items():
                if isinstance(query_data, dict) and 'query' in query_data:
                    template_normalized = normalize_query_for_comparison(query_data.get('query', ''))
                    if query_normalized == template_normalized:
                        return query_data.get(config_key, [])
    except Exception as e:
        st.error(f"Errore nel caricamento delle configurazioni predefinite: {e}")
    return []

def is_hashable(series: pd.Series) -> bool:
    """Controlla se una series pandas contiene dati hashable."""
    try:
        series.nunique()
        return True
    except TypeError:
        return False

def find_best_columns_for_analysis(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], str]:
    """Trova le migliori colonne per un'analisi automatica."""
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns
    numerical_cols = df.select_dtypes(include=np.number).columns
    
    suitable_candidates = {col: df[col].nunique() 
                         for col in categorical_cols 
                         if is_hashable(df[col]) and 3 < df[col].nunique() < 50}
    best_group_by = min(suitable_candidates, key=suitable_candidates.get) if suitable_candidates else None
    
    if not best_group_by: 
        return None, None, 'count'

    possible_agg_cols = [col for col in numerical_cols if col != best_group_by]
    return (best_group_by, possible_agg_cols[0], 'sum') if possible_agg_cols else (best_group_by, 'count', 'count')


### 6. FUNZIONE PRINCIPALE
# ==============================================================================
def show_analytics_page():
    """
    Funzione principale per visualizzare la pagina di analisi con supporto Spark MLlib.
    """
    dataset = st.session_state.get('last_query_result')
    if dataset is None or not isinstance(dataset, pd.DataFrame) or dataset.empty:
        st.warning("Esegui una query SQL per caricare un dataset valido.")
        return

    spark = st.session_state.spark_manager.get_spark_session()
    if spark is None:
        st.error("❌ Spark session non disponibile. Assicurati che Spark sia inizializzato.")
        return

    query_text = st.session_state.get('last_query_text', '')
    predefined_charts = find_predefined_config(query_text, 'charts')
    predefined_ml = find_predefined_config(query_text, 'ml_algorithms')
    
    if predefined_charts or predefined_ml:
        st.markdown("### 📊 Grafici")
        
        if predefined_charts:
            ChartVisualizer().display_charts_from_config(dataset, predefined_charts)
        
        if predefined_ml:
            try:
                spark_ml_processor = SparkMLProcessor(dataset, spark)
                spark_ml_visualizer = SparkMLVisualizer(spark_ml_processor)
                
                with st.spinner("Esecuzione algoritmi Spark MLlib..."):
                    ml_results = [spark_ml_processor.execute_ml_config(cfg) for cfg in predefined_ml]
                
                spark_ml_visualizer.display_ml_results(ml_results, predefined_ml)
                
            except Exception as e:
                st.error(f"Errore nell'esecuzione ML con Spark: {e}")