"""
Pagina Streamlit per l'analisi visiva automatica e intelligente dei dati.
Include supporto completo per grafici e algoritmi ML predefiniti dai metadati JSON.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, Tuple, Optional, List
import warnings
import re

# Import specifici per ML
from utils.utils import get_twitter_query_templates
from sklearn.cluster import KMeans, DBSCAN
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, IsolationForest
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.svm import SVR, SVC
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split, GridSearchCV, RandomizedSearchCV
from sklearn.metrics import silhouette_score, accuracy_score, mean_squared_error, classification_report
import xgboost as xgb
from statsmodels.tsa.seasonal import STL

# Gestione dipendenza opzionale (Prophet)
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

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
    Classe helper per eseguire aggregazioni sui dati utilizzando pandas.
    """
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe

    def perform_aggregation(self, group_by_col: str, agg_col: str, agg_type: str, sort_desc: bool = True) -> Optional[pd.DataFrame]:
        """
        Esegue un'aggregazione su un DataFrame pandas.
        """
        try:
            # Gestisce il caso speciale del conteggio
            if agg_col == "count" or agg_type == "count":
                result = self.df.groupby(group_by_col).size().reset_index(name='count')
                sort_column = 'count'
            # Gestisce le aggregazioni numeriche standard
            else:
                result = self.df.groupby(group_by_col)[agg_col].agg(agg_type).reset_index()
                sort_column = agg_col
            
            # Rinomina la colonna aggregata per maggiore chiarezza (es. 'val' -> 'sum_val')
            new_col_name = f"{agg_type}_{agg_col}" if agg_col != 'count' else 'count'
            result = result.rename(columns={agg_col: new_col_name})
            sort_column = new_col_name
            
            # Ordina i risultati come richiesto
            if sort_column in result.columns:
                result = result.sort_values(by=sort_column, ascending=(not sort_desc))
            
            return result
        except Exception as e:
            st.error(f"Errore durante l'aggregazione dei dati: {e}")
            return None

### 2. CLASSE PER LA VISUALIZZAZIONE DEI GRAFICI
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
                return False, f"Colonna etichette '{x_col}' non trovata"
        
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
        
        try:
            # ========== PREPARAZIONE CENTRALIZZATA DEI DATI ==========
            prepared_data = self._prepare_chart_data(df, x_col, y_col, chart_type)
            
            if prepared_data is None or prepared_data['df'].empty:
                st.warning("Nessun dato valido dopo la preparazione.")
                return None
            
            # Estrai i dati preparati
            df_final = prepared_data['df']
            x_col_final = prepared_data['x_col']
            y_col_final = prepared_data['y_col']
            
            # ========== CREAZIONE GRAFICO CON DATI UNIFICATI ==========
            fig = self._create_chart_by_type(df_final, chart_type, x_col_final, y_col_final, x_col)
            
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
                counts = df_clean[x_col].value_counts().head(20)
                df_final = pd.DataFrame({x_col: counts.index, 'Conteggio': counts.values})
                x_col_final, y_col_final = x_col, 'Conteggio'
            else:
                df_clean = df_clean.dropna(subset=[y_col])
                if df_clean[x_col].dtype == 'object' and pd.api.types.is_numeric_dtype(df_clean[y_col]):
                    df_final = df_clean.groupby(x_col)[y_col].sum().reset_index().head(20)
                else:
                    df_final = df_clean[[x_col, y_col]].head(500)
                x_col_final, y_col_final = x_col, y_col
        
        return {'df': df_final, 'x_col': x_col_final, 'y_col': y_col_final}

    def _create_chart_by_type(self, df: pd.DataFrame, chart_type: str, x_col: str, y_col: str, 
                            original_x_col: str) -> Optional[go.Figure]:
        """Crea il grafico specifico usando dati già preparati e unificati."""
        fig = None
        
        if chart_type == 'barre':
            fig = px.bar(df, x=x_col, y=y_col, title=f"Grafico a Barre - {y_col} per {original_x_col}")
            fig.update_traces(width=0.6)
            fig.update_xaxes(title=original_x_col, type='category')
        
        elif chart_type == 'linee':
            fig = px.line(df, x=x_col, y=y_col, markers=True, title=f"Grafico a Linee - {y_col} rispetto a {original_x_col}")
            fig.update_xaxes(title=original_x_col, type='category')
        
        elif chart_type == 'serie temporale con picchi':
            st.warning("Serie temporale con picchi richiede dati originali - implementazione speciale necessaria.")
            return None
        
        elif chart_type == 'torta':
            df_pie = df.nlargest(10, y_col) if len(df) > 10 else df
            fig = px.pie(df_pie, values=y_col, names=x_col, title=f"Distribuzione - Top {len(df_pie)} {original_x_col}")
        
        elif chart_type == 'heatmap':
            if len(df[x_col].unique()) <= 50 and len(df[y_col].unique()) <= 50:
                crosstab = pd.crosstab(df[y_col], df[x_col])
                fig = px.imshow(crosstab, title=f"Heatmap: {y_col} vs {x_col}")
            else:
                st.warning("Troppi valori unici per creare una heatmap significativa.")
                return None
        
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

### 3. CLASSE PER MACHINE LEARNING
# ==============================================================================

class MLProcessor:
    """
    Classe migliorata per gestire l'esecuzione di algoritmi ML con ottimizzazione automatica degli iperparametri.
    """
    
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe.copy()
        self.scaler = StandardScaler()
        self.label_encoders = {}
        
    def get_algorithm_requirements(self) -> Dict[str, Dict[str, Any]]:
        """Restituisce i requisiti per ogni algoritmo ML."""
        return {
            'K-Means': {'type': 'clustering', 'requires_target': False, 'min_features': 1, 'description': 'Clustering basato su centroidi'},
            'DBSCAN': {'type': 'clustering', 'requires_target': False, 'min_features': 1, 'description': 'Clustering basato su densità'},
            'Random Forest': {'type': 'supervised', 'requires_target': True, 'min_features': 1, 'description': 'Ensemble di alberi decisionali'},
            'XGBoost': {'type': 'supervised', 'requires_target': True, 'min_features': 1, 'description': 'Gradient boosting ottimizzato'},
            'Linear Regression': {'type': 'supervised', 'requires_target': True, 'min_features': 1, 'description': 'Regressione lineare semplice'},
            'Logistic Regression': {'type': 'supervised', 'requires_target': True, 'min_features': 1, 'description': 'Classificazione logistica'},
            'SVM': {'type': 'supervised', 'requires_target': True, 'min_features': 1, 'description': 'Support Vector Machine'},
            'PCA': {'type': 'dimensionality', 'requires_target': False, 'min_features': 2, 'description': 'Riduzione dimensionalità lineare'},
            'Isolation Forest': {'type': 'anomaly', 'requires_target': False, 'min_features': 1, 'description': 'Rilevamento anomalie basato su isolamento'},
            'STL Decomposition': {'type': 'anomaly', 'requires_target': False, 'min_features': 1, 'description': 'Decomposizione serie temporali'},
            'Prophet': {'type': 'anomaly', 'requires_target': False, 'min_features': 0, 'description': 'Previsioni serie temporali'}
        }
    
    def validate_ml_config(self, ml_config: Dict[str, Any]) -> Tuple[bool, str]:
        """Valida la configurazione ML con controlli più robusti."""
        algorithm = ml_config.get('algorithm')
        features = ml_config.get('features', [])
        target = ml_config.get('target')
        
        if not algorithm or algorithm not in self.get_algorithm_requirements():
            return False, f"Algoritmo '{algorithm}' non valido o non supportato"
        
        alg_req = self.get_algorithm_requirements()[algorithm]
        
        if algorithm in ['STL Decomposition', 'Prophet']:
            if not self._find_datetime_columns() and not features:
                return False, f"{algorithm} richiede almeno una colonna datetime"
        elif len(features) < alg_req['min_features']:
            return False, f"{algorithm} richiede almeno {alg_req['min_features']} feature(s)"
        
        if alg_req['requires_target'] and (not target or target not in self.df.columns):
            return False, f"Target '{target}' richiesto e non trovato per {algorithm}"
        
        return True, ""

    def _find_datetime_columns(self) -> List[str]:
        """Trova colonne datetime nel DataFrame con controlli robusti."""
        datetime_cols = []
        for col in self.df.columns:
            if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                datetime_cols.append(col)
            elif self.df[col].dtype == 'object':
                try:
                    sample_size = min(50, len(self.df))
                    sample_data = self.df[col].dropna().iloc[:sample_size]
                    if not sample_data.empty:
                        pd.to_datetime(sample_data, errors='raise')
                        datetime_cols.append(col)
                except (ValueError, TypeError):
                    pass
        return datetime_cols
    
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
    
    def _optimize_hyperparameters(self, algorithm: str, X: np.ndarray, y: Optional[np.ndarray] = None, is_classification: bool = True) -> Tuple[Any, Dict[str, Any]]:
        """Ottimizza gli iperparametri usando Grid Search."""
        param_grids = {
            'K-Means': {'n_clusters': [3, 4, 5, 6]},
            'DBSCAN': {'eps': [0.3, 0.5, 0.7], 'min_samples': [5, 10]},
            'Random Forest': {'n_estimators': [100, 200], 'max_depth': [5, 10, None]},
            'XGBoost': {'n_estimators': [100, 200], 'max_depth': [3, 5], 'learning_rate': [0.01, 0.1]},
            'SVM': {'C': [0.1, 1, 10], 'kernel': ['rbf', 'linear']},
        }
        model_map = {
            'K-Means': KMeans, 'DBSCAN': DBSCAN, 'Random Forest': RandomForestClassifier if is_classification else RandomForestRegressor,
            'XGBoost': xgb.XGBClassifier if is_classification else xgb.XGBRegressor, 'SVM': SVC if is_classification else SVR
        }
        
        if algorithm not in param_grids:
            return model_map.get(algorithm, lambda: None)(), {}

        model_class = model_map[algorithm]
        param_grid = param_grids[algorithm]
        
        if y is not None: # Algoritmi supervisionati
            search = GridSearchCV(model_class(random_state=42), param_grid, cv=3, n_jobs=-1)
            search.fit(X, y)
            return search.best_estimator_, search.best_params_
        else: # Clustering
            best_score = -1
            best_params = {}
            from itertools import product
            for params in [dict(zip(param_grid.keys(), v)) for v in product(*param_grid.values())]:
                try:
                    model = model_class(**params)
                    labels = model.fit_predict(X)
                    if len(set(labels)) > 1:
                        score = silhouette_score(X, labels)
                        if score > best_score:
                            best_score, best_params = score, params
                except: continue
            return model_class(**best_params).fit(X), best_params

    def run_clustering_optimized(self, algorithm: str, X: np.ndarray) -> Dict[str, Any]:
        """Esegue clustering con ottimizzazione automatica."""
        with st.spinner(f"Ottimizzazione iperparametri per {algorithm}..."):
            best_model, best_params = self._optimize_hyperparameters(algorithm, X)
        
        labels = best_model.labels_
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        silhouette = silhouette_score(X, labels) if n_clusters > 1 else 0
        
        return {
            'algorithm': algorithm, 'labels': labels, 'n_clusters': n_clusters,
            'n_noise': list(labels).count(-1) if algorithm == 'DBSCAN' else 0,
            'silhouette_score': silhouette, 'best_params': best_params, 'success': True
        }

    def run_supervised_optimized(self, algorithm: str, X: np.ndarray, y: np.ndarray, metadata: Dict) -> Dict[str, Any]:
        """Esegue algoritmi supervisionati con ottimizzazione."""
        is_classification = metadata['target_type'] == 'classification'
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y if is_classification else None)
        
        with st.spinner(f"Ottimizzazione iperparametri per {algorithm}..."):
            best_model, best_params = self._optimize_hyperparameters(algorithm, X_train, y_train, is_classification)
        
        best_model.fit(X_train, y_train)
        y_pred = best_model.predict(X_test)
        
        metric_name, score = ('accuracy', accuracy_score(y_test, y_pred)) if is_classification else ('mse', mean_squared_error(y_test, y_pred))
        
        return {
            'algorithm': algorithm, 'model': best_model, 'predictions': y_pred, 'test_actual': y_test,
            metric_name: score, 'is_classification': is_classification, 'best_params': best_params,
            'classification_report': classification_report(y_test, y_pred, output_dict=True, zero_division=0) if is_classification else None,
            'success': True
        }

    def run_anomaly_detection_optimized(self, algorithm: str, features: List[str]) -> Dict[str, Any]:
        """Esegue algoritmi di anomaly detection con ottimizzazione."""
        try:
            if algorithm == "Isolation Forest":
                return self._run_isolation_forest_optimized(features)
            elif algorithm == "STL Decomposition":
                return self._run_stl_decomposition_optimized(features)
            elif algorithm == "Prophet":
                return self._run_prophet_anomaly_optimized()
            else:
                return {'error': f"Algoritmo {algorithm} non supportato", 'success': False}
        except Exception as e:
            return {'error': f"Errore in {algorithm}: {e}", 'success': False}

    def _prepare_time_series_data(self, features: List[str] = None) -> Tuple[pd.DataFrame, str, str]:
        """Prepara dati per analisi serie temporali."""
        datetime_cols = self._find_datetime_columns()
        if not datetime_cols: raise ValueError("Nessuna colonna datetime trovata.")
        date_col = datetime_cols[0]
        
        value_col = features[0] if features else next((c for c in self.df.select_dtypes(include=np.number).columns if c != date_col), None)
        if not value_col: raise ValueError("Nessuna colonna numerica trovata.")

        df_ts = self.df[[date_col, value_col]].copy()
        df_ts[date_col] = pd.to_datetime(df_ts[date_col], errors='coerce')
        return df_ts.dropna().sort_values(date_col), date_col, value_col
    
    def _run_isolation_forest_optimized(self, features: List[str]) -> Dict[str, Any]:
        """Esegue Isolation Forest ottimizzato."""
        X, _, _ = self._prepare_data_robust(features)
        
        best_params = {}
        with st.spinner("Ottimizzazione Isolation Forest..."):
            contamination_levels = [0.05, 0.1, 0.15]
            best_contamination, min_anomalies = 0.1, float('inf')
            for cont in contamination_levels:
                iso = IsolationForest(contamination=cont, random_state=42).fit(X)
                n_anomalies = (iso.predict(X) == -1).sum()
                if 0 < n_anomalies < min_anomalies:
                    min_anomalies = n_anomalies
                    best_contamination = cont
            best_params['contamination'] = best_contamination

        final_model = IsolationForest(**best_params, random_state=42).fit(X)
        is_anomaly = final_model.predict(X) == -1
        
        return {
            'algorithm': 'Isolation Forest', 'is_anomaly': is_anomaly, 'n_anomalies': is_anomaly.sum(),
            'best_params': best_params, 'anomaly_scores': final_model.score_samples(X), 'success': True
        }

    def _run_stl_decomposition_optimized(self, features: List[str]) -> Dict[str, Any]:
        """Esegue STL Decomposition ottimizzato."""
        df_ts, date_col, value_col = self._prepare_time_series_data(features)
        df_ts = df_ts.set_index(date_col)
        
        best_params = {}
        with st.spinner("Ottimizzazione periodo STL..."):
            possible_periods = [p for p in [7, 30, 365] if len(df_ts) >= p * 2]
            best_period, best_aic = 7, float('inf')
            for period in possible_periods:
                resid = STL(df_ts[value_col], period=period).fit().resid
                aic = len(resid) * np.log(np.var(resid)) + 2 * period
                if aic < best_aic: best_aic, best_period = aic, period
            best_params['period'] = best_period
        
        decomposition = STL(df_ts[value_col], **best_params).fit()
        threshold = 2.5 * decomposition.resid.std()
        is_anomaly = np.abs(decomposition.resid) > threshold
        
        return {
            'algorithm': 'STL Decomposition', 'decomposition': decomposition, 'is_anomaly': is_anomaly,
            'n_anomalies': is_anomaly.sum(), 'best_params': best_params, 'data': df_ts, 'success': True
        }

    def _run_prophet_anomaly_optimized(self) -> Dict[str, Any]:
        """Esegue Prophet ottimizzato."""
        if not PROPHET_AVAILABLE: return {'error': "Prophet non installato.", 'success': False}
        df_ts, _, _ = self._prepare_time_series_data()
        prophet_df = df_ts.rename(columns={df_ts.columns[0]: 'ds', df_ts.columns[1]: 'y'})

        model = Prophet(interval_width=0.95, yearly_seasonality='auto', daily_seasonality='auto').fit(prophet_df)
        forecast = model.predict(prophet_df)
        is_anomaly = (prophet_df['y'] < forecast['yhat_lower']) | (prophet_df['y'] > forecast['yhat_upper'])

        return {
            'algorithm': 'Prophet', 'model': model, 'forecast': forecast, 'is_anomaly': is_anomaly,
            'n_anomalies': is_anomaly.sum(), 'best_params': {'interval_width': 0.95}, 'success': True
        }
    
    def run_pca_optimized(self, features: List[str]) -> Dict[str, Any]:
        """Esegue PCA con ottimizzazione automatica dei componenti."""
        X, _, _ = self._prepare_data_robust(features)
        
        with st.spinner("Ottimizzazione numero componenti PCA..."):
            pca_temp = PCA().fit(X)
            cumulative_variance = np.cumsum(pca_temp.explained_variance_ratio_)
            optimal_components = np.where(cumulative_variance >= 0.95)[0][0] + 1
        
        final_pca = PCA(n_components=optimal_components).fit(X)
        
        return {
            'algorithm': 'PCA', 'model': final_pca, 'transformed_data': final_pca.transform(X),
            'explained_variance_ratio': final_pca.explained_variance_ratio_,
            'total_explained_variance': sum(final_pca.explained_variance_ratio_),
            'n_components': optimal_components, 'best_params': {'n_components': optimal_components}, 'success': True
        }
    
    def execute_ml_config(self, ml_config: Dict[str, Any]) -> Dict[str, Any]:
        """Esegue una configurazione ML completa con ottimizzazione automatica."""
        is_valid, error_msg = self.validate_ml_config(ml_config)
        if not is_valid: return {'error': error_msg, 'success': False}
        
        algorithm = ml_config['algorithm']
        features = ml_config.get('features', [])
        target = ml_config.get('target')
        
        try:
            alg_type = self.get_algorithm_requirements()[algorithm]['type']
            
            if alg_type == 'anomaly':
                return self.run_anomaly_detection_optimized(algorithm, features)
            
            X, y, metadata = self._prepare_data_robust(features, target)
            
            if alg_type == 'clustering':
                return self.run_clustering_optimized(algorithm, X)
            elif alg_type == 'supervised':
                return self.run_supervised_optimized(algorithm, X, y, metadata)
            elif alg_type == 'dimensionality':
                return self.run_pca_optimized(features)
            
            return {'error': f"Tipo di algoritmo '{alg_type}' non gestito.", 'success': False}
                
        except Exception as e:
            return {'error': f"Errore nell'esecuzione di {algorithm}: {e}", 'success': False}

### 4. CLASSE PER VISUALIZZAZIONE ML
# ==============================================================================

class MLVisualizer:
    """Classe migliorata per visualizzare i risultati degli algoritmi ML."""
    
    def __init__(self, ml_processor: MLProcessor):
        self.ml_processor = ml_processor
    
    def visualize_results(self, results: Dict[str, Any], config: Dict[str, Any]):
        """Visualizza i risultati ML con una logica unificata."""
        if not results.get('success', False):
            st.error(f"❌ {results.get('error', 'Errore sconosciuto')}")
            return
        
        algorithm = results['algorithm']
        
        if 'best_params' in results and results['best_params']:
            with st.expander("🔧 Parametri Ottimizzati", expanded=False):
                st.json(results['best_params'])
        
        # Dispatch alla funzione di visualizzazione specifica
        if algorithm in ['K-Means', 'DBSCAN']:
            self._visualize_clustering(results, config)
        elif algorithm in ['Random Forest', 'XGBoost', 'Linear Regression', 'Logistic Regression', 'SVM']:
            self._visualize_supervised(results)
        elif algorithm == 'PCA':
            self._visualize_pca(results)
        elif algorithm in ['Isolation Forest', 'STL Decomposition', 'Prophet']:
            self._visualize_anomaly_detection(results)
    
    def _visualize_clustering(self, results: Dict, config: Dict):
        """Visualizza risultati clustering."""
        col1, col2 = st.columns(2)
        col1.metric("Cluster Trovati", results.get('n_clusters', 'N/A'))
        col2.metric("Silhouette Score", f"{results.get('silhouette_score', 0):.3f}")
        
        features = config.get('features', [])
        if len(features) >= 2 and 'labels' in results:
            df_clean = self.ml_processor.df[features].dropna()
            if len(df_clean) == len(results['labels']):
                df_plot = pd.DataFrame({features[0]: df_clean.iloc[:, 0], features[1]: df_clean.iloc[:, 1], 'Cluster': results['labels']})
                fig = px.scatter(df_plot, x=features[0], y=features[1], color='Cluster', title=f"Visualizzazione Cluster {results['algorithm']}")
                st.plotly_chart(fig, use_container_width=True)
    
    def _visualize_supervised(self, results: Dict):
        """Visualizza risultati algoritmi supervisionati."""
        is_classification = results.get('is_classification', False)
        metric_name = "Accuracy" if is_classification else "MSE"
        metric_val = results.get('accuracy') if is_classification else results.get('mse')
        st.metric(metric_name, f"{metric_val:.3f}")
        
        y_test, y_pred = results['test_actual'], results['predictions']
        if is_classification:
            conf_matrix = pd.crosstab(pd.Series(y_test, name='Reali'), pd.Series(y_pred, name='Predizioni'))
            fig = px.imshow(conf_matrix, text_auto=True, title="Matrice di Confusione")
        else:
            fig = px.scatter(x=y_test, y=y_pred, labels={'x': 'Valori Reali', 'y': 'Predizioni'}, title="Predizioni vs. Valori Reali")
            fig.add_shape(type="line", x0=min(y_test), y0=min(y_test), x1=max(y_test), y1=max(y_test), line=dict(dash="dash", color="red"))
        st.plotly_chart(fig, use_container_width=True)
    
    def _visualize_pca(self, results: Dict):
        """Visualizza risultati PCA."""
        col1, col2 = st.columns(2)
        col1.metric("Componenti Ottimali", results.get('n_components', 'N/A'))
        col2.metric("Varianza Spiegata", f"{results.get('total_explained_variance', 0):.2%}")
        
        explained_var = results.get('explained_variance_ratio', [])
        fig = px.bar(x=[f"PC{i+1}" for i in range(len(explained_var))], y=explained_var, title="Varianza Spiegata per Componente")
        st.plotly_chart(fig, use_container_width=True)
    
    def _visualize_anomaly_detection(self, results: Dict):
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

            
        elif results['algorithm'] == 'STL Decomposition':
            decomp = results['decomposition']
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=decomp.observed.index, y=decomp.observed, mode='lines', name='Originale'))
            fig.add_trace(go.Scatter(x=decomp.trend.index, y=decomp.trend, mode='lines', name='Trend'))
            anomalies = decomp.resid[results['is_anomaly']]
            fig.add_trace(go.Scatter(x=anomalies.index, y=anomalies, mode='markers', name='Anomalie', marker=dict(color='red', size=8)))
            fig.update_layout(title="Decomposizione STL con Anomalie")
            st.plotly_chart(fig, use_container_width=True)
            
        elif results['algorithm'] == 'Prophet':
            fig = results['model'].plot(results['forecast'])
            anomalies = results['forecast'][results['is_anomaly']]
            fig.add_trace(go.Scatter(x=anomalies['ds'], y=anomalies['yhat'], mode='markers', name='Anomalie', marker=dict(color='red', size=8)))
            st.plotly_chart(fig, use_container_width=True)

    def display_ml_results(self, ml_results: List[Dict[str, Any]], ml_configs: List[Dict[str, Any]]):
        """Visualizza tutti i risultati ML usando la logica unificata."""
        if not ml_results:
            st.info("Nessun algoritmo ML configurato")
            return

        st.divider()
        st.markdown("### 🤖 Risultati Machine Learning")

        tab_names = [cfg.get('algorithm', 'Sconosciuto') for cfg in ml_configs]
        tabs = st.tabs(tab_names)

        for tab, result, config in zip(tabs, ml_results, ml_configs):
            with tab:
                # Chiama la funzione di visualizzazione unificata per ogni risultato
                self.visualize_results(result, config)

### 5. FUNZIONI DI SUPPORTO
# ==============================================================================
def normalize_query_for_comparison(query: str) -> str:
    """Normalizza una query per il confronto."""
    if not query: return ""
    query = re.sub(r'--.*?$', '', query, flags=re.MULTILINE)
    query = re.sub(r'\s+', ' ', query.strip())
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
    
    suitable_candidates = {col: df[col].nunique() for col in categorical_cols if is_hashable(df[col]) and 3 < df[col].nunique() < 50}
    best_group_by = min(suitable_candidates, key=suitable_candidates.get) if suitable_candidates else None
    
    if not best_group_by: return None, None, 'count'

    possible_agg_cols = [col for col in numerical_cols if col != best_group_by]
    return (best_group_by, possible_agg_cols[0], 'sum') if possible_agg_cols else (best_group_by, 'count', 'count')

def suggest_charts(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """Suggerisce grafici appropriati per un DataFrame aggregato."""
    if df is None or df.empty or len(df.columns) < 2: return {}
    cat_col, num_col = df.columns[0], df.columns[1]

    suggestions = {'bar_chart': {'type': 'bar', 'title': f'Classifica per {cat_col}', 'config': {'x': cat_col, 'y': num_col}}}
    if df[cat_col].nunique() <= 10 and df[num_col].min() >= 0:
        suggestions['pie_chart'] = {'type': 'pie', 'title': f'Distribuzione per {cat_col}', 'config': {'names': cat_col, 'values': num_col}}
    return suggestions

def display_suggested_charts(df: pd.DataFrame, suggestions: Dict[str, Dict[str, Any]]):
    """Mostra i grafici suggeriti in un layout a tab."""
    if not suggestions: return
    tab_names = [cfg['title'] for cfg in suggestions.values()]
    tabs = st.tabs(tab_names)
    for tab, (key, config) in zip(tabs, suggestions.items()):
        with tab:
            if config['type'] == 'bar':
                fig = px.bar(df, x=config['config']['x'], y=config['config']['y'], title=config['title'])
            elif config['type'] == 'pie':
                fig = px.pie(df, names=config['config']['names'], values=config['config']['values'], title=config['title'])
            st.plotly_chart(fig, use_container_width=True)

### 6. FUNZIONE PRINCIPALE DELLA PAGINA
# ==============================================================================

def show_analytics_page():
    """
    Funzione principale per visualizzare la pagina di analisi.
    """
    dataset = st.session_state.get('last_query_result')
    if dataset is None or not isinstance(dataset, pd.DataFrame) or dataset.empty:
        st.warning("Esegui una query SQL per caricare un dataset valido.")
        return

    query_text = st.session_state.get('last_query_text', '')
    predefined_charts = find_predefined_config(query_text, 'charts')
    predefined_ml = find_predefined_config(query_text, 'ml_algorithms')
    
    if predefined_charts or predefined_ml:
        st.markdown("### 🎯 Analisi Predefinite")
        if predefined_charts:
            ChartVisualizer().display_charts_from_config(dataset, predefined_charts)
        if predefined_ml:
            ml_processor = MLProcessor(dataset)
            ml_visualizer = MLVisualizer(ml_processor)
            with st.spinner("Esecuzione algoritmi ML..."):
                ml_results = [ml_processor.execute_ml_config(cfg) for cfg in predefined_ml]
            ml_visualizer.display_ml_results(ml_results, predefined_ml)
        
        if not st.checkbox("Mostra anche analisi manuale/automatica", value=False):
            return
        st.divider()

    st.markdown("## 🔬 Analisi Dati")
    tab1, tab2 = st.tabs(["🤖 Analisi Automatica", "🔧 Analisi Manuale"])

    with tab1:
        group_by_col, agg_col, agg_type = find_best_columns_for_analysis(dataset)
        if group_by_col:
            st.info(f"Analisi automatica suggerita: Aggregazione di **{agg_col}** per **{group_by_col}**.")
            result_df = GeneralAnalytics(dataset).perform_aggregation(group_by_col, agg_col, agg_type)
            if result_df is not None and not result_df.empty:
                result_df = result_df.head(20)
                st.dataframe(result_df, use_container_width=True)
                suggestions = suggest_charts(result_df)
                display_suggested_charts(result_df, suggestions)
        else:
            st.warning("Non è stato possibile identificare colonne per un'analisi automatica.")

    with tab2:
        st.markdown("### ⚙️ Configura la Tua Analisi")
        cols = st.columns(3)
        group_by = cols[0].selectbox("Raggruppa per:", [c for c in dataset.columns if is_hashable(dataset[c])], key="manual_group")
        agg_col = cols[1].selectbox("Aggrega colonna:", ['count'] + list(dataset.select_dtypes(include=np.number).columns), key="manual_agg_col")
        agg_type = cols[2].selectbox("Tipo aggregazione:", ['sum', 'mean', 'max', 'min'] if agg_col != 'count' else ['count'], key="manual_agg_type")
        
        if st.button("🚀 Genera Analisi", type="primary", use_container_width=True):
            result_df = GeneralAnalytics(dataset).perform_aggregation(group_by, agg_col, agg_type)
            if result_df is not None and not result_df.empty:
                st.dataframe(result_df.head(20), use_container_width=True)
                suggestions = suggest_charts(result_df.head(20))
                display_suggested_charts(result_df.head(20), suggestions)
            else:
                st.error("Nessun risultato trovato.")