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
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, mean_squared_error, silhouette_score
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
        self.CHART_COLOR = '#89CFF0'#0DF9FF'#19D3F3'
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
            is_temporal_discrete = prepared_data['is_temporal_discrete']
            
            # Debug: mostra sempre i dati finali
            st.write("**Dati finali per il grafico:**")
            st.dataframe(df_final[[x_col_final, y_col_final]])
            
            # ========== CREAZIONE GRAFICO CON DATI UNIFICATI ==========
            fig = self._create_chart_by_type(df_final, chart_type, x_col_final, y_col_final, 
                                        x_col, is_temporal_discrete)
            
            if fig:
                fig.update_layout(height=500, template='plotly_white', 
                                margin=dict(t=60, b=60, l=60, r=60))
            
            return fig
            
        except Exception as e:
            st.error(f"Errore nella creazione del grafico {chart_type}: {e}")
            return None

    def _prepare_chart_data(self, df: pd.DataFrame, x_col: str, y_col: str, chart_type: str) -> Optional[Dict]:
        """Prepara i dati in modo centralizzato per tutti i tipi di grafico."""
        
        # ========== FASE 1: PULIZIA BASE ==========
        df_clean = df.copy()
        
        # Rimuovi righe con x_col nullo/NaN
        df_clean = df_clean.dropna(subset=[x_col])
        df_clean = df_clean[df_clean[x_col].notna()]
        
        if df_clean.empty:
            return None
        
        # ========== FASE 2: GESTIONE DATI TEMPORALI ==========
        is_temporal = pd.api.types.is_datetime64_any_dtype(df_clean[x_col])
        is_temporal_discrete = False
        
        if is_temporal and chart_type != 'serie temporale con picchi':
            st.info("Rilevata serie temporale, dati aggregati per giorno.")
            
            # Pulizia rigorosa delle date
            df_clean = df_clean[pd.to_datetime(df_clean[x_col], errors='coerce').notna()]
            df_clean[x_col] = pd.to_datetime(df_clean[x_col])
            
            if df_clean.empty:
                return None
            
            # Crea colonna data giornaliera
            df_clean['date_only'] = df_clean[x_col].dt.date
            
            # ========== AGGREGAZIONE UNIFICATA ==========
            if not y_col or y_col == '':
                # Caso 1: Solo conteggio delle occorrenze per data
                agg_df = df_clean.groupby('date_only').size().reset_index(name='Conteggio')
                y_col_final = 'Conteggio'
                
            elif pd.api.types.is_numeric_dtype(df_clean[y_col]):
                # Caso 2: Somma dei valori numerici per data
                df_clean = df_clean.dropna(subset=[y_col])
                agg_df = df_clean.groupby('date_only')[y_col].sum().reset_index()
                y_col_final = y_col
                
            else:
                # Caso 3: Conteggio per valori non numerici
                agg_df = df_clean.groupby('date_only').size().reset_index(name=f'Conteggio_{y_col}')
                y_col_final = f'Conteggio_{y_col}'
            
            # Converti date_only back a datetime
            agg_df['date_datetime'] = pd.to_datetime(agg_df['date_only'])
            
            # FILTRO CRITICO: Rimuovi valori nulli e zero
            agg_df = agg_df.dropna(subset=[y_col_final])
            agg_df = agg_df[agg_df[y_col_final] > 0]
            
            if agg_df.empty:
                return None
            
            # Crea colonna data come stringa per evitare spazi vuoti nei grafici
            agg_df['date_str'] = agg_df['date_datetime'].dt.strftime('%Y-%m-%d')
            
            df_final = agg_df.sort_values('date_datetime')[['date_str', y_col_final]].reset_index(drop=True)
            x_col_final = 'date_str'
            is_temporal_discrete = True
            
        else:
            # ========== GESTIONE DATI NON TEMPORALI ==========
            
            if not y_col or y_col == '':
                # Caso 1: Distribuzione di una sola variabile
                if df_clean[x_col].dtype == 'object':
                    # Variabile categorica: conta le occorrenze
                    counts = df_clean[x_col].value_counts().head(20)
                    counts = counts.dropna()
                    df_final = pd.DataFrame({
                        x_col: counts.index,
                        'Conteggio': counts.values
                    }).reset_index(drop=True)
                    x_col_final = x_col
                    y_col_final = 'Conteggio'
                else:
                    # Variabile numerica: usa i valori originali (limitati)
                    df_final = df_clean[[x_col]].head(100).reset_index(drop=True)
                    df_final['Valore'] = 1  # Per grafici che richiedono y
                    x_col_final = x_col
                    y_col_final = 'Valore'
                    
            else:
                # Caso 2: Relazione tra due variabili
                df_clean = df_clean.dropna(subset=[y_col])
                
                if df_clean[x_col].dtype == 'object' and pd.api.types.is_numeric_dtype(df_clean[y_col]):
                    # x categorica, y numerica: aggrega per categoria
                    agg_df = df_clean.groupby(x_col)[y_col].sum().reset_index()
                    agg_df = agg_df.sort_values(x_col, ascending=True).head(20)
                    df_final = agg_df.reset_index(drop=True)
                    x_col_final = x_col
                    y_col_final = y_col
                    
                else:
                    # Altri casi: usa i dati originali (limitati)
                    df_final = df_clean[[x_col, y_col]].head(500).reset_index(drop=True)
                    x_col_final = x_col
                    y_col_final = y_col
        
        return {
            'df': df_final,
            'x_col': x_col_final,
            'y_col': y_col_final,
            'is_temporal_discrete': is_temporal_discrete
        }

    def _create_chart_by_type(self, df: pd.DataFrame, chart_type: str, x_col: str, y_col: str, 
                            original_x_col: str, is_temporal_discrete: bool) -> Optional[go.Figure]:
        """Crea il grafico specifico usando dati già preparati e unificati."""
        
        fig = None
        
        if chart_type == 'barre':
            fig = go.Figure(data=[
                go.Bar(x=df[x_col], y=df[y_col], marker_color=self.CHART_COLOR, width=0.5 if len(x_col) < 5 else 1)
            ])
            fig.update_layout(
                title_text=f"Grafico a Barre - {y_col} per {original_x_col}",
                xaxis_title=original_x_col,
                yaxis_title=y_col
            )
            fig.update_xaxes(tickangle=0 if len(x_col) < 10 else 30, type='category')

            # fig = px.bar(df, x=x_col, y=y_col, 
            #             title=f"Grafico a Barre - {y_col} per {original_x_col}")
            # if is_temporal_discrete:
            #     fig.update_xaxes(title=original_x_col, tickangle=45, type='category')
        
        elif chart_type == 'linee':
            # fig = px.line(df, x=x_col, y=y_col, markers=True,
            #             title=f"Grafico a Linee - {y_col} rispetto a {original_x_col}")
            # if is_temporal_discrete:
            #     fig.update_xaxes(title=original_x_col, tickangle=45, type='category')
            fig = go.Figure(data=[
                go.Scatter(x=df[x_col], y=df[y_col], marker_color=self.CHART_COLOR, mode='lines+markers')
            ])
            fig.update_layout(
                title_text=f"Grafico a Linee - {y_col} rispetto a {original_x_col}",
                xaxis_title=original_x_col,
                yaxis_title=y_col
            )
            fig.update_xaxes(tickangle=0 if len(x_col) < 10 else 30, type='category')
        
        elif chart_type == 'serie temporale con picchi':
            # ECCEZIONE: per i picchi usa sempre i dati originali non aggregati
            # Questo richiede un handling speciale
            st.warning("Serie temporale con picchi richiede dati originali - implementazione speciale necessaria")
            return None
        
        elif chart_type == 'torta':
            # Prendi i top 10 valori per la torta
            # df_pie = df.nlargest(10, y_col) if len(df) > 10 else df
            # fig = px.pie(df_pie, values=y_col, names=x_col,
            #             title=f"Distribuzione - Top {len(df_pie)} {original_x_col}")
            df_pie = df.nlargest(10, y_col) if len(df) > 10 else df
            fig = go.Figure(data=[
                go.Pie(
                    labels=df_pie[x_col], 
                    values=df_pie[y_col],
                    marker_colors=px.colors.qualitative.Set3 
                )
            ])
            fig.update_layout(title_text=f"Distribuzione - Top {len(df_pie)} {original_x_col}")
        
        elif chart_type == 'heatmap':
            if is_temporal_discrete:
                st.warning("Heatmap per dati temporali aggregati non implementata")
                return None
            else:
                crosstab = pd.crosstab(df[y_col], df[x_col])
                fig = go.Figure(data=go.Heatmap(
                    z=crosstab.values,
                    x=crosstab.columns,
                    y=crosstab.index,
                    colorscale='Viridis' # Scegli una palette di colori
                ))
                fig.update_layout(
                    title_text=f"Heatmap: {y_col} vs {x_col}",
                    xaxis_title=x_col,
                    yaxis_title=y_col
                )
            # if is_temporal_discrete:
            #     st.warning("Heatmap per dati temporali aggregati non implementata - richiede logica speciale")
            #     return None
            # else:
            #     # Per heatmap, verifica se possiamo creare una matrice
            #     if len(df[x_col].unique()) <= 50 and len(df[y_col].unique()) <= 50:
            #         # Crea crosstab se i dati sono categorici o limitati
            #         if df[x_col].dtype == 'object' or df[y_col].dtype == 'object':
            #             crosstab = pd.crosstab(df[y_col], df[x_col])
            #             fig = px.imshow(crosstab, title=f"Heatmap: {y_col} vs {x_col}")
            #         else:
            #             # Correlazione per dati numerici
            #             corr_df = df[[x_col, y_col]].corr()
            #             fig = px.imshow(corr_df, title=f"Correlazione tra {x_col} e {y_col}")
            #     else:
            #         st.warning("Troppi valori unici per creare una heatmap significativa")
            #         return None
        return fig
    
    def display_charts_from_config(self, df: pd.DataFrame, charts_config: List[Dict[str, Any]]):
        """Visualizza tutti i grafici dalla configurazione JSON."""
        if not charts_config:
            st.info("Nessun grafico configurato per questa query")
            return
        
        if len(charts_config) == 1:
            chart_config = charts_config[0]
            chart_type = chart_config.get('type', 'Grafico')
            st.markdown(f"**{chart_type.title()}**")
            fig = self.create_chart(df, chart_config)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        else:
            tab_names = []
            for i, config in enumerate(charts_config):
                chart_type = config.get('type', f'Grafico {i+1}')
                icon_map = {'barre': '📊', 'linee': '📈', 'torta': '🥧', 'heatmap': '🔥', 'serie temporale con picchi': '⚡'}
                icon = icon_map.get(chart_type.lower(), '📈')
                tab_names.append(f"{icon} {chart_type.title()}")
            
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
    Classe per gestire l'esecuzione di algoritmi ML predefiniti dai metadati JSON.
    """
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe
        self.scaler = StandardScaler()
        
    def validate_ml_config(self, ml_config: Dict[str, Any]) -> Tuple[bool, str]:
        """Valida la configurazione ML."""
        if not isinstance(ml_config, dict):
            return False, "Configurazione ML non valida"
        
        algorithm = ml_config.get('algorithm')
        features = ml_config.get('features', [])
        target = ml_config.get('target')
        
        if not algorithm:
            return False, "Algoritmo non specificato"
        
        if algorithm in ['STL Decomposition', 'Prophet']:
            datetime_cols = self._find_datetime_columns()
            if not datetime_cols and not features:
                return False, f"{algorithm} richiede almeno una colonna datetime"
        elif not features:
            return False, "Features non specificate"
        
        if features:
            missing_features = [f for f in features if f not in self.df.columns]
            if missing_features:
                return False, f"Features mancanti: {missing_features}"
        
        supervised_algorithms = ['Random Forest', 'XGBoost', 'Linear Regression', 'Logistic Regression', 'SVM']
        if algorithm in supervised_algorithms:
            if not target or target not in self.df.columns:
                return False, f"Target '{target}' richiesto per {algorithm}"
        
        return True, ""
    
    def _find_datetime_columns(self) -> List[str]:
        """Trova colonne datetime nel DataFrame."""
        datetime_cols = []
        for col in self.df.columns:
            if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                datetime_cols.append(col)
            elif self.df[col].dtype == 'object':
                try:
                    pd.to_datetime(self.df[col].iloc[:100], errors='raise')
                    datetime_cols.append(col)
                except (ValueError, TypeError):
                    pass
        return datetime_cols
    
    def _prepare_time_series_data(self, features: List[str] = None) -> Tuple[pd.DataFrame, str, str]:
        """Prepara dati per analisi serie temporali."""
        datetime_cols = self._find_datetime_columns()
        if not datetime_cols:
            raise ValueError("Nessuna colonna datetime trovata per l'analisi temporale")
        
        date_col = datetime_cols[0]
        df_ts = self.df.copy()
        df_ts[date_col] = pd.to_datetime(df_ts[date_col])
        
        if features:
            value_col = features[0]
        else:
            numeric_cols = [col for col in df_ts.select_dtypes(include=np.number).columns if col != date_col]
            if not numeric_cols:
                raise ValueError("Nessuna colonna numerica trovata per l'analisi temporale")
            value_col = numeric_cols[0]
        
        df_ts = df_ts.sort_values(date_col).dropna(subset=[date_col, value_col])
        return df_ts, date_col, value_col
    
    def _run_isolation_forest(self, features: List[str], params: Dict = None) -> Dict[str, Any]:
        """Esegue Isolation Forest per anomaly detection."""
        if not features:
            features = list(self.df.select_dtypes(include=[np.number]).columns)
        if not features:
            return {'error': "Nessuna feature numerica trovata per Isolation Forest"}
        
        df_clean = self.df[features].dropna()
        if df_clean.empty:
            return {'error': "Nessun dato valido dopo la pulizia"}
        
        X_scaled = self.scaler.fit_transform(df_clean)
        contamination = params.get('contamination', 0.1) if params else 0.1
        
        iso_forest = IsolationForest(contamination=contamination, random_state=42)
        anomaly_labels = iso_forest.fit_predict(X_scaled)
        
        is_anomaly = anomaly_labels == -1
        return {
            'algorithm': 'Isolation Forest', 'is_anomaly': is_anomaly, 'n_anomalies': sum(is_anomaly),
            'features': features, 'data_clean': df_clean, 'model': iso_forest
        }
    
    def _run_stl_decomposition(self, features: List[str] = None, params: Dict = None) -> Dict[str, Any]:
        """Esegue STL Decomposition per rilevare anomalie temporali."""
        df_ts, date_col, value_col = self._prepare_time_series_data(features)
        period = params.get('period', 7) if params else 7
        df_ts = df_ts.set_index(date_col)
        
        if len(df_ts) < period * 2:
            period = max(2, len(df_ts) // 3)
        
        stl = STL(df_ts[value_col], period=period)
        decomposition = stl.fit()
        
        residuals = decomposition.resid
        threshold = params.get('threshold', 3) if params else 3
        is_anomaly = np.abs(residuals) > threshold * residuals.std()
        
        return {
            'algorithm': 'STL Decomposition', 'decomposition': decomposition, 'is_anomaly': is_anomaly,
            'n_anomalies': sum(is_anomaly), 'date_col': date_col, 'value_col': value_col, 'data': df_ts
        }
    
    def _run_prophet_anomaly(self, features: List[str] = None, params: Dict = None) -> Dict[str, Any]:
        """Esegue Prophet per anomaly detection su serie temporali."""
        if not PROPHET_AVAILABLE:
            return {'error': "Prophet non è installato. Eseguire: pip install prophet"}
        
        df_ts, date_col, value_col = self._prepare_time_series_data(features)
        prophet_df = pd.DataFrame({'ds': df_ts[date_col], 'y': df_ts[value_col]}).dropna()
        
        if len(prophet_df) < 10:
            return {'error': "Dati insufficienti per Prophet (minimo 10 punti)"}
        
        model = Prophet(interval_width=params.get('interval_width', 0.95) if params else 0.95)
        with st.spinner("Addestramento modello Prophet..."):
            model.fit(prophet_df)
        
        forecast = model.predict(prophet_df)
        is_anomaly = (prophet_df['y'] < forecast['yhat_lower']) | (prophet_df['y'] > forecast['yhat_upper'])
        
        return {
            'algorithm': 'Prophet', 'model': model, 'forecast': forecast, 'original_data': prophet_df,
            'is_anomaly': is_anomaly, 'n_anomalies': sum(is_anomaly)
        }

    def run_anomaly_detection(self, algorithm: str, features: List[str] = None, params: Dict = None) -> Dict[str, Any]:
        """Esegue algoritmi di anomaly detection."""
        try:
            if algorithm == "Isolation Forest":
                return self._run_isolation_forest(features, params)
            elif algorithm == "STL Decomposition":
                return self._run_stl_decomposition(features, params)
            elif algorithm == "Prophet":
                return self._run_prophet_anomaly(features, params)
            else:
                return {'error': f"Algoritmo di anomaly detection {algorithm} non supportato"}
        except Exception as e:
            return {'error': f"Errore in {algorithm}: {str(e)}"}

    def prepare_data(self, features: List[str], target: Optional[str] = None) -> Tuple[np.ndarray, Optional[np.ndarray]]:
        """Prepara i dati per l'algoritmo ML."""
        df_clean = self.df[features + ([target] if target else [])].dropna()
        X = df_clean[features].select_dtypes(include=[np.number])
        if X.empty:
            raise ValueError("Nessuna feature numerica trovata")
        
        X_scaled = self.scaler.fit_transform(X)
        y = df_clean[target] if target else None
        if y is not None and y.dtype == 'object':
            y = pd.Categorical(y).codes
        return X_scaled, y
    
    def run_clustering(self, algorithm: str, X: np.ndarray, params: Dict = None) -> Dict[str, Any]:
        """Esegue algoritmi di clustering."""
        try:
            if algorithm == "K-Means":
                optimal_k = params.get('n_clusters', 3) if params else 3
                model = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
                labels = model.fit_predict(X)
                return {
                    'algorithm': 'K-Means', 'labels': labels, 'n_clusters': optimal_k,
                    'silhouette_score': silhouette_score(X, labels) if len(set(labels)) > 1 else 0, 'model': model
                }
            elif algorithm == "DBSCAN":
                model = DBSCAN(eps=params.get('eps', 0.5), min_samples=params.get('min_samples', 5))
                labels = model.fit_predict(X)
                n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
                return {
                    'algorithm': 'DBSCAN', 'labels': labels, 'n_clusters': n_clusters, 'n_noise': list(labels).count(-1),
                    'silhouette_score': silhouette_score(X, labels) if n_clusters > 1 else 0, 'model': model
                }
        except Exception as e:
            return {'error': f"Errore in {algorithm}: {str(e)}"}
        return {}

    def run_supervised(self, algorithm: str, X: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
        """Esegue algoritmi supervisionati."""
        try:
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            is_classification = len(np.unique(y)) < 10
            
            model_map = {
                "Random Forest": RandomForestClassifier if is_classification else RandomForestRegressor,
                "XGBoost": xgb.XGBClassifier if is_classification else xgb.XGBRegressor,
                "Linear Regression": LinearRegression,
                "Logistic Regression": LogisticRegression,
                "SVM": SVC if is_classification else SVR
            }
            model = model_map[algorithm](random_state=42) if "Regression" not in algorithm else model_map[algorithm]()
            
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            
            metric_name = 'accuracy' if is_classification else 'mse'
            score_func = accuracy_score if is_classification else mean_squared_error
            score = score_func(y_test, y_pred)
            
            return {
                'algorithm': algorithm, 'model': model, 'predictions': y_pred,
                'test_actual': y_test, metric_name: score, 'is_classification': is_classification
            }
        except Exception as e:
            return {'error': f"Errore in {algorithm}: {str(e)}"}
        return {}
    
    def run_dimensionality_reduction(self, algorithm: str, X: np.ndarray, params: Dict = None) -> Dict[str, Any]:
        """Esegue algoritmi di riduzione dimensionalità."""
        try:
            if algorithm == "PCA":
                n_components = params.get('n_components', 2) if params else 2
                n_components = min(n_components, X.shape[1], X.shape[0])
                model = PCA(n_components=n_components)
                X_transformed = model.fit_transform(X)
                return {
                    'algorithm': 'PCA', 'transformed_data': X_transformed,
                    'explained_variance_ratio': model.explained_variance_ratio_,
                    'total_explained_variance': sum(model.explained_variance_ratio_),
                    'model': model, 'n_components': n_components
                }
        except Exception as e:
            return {'error': f"Errore in {algorithm}: {str(e)}"}
        return {}

    def execute_ml_config(self, ml_config: Dict[str, Any]) -> Dict[str, Any]:
        """Esegue una configurazione ML completa."""
        is_valid, error_msg = self.validate_ml_config(ml_config)
        if not is_valid:
            return {'error': error_msg}
        
        algorithm = ml_config['algorithm']
        features = ml_config.get('features', [])
        target = ml_config.get('target')
        params = ml_config.get('params', {})
        
        try:
            alg_map = {
                'K-Means': ('clustering', True), 'DBSCAN': ('clustering', True),
                'Random Forest': ('supervised', True), 'XGBoost': ('supervised', True),
                'Linear Regression': ('supervised', True), 'Logistic Regression': ('supervised', True),
                'SVM': ('supervised', True), 'PCA': ('dimensionality', True),
                'Isolation Forest': ('anomaly', False), 'STL Decomposition': ('anomaly', False),
                'Prophet': ('anomaly', False)
            }
            
            alg_type, needs_prepare = alg_map.get(algorithm, (None, False))

            if alg_type == 'anomaly':
                return self.run_anomaly_detection(algorithm, features, params)
            
            if needs_prepare:
                X, y = self.prepare_data(features, target)
                if alg_type == 'clustering':
                    return self.run_clustering(algorithm, X, params)
                if alg_type == 'supervised':
                    return self.run_supervised(algorithm, X, y)
                if alg_type == 'dimensionality':
                    return self.run_dimensionality_reduction(algorithm, X, params)
            
            return {'error': f"Algoritmo {algorithm} non supportato"}

        except Exception as e:
            return {'error': f"Errore nell'esecuzione di {algorithm}: {str(e)}"}

### 4. CLASSE PER VISUALIZZAZIONE ML
# ==============================================================================

class MLVisualizer:
    """
    Classe per visualizzare i risultati degli algoritmi ML.
    """
    def __init__(self, ml_processor: MLProcessor):
        self.ml_processor = ml_processor

    def visualize_clustering_results(self, results: Dict[str, Any], features: List[str]):
        """Visualizza risultati di clustering."""
        if 'error' in results:
            st.error(results['error'])
            return
        
        st.success(f"✅ {results['algorithm']} completato!")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Numero di cluster", results.get('n_clusters', 'N/A'))
            if 'silhouette_score' in results:
                st.metric("Silhouette Score", f"{results['silhouette_score']:.3f}")
        
        # Se abbiamo 2+ features, mostra scatter plot
        if len(features) >= 2:
            df_clean = self.ml_processor.df[features].dropna()
            X_vis = self.ml_processor.scaler.transform(df_clean.select_dtypes(include=[np.number]))
            
            if len(X_vis) == len(results['labels']):
                df_plot = pd.DataFrame({'x': X_vis[:, 0], 'y': X_vis[:, 1], 'Cluster': results['labels']})
                fig = px.scatter(df_plot, x='x', y='y', color='Cluster', color_continuous_scale='viridis',
                               title=f"Visualizzazione Cluster - {results['algorithm']}", labels={'x': features[0], 'y': features[1]})
                st.plotly_chart(fig, use_container_width=True)
    
    def visualize_supervised_results(self, results: Dict[str, Any]):
        """Visualizza risultati di algoritmi supervisionati."""
        if 'error' in results:
            st.error(results['error'])
            return
        
        st.success(f"✅ {results['algorithm']} completato!")
        is_classification = results.get('is_classification', False)
        metric_name = 'Accuracy' if is_classification else 'Mean Squared Error'
        metric_val = results.get('accuracy') if is_classification else results.get('mse')
        st.metric(metric_name, f"{metric_val:.3f}")

        y_test, y_pred = results['test_actual'], results['predictions']
        if is_classification:
            conf_matrix = pd.crosstab(pd.Series(y_test, name='Actual'), pd.Series(y_pred, name='Predicted'))
            fig = px.imshow(conf_matrix, title="Matrice di Confusione", text_auto=True)
        else:
            fig = px.scatter(x=y_test, y=y_pred, title="Predizioni vs Valori Reali", labels={'x': 'Valori Reali', 'y': 'Valori Predetti'})
            fig.add_shape(type="line", x0=min(y_test), y0=min(y_test), x1=max(y_test), y1=max(y_test), line=dict(dash="dash"))
        st.plotly_chart(fig, use_container_width=True)
    
    def visualize_pca_results(self, results: Dict[str, Any]):
        """Visualizza risultati PCA."""
        if 'error' in results:
            st.error(results['error'])
            return
        
        st.success("✅ PCA completata!")
        st.metric("Varianza Spiegata Totale", f"{results['total_explained_variance']:.3f}")

        # Grafico della varianza spiegata
        explained_var = results['explained_variance_ratio']
        fig = px.bar(x=[f"PC{i+1}" for i in range(len(explained_var))], y=explained_var, title="Varianza Spiegata per Componente")
        st.plotly_chart(fig, use_container_width=True)
    
    def display_ml_results(self, ml_results: List[Dict[str, Any]], ml_configs: List[Dict[str, Any]]):
        """Visualizza tutti i risultati ML."""
        if not ml_results:
            st.info("Nessun algoritmo ML configurato")
            return

        st.divider()
        st.markdown("### 🤖 Risultati Machine Learning")

        # Crea una tab per ogni configurazione
        tab_names = [cfg.get('algorithm', 'Sconosciuto') for cfg in ml_configs]
        tabs = st.tabs(tab_names)

        # Mostra i risultati dentro ogni tab
        for tab, result, config in zip(tabs, ml_results, ml_configs):
            with tab:
                algorithm = config.get('algorithm', 'N/A')

                if algorithm in ['K-Means', 'DBSCAN']:
                    self.visualize_clustering_results(result, config.get('features', []))
                elif algorithm in ['Random Forest', 'XGBoost', 'Linear Regression', 'Logistic Regression', 'SVM']:
                    self.visualize_supervised_results(result)
                elif algorithm == 'PCA':
                    self.visualize_pca_results(result)
                elif 'error' in result:
                    st.error(result['error'])


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
                        configs = query_data.get(config_key, [])
                        if configs:
                            st.info(f"{config_key.replace('_', ' ').title()} caricati da: {name} ({category})")
                        return configs
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
    best_group_by = None
    
    suitable_candidates = {col: df[col].nunique() for col in categorical_cols if is_hashable(df[col]) and 3 < df[col].nunique() < 50}
    if suitable_candidates:
        best_group_by = min(suitable_candidates, key=suitable_candidates.get)
    if not best_group_by:
        return None, None, 'count'

    possible_agg_cols = [col for col in numerical_cols if col != best_group_by]
    if possible_agg_cols:
        return best_group_by, possible_agg_cols[0], 'sum'
    return best_group_by, 'count', 'count'

def suggest_charts(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """Suggerisce grafici appropriati per un DataFrame aggregato."""
    suggestions = {}
    if df is None or df.empty or len(df.columns) < 2: return suggestions
    cat_col, num_col = df.columns[0], df.columns[1]

    suggestions['horizontal_bar'] = {'type': 'horizontal_bar', 'title': f'Classifica per {cat_col}', 'config': {'x_col': cat_col, 'y_col': num_col}}
    if df[cat_col].nunique() <= 10 and df[num_col].min() >= 0:
        suggestions['pie_chart'] = {'type': 'pie', 'title': f'Distribuzione per {cat_col}', 'config': {'labels_col': cat_col, 'values_col': num_col}}
    return suggestions

def _create_and_display_chart(df: pd.DataFrame, config: Dict[str, Any]):
    """Crea e visualizza un singolo grafico Plotly."""
    try:
        st.markdown(f"**{config['title']}**")
        fig = None
        if config['type'] == 'horizontal_bar':
            cfg = config['config']
            fig = px.bar(df, x=cfg['y_col'], y=cfg['x_col'], orientation='h', title=config['title'], color=cfg['y_col'])
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        elif config['type'] == 'pie':
            cfg = config['config']
            fig = px.pie(df, names=cfg['labels_col'], values=cfg['values_col'], title=config['title'])
        if fig:
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Errore nella creazione del grafico '{config.get('title', 'N/A')}': {e}")

def display_suggested_charts(df: pd.DataFrame, suggestions: Dict[str, Dict[str, Any]]):
    """Mostra i grafici suggeriti in un layout a tab."""
    if not suggestions: return
    tab_names = [cfg['type'].replace('_', ' ').title() for cfg in suggestions.values()]
    tabs = st.tabs(tab_names)
    for tab, config in zip(tabs, suggestions.values()):
        with tab:
            _create_and_display_chart(df, config)

def execute_smart_aggregation(analytics: GeneralAnalytics, params: Tuple, options: Tuple):
    """Esegue l'aggregazione richiesta e mostra i risultati."""
    group_by, agg_col, agg_type = params
    limit, sort_desc = options
    with st.spinner("Elaborazione analisi..."):
        result = analytics.perform_aggregation(group_by, agg_col, agg_type, sort_desc)
        if result is not None and not result.empty:
            st.dataframe(result.head(limit), use_container_width=True)
            suggestions = suggest_charts(result.head(limit))
            display_suggested_charts(result.head(limit), suggestions)
        else:
            st.error("Nessun risultato trovato per l'aggregazione specificata.")

### 6. FUNZIONE PRINCIPALE DELLA PAGINA
# ==============================================================================

def show_analytics_page():
    """
    Funzione principale per visualizzare la pagina di analisi.
    """
    dataset = st.session_state.get('last_query_result')
    if dataset is None:
        st.warning("Esegui una query SQL per caricare un dataset.")
        return
    if not isinstance(dataset, pd.DataFrame):
        try:
            dataset = dataset.toPandas()
        except Exception as e:
            st.error(f"Errore nella conversione del dataset: {e}")
            return
    if dataset.empty:
        st.warning("Il dataset è vuoto.")
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
        
        if not st.checkbox("Mostra anche analisi automatica/manuale", value=False):
            return
        st.markdown("---")

    st.markdown("## 🔬 Analisi Dati")
    tab1, tab2 = st.tabs(["🤖 Analisi Automatica", "🔧 Analisi Manuale"])

    with tab1:
        general_analytics = GeneralAnalytics(dataset)
        group_by_col, agg_col, agg_type = find_best_columns_for_analysis(dataset)
        if group_by_col:
            st.info(f"Analisi automatica suggerita: Aggregazione di **{agg_col}** per **{group_by_col}**.")
            result_df = general_analytics.perform_aggregation(group_by_col, agg_col, agg_type, sort_desc=True)
            if result_df is not None and not result_df.empty:
                result_df = result_df.head(20)
                st.dataframe(result_df, use_container_width=True)
                suggestions = suggest_charts(result_df)
                display_suggested_charts(result_df, suggestions)
        else:
            st.warning("Non è stato possibile identificare una combinazione di colonne per un'analisi automatica.")

    with tab2:
        st.markdown("### ⚙️ Configura la Tua Analisi")
        cols = st.columns(3)
        group_by = cols[0].selectbox("Raggruppa per:", [c for c in dataset.columns if is_hashable(dataset[c])])
        agg_col = cols[1].selectbox("Aggrega colonna:", list(dataset.select_dtypes(include=np.number).columns) + ["count"])
        agg_type = cols[2].selectbox("Tipo aggregazione:", ['sum', 'mean', 'max', 'min'] if agg_col != 'count' else ['count'])
        
        if st.button("🚀 Genera Analisi", type="primary", use_container_width=True):
            execute_smart_aggregation(GeneralAnalytics(dataset), (group_by, agg_col, agg_type), (20, True))