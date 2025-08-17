"""
Modulo per la creazione di grafici e visualizzazioni
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import streamlit as st
from typing import Optional, Dict, List, Any
import logging

from .config import Config

logger = logging.getLogger(__name__)

class DisasterVisualizations:
    """Classe per visualizzazioni specifiche sui disastri naturali"""
    
    def __init__(self):
        self.color_palette = Config.COLOR_PALETTE
        self.chart_config = Config.CHART_CONFIG
    
    def create_disaster_type_chart(self, data: pd.DataFrame, type_col: str = None, count_col: str = None) -> go.Figure:
        """
        Crea un grafico a barre per i tipi di disastro
        
        Args:
            data (pd.DataFrame): Dati da visualizzare
            type_col (str): Nome colonna tipo di disastro
            count_col (str): Nome colonna conteggio
        
        Returns:
            go.Figure: Grafico Plotly
        """
        try:
            if data.empty:
                return self._create_empty_chart("Nessun dato disponibile")
            
            # Auto-rileva colonne se non specificate
            if type_col is None:
                type_col = data.columns[0]
            if count_col is None:
                count_col = data.columns[1] if len(data.columns) > 1 else 'count'
            
            fig = px.bar(
                data, 
                x=type_col, 
                y=count_col,
                title="Distribuzione per Tipo di Disastro",
                color=count_col,
                color_continuous_scale='Viridis'
            )
            
            fig.update_layout(
                xaxis_tickangle=45,
                height=self.chart_config['height'],
                showlegend=False
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione del grafico tipi disastro: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def create_geographical_map(self, data: pd.DataFrame, country_col: str = None, count_col: str = None) -> go.Figure:
        """
        Crea una mappa geografica della distribuzione dei disastri
        
        Args:
            data (pd.DataFrame): Dati da visualizzare
            country_col (str): Nome colonna paese
            count_col (str): Nome colonna conteggio
        
        Returns:
            go.Figure: Mappa Plotly
        """
        try:
            if data.empty:
                return self._create_empty_chart("Nessun dato geografico disponibile")
            
            # Auto-rileva colonne se non specificate
            if country_col is None:
                country_col = data.columns[0]
            if count_col is None:
                count_col = data.columns[1] if len(data.columns) > 1 else 'disaster_count'
            
            fig = px.choropleth(
                data,
                locations=country_col,
                color=count_col,
                hover_name=country_col,
                hover_data=[count_col],
                color_continuous_scale='Reds',
                title="Distribuzione Geografica dei Disastri Naturali",
                locationmode='country names'
            )
            
            fig.update_layout(
                height=self.chart_config['height'],
                geo=dict(showframe=False, showcoastlines=True)
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione della mappa: {str(e)}")
            # Fallback a grafico a barre
            return self.create_horizontal_bar_chart(data, country_col, count_col, "Top Paesi per Numero di Disastri")
    
    def create_temporal_trend(self, data: pd.DataFrame, time_col: str = None, count_col: str = None) -> go.Figure:
        """
        Crea un grafico temporale dei trend
        
        Args:
            data (pd.DataFrame): Dati da visualizzare
            time_col (str): Nome colonna temporale
            count_col (str): Nome colonna conteggio
        
        Returns:
            go.Figure: Grafico temporale Plotly
        """
        try:
            if data.empty:
                return self._create_empty_chart("Nessun dato temporale disponibile")
            
            # Auto-rileva colonne se non specificate
            if time_col is None:
                time_col = data.columns[0]
            if count_col is None:
                count_col = data.columns[1] if len(data.columns) > 1 else 'disaster_count'
            
            fig = px.line(
                data,
                x=time_col,
                y=count_col,
                title="Trend Temporale dei Disastri Naturali",
                markers=True
            )
            
            # Aggiungi linea di tendenza
            fig.add_scatter(
                x=data[time_col],
                y=data[count_col],
                mode='lines',
                name='Trend',
                line=dict(dash='dash', color='red')
            )
            
            fig.update_layout(
                height=self.chart_config['height'],
                xaxis_title=time_col.title(),
                yaxis_title="Numero di Disastri"
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione del trend temporale: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def create_severity_analysis(self, data: Dict[str, pd.DataFrame]) -> go.Figure:
        """
        Crea visualizzazioni per l'analisi di severità
        
        Args:
            data (Dict[str, pd.DataFrame]): Dati di impatto organizzati per metrica
        
        Returns:
            go.Figure: Grafico combinato
        """
        try:
            if not data:
                return self._create_empty_chart("Nessun dato di impatto disponibile")
            
            # Crea subplot per diverse metriche
            metrics = list(data.keys())
            n_metrics = len(metrics)
            
            if n_metrics == 1:
                fig = make_subplots(rows=1, cols=1, subplot_titles=metrics)
            else:
                cols = min(2, n_metrics)
                rows = (n_metrics + 1) // 2
                fig = make_subplots(
                    rows=rows, cols=cols,
                    subplot_titles=[metric.replace('_', ' ').title() for metric in metrics]
                )
            
            for i, (metric, df) in enumerate(data.items()):
                if df.empty:
                    continue
                
                row = (i // 2) + 1 if n_metrics > 1 else 1
                col = (i % 2) + 1 if n_metrics > 1 else 1
                
                # Prendi le prime colonne come x e y
                x_col, y_col = df.columns[0], df.columns[1]
                
                fig.add_trace(
                    go.Bar(x=df[x_col], y=df[y_col], name=metric),
                    row=row, col=col
                )
            
            fig.update_layout(
                height=self.chart_config['height'] * (1 if n_metrics <= 2 else 1.5),
                title_text="Analisi di Impatto e Severità",
                showlegend=False
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione dell'analisi di severità: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def create_correlation_heatmap(self, correlation_matrix: pd.DataFrame) -> go.Figure:
        """
        Crea una heatmap delle correlazioni
        
        Args:
            correlation_matrix (pd.DataFrame): Matrice di correlazione
        
        Returns:
            go.Figure: Heatmap Plotly
        """
        try:
            if correlation_matrix.empty:
                return self._create_empty_chart("Nessuna correlazione calcolabile")
            
            fig = px.imshow(
                correlation_matrix,
                title="Matrice di Correlazione",
                color_continuous_scale='RdBu',
                aspect='auto'
            )
            
            fig.update_layout(
                height=self.chart_config['height']
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione della heatmap: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")

class GeneralVisualizations:
    """Classe per visualizzazioni generali"""
    
    def __init__(self):
        self.color_palette = Config.COLOR_PALETTE
        self.chart_config = Config.CHART_CONFIG
    
    def create_horizontal_bar_chart(self, data: pd.DataFrame, x_col: str, y_col: str, title: str) -> go.Figure:
        """Crea un grafico a barre orizzontale"""
        try:
            if data.empty:
                return self._create_empty_chart("Nessun dato disponibile")
            
            # Ordina i dati per il grafico
            data_sorted = data.nlargest(20, y_col) if len(data) > 20 else data
            
            fig = px.bar(
                data_sorted,
                x=y_col,
                y=x_col,
                orientation='h',
                title=title,
                color=y_col,
                color_continuous_scale='Viridis'
            )
            
            fig.update_layout(
                height=max(400, len(data_sorted) * 25),
                yaxis={'categoryorder': 'total ascending'}
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione del grafico a barre: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def create_pie_chart(self, data: pd.DataFrame, labels_col: str, values_col: str, title: str) -> go.Figure:
        """Crea un grafico a torta"""
        try:
            if data.empty:
                return self._create_empty_chart("Nessun dato disponibile")
            
            # Prendi i top 10 per leggibilità
            data_top = data.nlargest(10, values_col)
            
            fig = px.pie(
                data_top,
                values=values_col,
                names=labels_col,
                title=title,
                color_discrete_sequence=self.color_palette
            )
            
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=self.chart_config['height'])
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione del grafico a torta: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def create_scatter_plot(self, data: pd.DataFrame, x_col: str, y_col: str, title: str, color_col: str = None) -> go.Figure:
        """Crea un grafico di dispersione"""
        try:
            if data.empty:
                return self._create_empty_chart("Nessun dato disponibile")
            
            fig = px.scatter(
                data,
                x=x_col,
                y=y_col,
                title=title,
                color=color_col,
                color_continuous_scale='Viridis' if color_col else None
            )
            
            # Aggiungi linea di tendenza se i dati sono numerici
            try:
                fig.add_scatter(
                    x=data[x_col],
                    y=data[y_col],
                    mode='lines',
                    name='Tendenza',
                    line=dict(dash='dash')
                )
            except:
                pass  # Non aggiungere se non possibile
            
            fig.update_layout(height=self.chart_config['height'])
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione dello scatter plot: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def create_histogram(self, data: pd.DataFrame, col: str, title: str, bins: int = 30) -> go.Figure:
        """Crea un istogramma"""
        try:
            if data.empty or col not in data.columns:
                return self._create_empty_chart("Nessun dato disponibile")
            
            fig = px.histogram(
                data,
                x=col,
                title=title,
                nbins=bins,
                color_discrete_sequence=[self.color_palette[0]]
            )
            
            fig.update_layout(height=self.chart_config['height'])
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione dell'istogramma: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def create_box_plot(self, data: pd.DataFrame, y_col: str, x_col: str = None, title: str = None) -> go.Figure:
        """Crea un box plot"""
        try:
            if data.empty:
                return self._create_empty_chart("Nessun dato disponibile")
            
            if x_col:
                fig = px.box(data, x=x_col, y=y_col, title=title or f"Box Plot di {y_col} per {x_col}")
            else:
                fig = px.box(data, y=y_col, title=title or f"Box Plot di {y_col}")
            
            fig.update_layout(height=self.chart_config['height'])
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione del box plot: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def _create_empty_chart(self, message: str) -> go.Figure:
        """Crea un grafico vuoto con un messaggio"""
        fig = go.Figure()
        fig.add_annotation(
            text=message,
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            xanchor='center', yanchor='middle',
            showarrow=False,
            font=dict(size=16)
        )
        fig.update_layout(
            height=self.chart_config['height'],
            xaxis={'visible': False},
            yaxis={'visible': False}
        )
        return fig

class QualityVisualizations:
    """Classe per visualizzazioni sulla qualità dei dati"""
    
    def __init__(self):
        self.color_palette = Config.COLOR_PALETTE
        self.chart_config = Config.CHART_CONFIG
    
    def create_completeness_chart(self, completeness_data: Dict[str, Dict]) -> go.Figure:
        """
        Crea un grafico sulla completezza dei dati
        
        Args:
            completeness_data (Dict): Dati di completezza per colonna
        
        Returns:
            go.Figure: Grafico completezza
        """
        try:
            if not completeness_data:
                return self._create_empty_chart("Nessun dato di completezza")
            
            columns = list(completeness_data.keys())
            complete_percentages = [data['complete_percentage'] for data in completeness_data.values()]
            null_percentages = [data['null_percentage'] for data in completeness_data.values()]
            
            fig = go.Figure()
            
            # Barre per completezza
            fig.add_trace(go.Bar(
                name='Completo',
                x=columns,
                y=complete_percentages,
                marker_color='lightgreen'
            ))
            
            # Barre per valori mancanti
            fig.add_trace(go.Bar(
                name='Mancante',
                x=columns,
                y=null_percentages,
                marker_color='lightcoral'
            ))
            
            fig.update_layout(
                barmode='stack',
                title='Completezza dei Dati per Colonna',
                xaxis_title='Colonne',
                yaxis_title='Percentuale (%)',
                height=self.chart_config['height'],
                xaxis_tickangle=45
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione del grafico di completezza: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def create_quality_score_gauge(self, quality_score: float) -> go.Figure:
        """
        Crea un indicatore circolare per il punteggio qualità
        
        Args:
            quality_score (float): Punteggio qualità (0-100)
        
        Returns:
            go.Figure: Gauge chart
        """
        try:
            # Determina colore in base al punteggio
            if quality_score >= 80:
                color = "green"
            elif quality_score >= 60:
                color = "yellow"
            else:
                color = "red"
            
            fig = go.Figure(go.Indicator(
                mode = "gauge+number+delta",
                value = quality_score,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "Punteggio Qualità Dataset"},
                delta = {'reference': 80},
                gauge = {
                    'axis': {'range': [None, 100]},
                    'bar': {'color': color},
                    'steps': [
                        {'range': [0, 50], 'color': "lightgray"},
                        {'range': [50, 80], 'color': "gray"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 90
                    }
                }
            ))
            
            fig.update_layout(height=400)
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione del gauge: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def create_outliers_visualization(self, data: pd.DataFrame, column: str) -> go.Figure:
        """
        Crea visualizzazione per gli outlier
        
        Args:
            data (pd.DataFrame): Dati con outlier
            column (str): Nome della colonna
        
        Returns:
            go.Figure: Box plot con outlier evidenziati
        """
        try:
            if data.empty:
                return self._create_empty_chart("Nessun outlier trovato")
            
            fig = px.box(
                data,
                y=column,
                title=f"Outlier nella colonna '{column}'",
                points="outliers"  # Mostra solo gli outlier come punti
            )
            
            fig.update_layout(height=self.chart_config['height'])
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella visualizzazione outlier: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def _create_empty_chart(self, message: str) -> go.Figure:
        """Crea un grafico vuoto con un messaggio"""
        fig = go.Figure()
        fig.add_annotation(
            text=message,
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            xanchor='center', yanchor='middle',
            showarrow=False,
            font=dict(size=16)
        )
        fig.update_layout(
            height=self.chart_config['height'],
            xaxis={'visible': False},
            yaxis={'visible': False}
        )
        return fig

class InteractiveVisualizations:
    """Classe per visualizzazioni interattive avanzate"""
    
    def __init__(self):
        self.color_palette = Config.COLOR_PALETTE
        self.chart_config = Config.CHART_CONFIG
    
    def create_multi_metric_dashboard(self, data: pd.DataFrame, metrics: List[str]) -> go.Figure:
        """
        Crea un dashboard multi-metrica
        
        Args:
            data (pd.DataFrame): Dati da visualizzare
            metrics (List[str]): Lista delle metriche da includere
        
        Returns:
            go.Figure: Dashboard interattivo
        """
        try:
            if data.empty or not metrics:
                return self._create_empty_chart("Nessun dato per il dashboard")
            
            # Calcola layout subplot
            n_metrics = len(metrics)
            cols = min(2, n_metrics)
            rows = (n_metrics + 1) // 2
            
            subplot_titles = [metric.replace('_', ' ').title() for metric in metrics]
            fig = make_subplots(
                rows=rows, 
                cols=cols,
                subplot_titles=subplot_titles,
                vertical_spacing=0.1,
                horizontal_spacing=0.1
            )
            
            for i, metric in enumerate(metrics):
                if metric not in data.columns:
                    continue
                
                row = (i // 2) + 1
                col = (i % 2) + 1
                
                # Crea istogramma per ogni metrica
                fig.add_trace(
                    go.Histogram(
                        x=data[metric],
                        name=metric,
                        marker_color=self.color_palette[i % len(self.color_palette)]
                    ),
                    row=row, col=col
                )
            
            fig.update_layout(
                height=self.chart_config['height'] * rows,
                title_text="Dashboard Multi-Metrica",
                showlegend=False
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione del dashboard: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def create_time_series_with_events(self, time_data: pd.DataFrame, events_data: pd.DataFrame = None) -> go.Figure:
        """
        Crea una serie temporale con eventi marcati
        
        Args:
            time_data (pd.DataFrame): Dati della serie temporale
            events_data (pd.DataFrame, optional): Dati degli eventi da marcare
        
        Returns:
            go.Figure: Grafico temporale interattivo
        """
        try:
            if time_data.empty:
                return self._create_empty_chart("Nessun dato temporale")
            
            # Assume che le prime due colonne siano tempo e valore
            time_col, value_col = time_data.columns[0], time_data.columns[1]
            
            fig = go.Figure()
            
            # Serie temporale principale
            fig.add_trace(go.Scatter(
                x=time_data[time_col],
                y=time_data[value_col],
                mode='lines+markers',
                name='Trend',
                line=dict(color='blue', width=2)
            ))
            
            # Aggiungi eventi se forniti
            if events_data is not None and not events_data.empty:
                event_time_col = events_data.columns[0]
                for _, event in events_data.iterrows():
                    fig.add_vline(
                        x=event[event_time_col],
                        line_dash="dash",
                        line_color="red",
                        annotation_text=f"Evento: {event.get('event_type', 'N/A')}",
                        annotation_position="top"
                    )
            
            fig.update_layout(
                title="Serie Temporale con Eventi",
                xaxis_title=time_col.title(),
                yaxis_title=value_col.title(),
                height=self.chart_config['height'],
                hovermode='x unified'
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nella creazione della serie temporale: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def create_comparative_analysis(self, data: pd.DataFrame, group_col: str, value_col: str) -> go.Figure:
        """
        Crea analisi comparativa tra gruppi
        
        Args:
            data (pd.DataFrame): Dati da confrontare
            group_col (str): Colonna di raggruppamento
            value_col (str): Colonna dei valori
        
        Returns:
            go.Figure: Grafico comparativo
        """
        try:
            if data.empty:
                return self._create_empty_chart("Nessun dato per l'analisi comparativa")
            
            # Crea box plot e violin plot combinati
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=('Box Plot', 'Violin Plot'),
                horizontal_spacing=0.1
            )
            
            # Box plot
            for i, group in enumerate(data[group_col].unique()):
                group_data = data[data[group_col] == group]
                fig.add_trace(
                    go.Box(
                        y=group_data[value_col],
                        name=str(group),
                        marker_color=self.color_palette[i % len(self.color_palette)]
                    ),
                    row=1, col=1
                )
            
            # Violin plot
            for i, group in enumerate(data[group_col].unique()):
                group_data = data[data[group_col] == group]
                fig.add_trace(
                    go.Violin(
                        y=group_data[value_col],
                        name=str(group),
                        marker_color=self.color_palette[i % len(self.color_palette)],
                        showlegend=False
                    ),
                    row=1, col=2
                )
            
            fig.update_layout(
                title=f"Analisi Comparativa: {value_col} per {group_col}",
                height=self.chart_config['height']
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Errore nell'analisi comparativa: {str(e)}")
            return self._create_empty_chart(f"Errore: {str(e)}")
    
    def _create_empty_chart(self, message: str) -> go.Figure:
        """Crea un grafico vuoto con un messaggio"""
        fig = go.Figure()
        fig.add_annotation(
            text=message,
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            xanchor='center', yanchor='middle',
            showarrow=False,
            font=dict(size=16)
        )
        fig.update_layout(
            height=self.chart_config['height'],
            xaxis={'visible': False},
            yaxis={'visible': False}
        )
        return fig

# Funzioni utility per Streamlit
def display_chart(fig: go.Figure, use_container_width: bool = True):
    """
    Mostra un grafico Plotly in Streamlit
    
    Args:
        fig (go.Figure): Grafico da mostrare
        use_container_width (bool): Se usare la larghezza del container
    """
    try:
        st.plotly_chart(fig, use_container_width=use_container_width)
    except Exception as e:
        st.error(f"Errore nella visualizzazione del grafico: {str(e)}")

def create_download_button(fig: go.Figure, filename: str = "chart"):
    """
    Crea un bottone per scaricare il grafico
    
    Args:
        fig (go.Figure): Grafico da scaricare
        filename (str): Nome del file
    """
    try:
        # Converti in HTML
        html_string = fig.to_html()
        
        st.download_button(
            label="📥 Scarica Grafico (HTML)",
            data=html_string,
            file_name=f"{filename}.html",
            mime="text/html"
        )
        
        # Opzione per PNG (richiede kaleido)
        try:
            img_bytes = fig.to_image(format="png")
            st.download_button(
                label="📥 Scarica Grafico (PNG)",
                data=img_bytes,
                file_name=f"{filename}.png",
                mime="image/png"
            )
        except:
            pass  # kaleido non disponibile
            
    except Exception as e:
        st.warning(f"Download non disponibile: {str(e)}")

def show_chart_info(fig: go.Figure):
    """
    Mostra informazioni sul grafico
    
    Args:
        fig (go.Figure): Grafico di cui mostrare le info
    """
    try:
        with st.expander("ℹ️ Informazioni Grafico"):
            st.write(f"**Titolo:** {fig.layout.title.text if fig.layout.title else 'N/A'}")
            st.write(f"**Numero di tracce:** {len(fig.data)}")
            st.write(f"**Tipo di grafico:** {fig.data[0].type if fig.data else 'N/A'}")
            
            # Mostra dati se disponibili
            if fig.data:
                trace = fig.data[0]
                if hasattr(trace, 'x') and hasattr(trace, 'y'):
                    n_points = len(trace.x) if trace.x else 0
                    st.write(f"**Punti dati:** {n_points}")
                    
    except Exception as e:
        st.warning(f"Impossibile mostrare informazioni: {str(e)}")