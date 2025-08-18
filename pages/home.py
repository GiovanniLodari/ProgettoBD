"""
Analizzatore Disastri Naturali - Multi-Dataset
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import io
import os
import gzip
import json
import tempfile
import shutil

# Configurazione pagina
st.set_page_config(
    page_title="Disaster Analytics",
    page_icon="🌪️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS minimalista
st.markdown("""
<style>
    .stApp > header {visibility: hidden;}
    .stDeployButton {visibility: hidden;}
    .main .block-container {padding-top: 1rem;}
    .metric-container {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #e9ecef;
    }
    .stTabs [data-baseweb="tab"] {
        height: 40px;
        padding: 8px 16px;
    }
    .upload-section {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

def load_sample_data():
    """Genera dati di esempio per il demo"""
    np.random.seed(42)
    
    disaster_types = ['Earthquake', 'Flood', 'Hurricane', 'Wildfire', 'Tornado', 'Tsunami']
    countries = ['Italy', 'USA', 'Japan', 'Australia', 'Germany', 'Brazil', 'India', 'China']
    
    n_records = 1000
    
    data = {
        'disaster_id': range(1, n_records + 1),
        'type': np.random.choice(disaster_types, n_records),
        'country': np.random.choice(countries, n_records),
        'date': pd.date_range('2020-01-01', '2023-12-31', periods=n_records),
        'magnitude': np.random.uniform(1, 9, n_records).round(1),
        'casualties': np.random.poisson(50, n_records),
        'economic_loss': np.random.lognormal(15, 2, n_records).astype(int),
        'duration_days': np.random.exponential(5, n_records).astype(int) + 1
    }
    
    return pd.DataFrame(data)

def convert_json_gz_to_parquet(uploaded_file, temp_dir):
    """Converte file JSON/JSON.GZ in Parquet per ottimizzazione"""
    try:
        # Leggi contenuto file
        content = uploaded_file.read()
        if uploaded_file.name.endswith('.gz'):
            content = gzip.decompress(content)

        df = None

        # 1. Prova come NDJSON (ogni riga un JSON)
        try:
            df = pd.read_json(io.BytesIO(content), lines=True)
        except Exception:
            # 2. Fallback: singolo JSON standard
            json_data = json.loads(content.decode('utf-8'))

            # Se è lista → DataFrame diretto
            if isinstance(json_data, list):
                df = pd.DataFrame(json_data)

            if isinstance(json_data, list):
                df = pd.DataFrame(json_data)
            elif isinstance(json_data, dict):
                try:
                    df = pd.json_normalize(json_data)
                except Exception:
                    df = pd.DataFrame([json_data])
            else:
                raise ValueError("Formato JSON non supportato")

        # 3. Fix colonne problematiche (dict/list → stringa)
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df[col] = df[col].astype(str)

        # 4. Salva come parquet temporaneo
        parquet_name = uploaded_file.name.replace('.json.gz', '.parquet').replace('.json', '.parquet')
        parquet_path = os.path.join(temp_dir, parquet_name)
        df.to_parquet(parquet_path, index=False)

        return df, parquet_path, parquet_name

    except Exception as e:
        raise Exception(f"Errore conversione {uploaded_file.name}: {str(e)}")

        
    except Exception as e:
        raise Exception(f"Errore conversione {uploaded_file.name}: {str(e)}")

def load_multiple_files(uploaded_files):
    """Carica e combina multiple file con supporto JSON.GZ"""
    all_dataframes = []
    load_info = []
    conversion_info = []
    
    # Crea directory temporanea per parquet
    temp_dir = tempfile.mkdtemp()
    
    total_files = len(uploaded_files)
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for idx, uploaded_file in enumerate(uploaded_files):
        try:
            status_text.text(f"Processing {uploaded_file.name} ({idx+1}/{total_files})")
            progress_bar.progress((idx + 1) / total_files)
            
            original_size = uploaded_file.size
            df = None
            parquet_path = None
            converted = False
            
            # Reset file pointer
            uploaded_file.seek(0)
            
            if uploaded_file.name.endswith('.json.gz') or uploaded_file.name.endswith('.json'):
                # Converti JSON/JSON.GZ in Parquet
                df, parquet_path, parquet_name = convert_json_gz_to_parquet(uploaded_file, temp_dir)
                converted = True
                
                # Info conversione
                parquet_size = os.path.getsize(parquet_path)
                compression_ratio = (1 - parquet_size / original_size) * 100
                
                conversion_info.append({
                    'original_file': uploaded_file.name,
                    'parquet_file': parquet_name,
                    'original_size_mb': original_size / 1024 / 1024,
                    'parquet_size_mb': parquet_size / 1024 / 1024,
                    'compression_ratio': compression_ratio
                })
                
            elif uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            elif uploaded_file.name.endswith('.parquet'):
                df = pd.read_parquet(uploaded_file)
            else:
                st.warning(f"Formato non supportato: {uploaded_file.name}")
                continue
            
            if df is not None:
                # Aggiungi colonna source per tracciare origine
                df['source_file'] = uploaded_file.name
                df['source_id'] = idx + 1
                
                all_dataframes.append(df)
                load_info.append({
                    'file': uploaded_file.name,
                    'rows': len(df),
                    'columns': len(df.columns),
                    'size_mb': original_size / 1024 / 1024,
                    'converted': converted,
                    'parquet_path': parquet_path
                })
                
        except Exception as e:
            st.error(f"Error loading {uploaded_file.name}: {str(e)}")
    
    # Clear progress
    progress_bar.empty()
    status_text.empty()
    
    # Store temp directory in session state for cleanup
    st.session_state.temp_dir = temp_dir
    st.session_state.conversion_info = conversion_info
    
    return all_dataframes, load_info

def create_overview_metrics(df, load_info=None):
    """Crea metriche di overview"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Events", f"{len(df):,}")
        if load_info:
            st.caption(f"From {len(load_info)} files")
    
    with col2:
        if 'casualties' in df.columns:
            avg_casualties = df['casualties'].mean()
            st.metric("Avg Casualties", f"{avg_casualties:.0f}")
        else:
            st.metric("Datasets", f"{df['source_id'].nunique() if 'source_id' in df.columns else 1}")
    
    with col3:
        if 'economic_loss' in df.columns:
            total_loss = df['economic_loss'].sum() / 1e9
            st.metric("Total Loss", f"${total_loss:.1f}B")
        else:
            st.metric("Columns", f"{len(df.columns)}")
    
    with col4:
        if 'date' in df.columns:
            date_range = (df['date'].max() - df['date'].min()).days
            st.metric("Period (days)", f"{date_range:,}")
        else:
            st.metric("Total Rows", f"{len(df):,}")

def create_disaster_distribution(df):
    """Grafico distribuzione per tipo"""
    if 'type' not in df.columns:
        # Trova colonna più probabile per tipo disastro
        type_cols = [col for col in df.columns if 'type' in col.lower() or 'disaster' in col.lower()]
        if not type_cols:
            return None
        type_col = type_cols[0]
    else:
        type_col = 'type'
    
    type_counts = df[type_col].value_counts()
    
    fig = px.bar(
        x=type_counts.values,
        y=type_counts.index,
        orientation='h',
        title=f"Events by {type_col}",
        color=type_counts.values,
        color_continuous_scale="viridis"
    )
    fig.update_layout(
        height=400,
        showlegend=False,
        xaxis_title="Count",
        yaxis_title=type_col.title()
    )
    return fig

def create_temporal_trend(df):
    """Grafico trend temporale"""
    # Trova colonna data
    date_cols = [col for col in df.columns if any(keyword in col.lower() 
                 for keyword in ['date', 'time', 'year', 'occurred'])]
    
    if not date_cols:
        return None
    
    date_col = date_cols[0]
    
    try:
        # Converti in datetime se non lo è già
        if df[date_col].dtype == 'object':
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Raggruppa per mese
        df['year_month'] = df[date_col].dt.to_period('M')
        monthly_counts = df.groupby('year_month').size().reset_index(name='count')
        monthly_counts['date'] = monthly_counts['year_month'].dt.to_timestamp()
        
        fig = px.line(
            monthly_counts,
            x='date',
            y='count',
            title="Temporal Trend",
            markers=True
        )
        fig.update_layout(
            height=400,
            xaxis_title="Date",
            yaxis_title="Number of Events"
        )
        return fig
    except:
        return None

def create_geographical_distribution(df):
    """Grafico distribuzione geografica"""
    # Trova colonna paese/regione
    geo_cols = [col for col in df.columns if any(keyword in col.lower() 
                for keyword in ['country', 'nation', 'region', 'location'])]
    
    if not geo_cols:
        return None
    
    geo_col = geo_cols[0]
    country_counts = df[geo_col].value_counts().head(10)
    
    fig = px.bar(
        x=country_counts.values,
        y=country_counts.index,
        orientation='h',
        title=f"Top 10 {geo_col.title()}",
        color=country_counts.values,
        color_continuous_scale="plasma"
    )
    fig.update_layout(
        height=400,
        showlegend=False,
        xaxis_title="Count",
        yaxis_title=geo_col.title()
    )
    return fig

def create_files_distribution(df):
    """Distribuzione per file sorgente"""
    if 'source_file' not in df.columns:
        return None
    
    file_counts = df['source_file'].value_counts()
    
    fig = px.pie(
        values=file_counts.values,
        names=file_counts.index,
        title="Events by Source File"
    )
    fig.update_layout(height=400)
    return fig

def perform_custom_aggregation(df, group_by, agg_col, agg_func):
    """Aggregazione personalizzata"""
    try:
        if agg_func == 'count':
            result = df.groupby(group_by).size().reset_index(name='count')
        else:
            result = df.groupby(group_by)[agg_col].agg(agg_func).reset_index()
        
        # Ordina per valore decrescente
        sort_col = result.columns[-1]
        result = result.sort_values(sort_col, ascending=False).head(20)
        return result
    except Exception as e:
        st.error(f"Aggregation error: {str(e)}")
        return None

def cleanup_temp_files():
    """Pulizia file temporanei"""
    if hasattr(st.session_state, 'temp_dir') and st.session_state.temp_dir:
        try:
            shutil.rmtree(st.session_state.temp_dir)
            st.session_state.temp_dir = None
        except:
            pass

def main():
    """Applicazione principale"""
    
    # Cleanup on app restart
    if 'app_initialized' not in st.session_state:
        cleanup_temp_files()
        st.session_state.app_initialized = True
    
    # Header
    st.title("🌪️ Disaster Analytics - Multi Dataset")
    
    # Inizializza session state
    if 'datasets_loaded' not in st.session_state:
        st.session_state.datasets_loaded = False
        st.session_state.combined_df = None
        st.session_state.load_info = None
    
    # Upload section
    with st.container():
        st.markdown('<div class="upload-section">', unsafe_allow_html=True)
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            uploaded_files = st.file_uploader(
                "Upload multiple datasets (CSV, JSON, JSON.GZ, Parquet)",
                type=['csv', 'json', 'gz', 'parquet'],
                accept_multiple_files=True,
                help="JSON.GZ files will be automatically converted to Parquet for better performance"
            )
        
        with col2:
            use_sample = st.checkbox("Use sample data", value=not uploaded_files)
            if st.button("🔄 Reset", help="Clear all loaded data"):
                cleanup_temp_files()
                st.session_state.datasets_loaded = False
                st.session_state.combined_df = None
                st.session_state.load_info = None
                st.rerun()
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Process files
    if uploaded_files or use_sample:
        if uploaded_files and not st.session_state.datasets_loaded:
            with st.spinner(f"Loading {len(uploaded_files)} files..."):
                dataframes, load_info = load_multiple_files(uploaded_files)
                
                if dataframes:
                    # Combina tutti i dataframe
                    combined_df = pd.concat(dataframes, ignore_index=True, sort=False)
                    
                    st.session_state.combined_df = combined_df
                    st.session_state.load_info = load_info
                    st.session_state.datasets_loaded = True
                    
                    # Info caricamento
                    total_rows = sum(info['rows'] for info in load_info)
                    total_size = sum(info['size_mb'] for info in load_info)
                    
                    st.success(f"✅ Loaded {len(load_info)} files: {total_rows:,} total records ({total_size:.1f} MB)")
                    
                    # Mostra info conversioni se presenti
                    if hasattr(st.session_state, 'conversion_info') and st.session_state.conversion_info:
                        with st.expander("🔄 JSON.GZ → Parquet Conversion Details"):
                            conv_df = pd.DataFrame(st.session_state.conversion_info)
                            conv_df['compression_ratio'] = conv_df['compression_ratio'].round(1)
                            st.dataframe(conv_df, use_container_width=True)
                            
                            avg_compression = conv_df['compression_ratio'].mean()
                            st.success(f"Average compression: {avg_compression:.1f}% size reduction")
        
        elif use_sample and not st.session_state.datasets_loaded:
            st.session_state.combined_df = load_sample_data()
            st.session_state.datasets_loaded = True
            st.info("📊 Using sample data - upload your files above")
    
    # Mostra dati se caricati
    if st.session_state.datasets_loaded and st.session_state.combined_df is not None:
        df = st.session_state.combined_df
        
        # File info expandable
        if st.session_state.load_info:
            with st.expander(f"📁 File Details ({len(st.session_state.load_info)} files)"):
                info_df = pd.DataFrame(st.session_state.load_info)
                
                # Add converted indicator
                if 'converted' in info_df.columns:
                    info_df['format'] = info_df.apply(lambda x: 
                        '📦 JSON.GZ→Parquet' if x['converted'] else 
                        '📄 ' + x['file'].split('.')[-1].upper(), axis=1)
                
                st.dataframe(info_df[['file', 'format', 'rows', 'columns', 'size_mb']], 
                           use_container_width=True)
        
        # Quick dataset info
        with st.expander("📋 Dataset Overview", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Columns:**")
                for col in df.columns[:10]:  # Mostra solo prime 10
                    st.write(f"• {col}")
                if len(df.columns) > 10:
                    st.write(f"• ... and {len(df.columns)-10} more")
            
            with col2:
                st.write("**Sample:**")
                st.dataframe(df.head(3), use_container_width=True)
        
        # Metriche overview
        create_overview_metrics(df, st.session_state.load_info)
        
        st.divider()
        
        # Layout principale con tabs
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Analysis", "🔍 Custom Query", "📈 Advanced", "💾 Export"])
        
        with tab1:
            col1, col2 = st.columns(2)
            
            with col1:
                # Distribuzione per tipo
                fig1 = create_disaster_distribution(df)
                if fig1:
                    st.plotly_chart(fig1, use_container_width=True)
                else:
                    st.info("Type distribution: No suitable column found")
                
                # Distribuzione geografica
                fig2 = create_geographical_distribution(df)
                if fig2:
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.info("Geographic distribution: No suitable column found")
            
            with col2:
                # Trend temporale
                fig3 = create_temporal_trend(df)
                if fig3:
                    st.plotly_chart(fig3, use_container_width=True)
                else:
                    st.info("Temporal trend: No suitable date column found")
                
                # Distribuzione per file sorgente
                fig4 = create_files_distribution(df)
                if fig4:
                    st.plotly_chart(fig4, use_container_width=True)
                else:
                    st.info("Multiple files analysis not available")
        
        with tab2:
            st.subheader("🔍 Custom Aggregation")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                group_col = st.selectbox("Group by:", df.columns)
            
            with col2:
                numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                available_cols = ['count'] + numeric_cols
                agg_col = st.selectbox("Aggregate:", available_cols)
            
            with col3:
                if agg_col != 'count':
                    agg_func = st.selectbox("Function:", ['sum', 'mean', 'max', 'min', 'std'])
                else:
                    agg_func = 'count'
            
            with col4:
                if st.button("🚀 Run", type="primary"):
                    result = perform_custom_aggregation(df, group_col, agg_col, agg_func)
                    
                    if result is not None:
                        col_left, col_right = st.columns(2)
                        with col_left:
                            st.dataframe(result, use_container_width=True)
                        
                        with col_right:
                            if len(result) > 0:
                                fig = px.bar(
                                    result.head(10),
                                    x=result.columns[-1],
                                    y=result.columns[0],
                                    orientation='h',
                                    title=f"{agg_func.title()} of {agg_col} by {group_col}"
                                )
                                st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            col1, col2 = st.columns(2)
            
            with col1:
                # Correlation matrix
                numeric_df = df.select_dtypes(include=[np.number])
                if len(numeric_df.columns) > 1:
                    corr_matrix = numeric_df.corr()
                    
                    fig = px.imshow(
                        corr_matrix,
                        text_auto=True,
                        aspect="auto",
                        title="Correlation Matrix",
                        color_continuous_scale="RdBu_r"
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Need at least 2 numeric columns for correlation")
                
                # Top events per file
                if 'source_file' in df.columns:
                    st.subheader("📁 Events per File")
                    file_counts = df['source_file'].value_counts()
                    st.dataframe(file_counts.head(10), use_container_width=True)
            
            with col2:
                # Data quality overview
                st.subheader("🎯 Data Quality")
                
                quality_info = []
                for col in df.columns:
                    null_pct = (df[col].isnull().sum() / len(df)) * 100
                    quality_info.append({
                        'Column': col,
                        'Null %': f"{null_pct:.1f}%",
                        'Unique': df[col].nunique(),
                        'Type': str(df[col].dtype)
                    })
                
                quality_df = pd.DataFrame(quality_info)
                st.dataframe(quality_df, use_container_width=True)
        
        with tab4:
            st.subheader("💾 Export Data")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Export combined dataset
                csv = df.to_csv(index=False)
                st.download_button(
                    "📁 Download Combined Dataset (CSV)",
                    csv,
                    f"combined_disaster_data_{len(df)}_records.csv",
                    "text/csv"
                )
                
                # Export as optimized parquet
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                st.download_button(
                    "🚀 Download Combined Dataset (Parquet)",
                    parquet_buffer.getvalue(),
                    f"combined_disaster_data_{len(df)}_records.parquet",
                    "application/octet-stream"
                )
            
            with col2:
                # Export per source file
                if 'source_file' in df.columns:
                    selected_source = st.selectbox(
                        "Export single file:",
                        ['All'] + df['source_file'].unique().tolist()
                    )
                    
                    if selected_source != 'All':
                        filtered_df = df[df['source_file'] == selected_source]
                        filtered_csv = filtered_df.to_csv(index=False)
                        st.download_button(
                            f"📋 Download {selected_source}",
                            filtered_csv,
                            f"filtered_{selected_source}.csv",
                            "text/csv"
                        )
            
            with col3:
                # Export summary
                summary_data = {
                    'Metric': ['Total Records', 'Total Columns', 'Files Loaded', 'Date Range'],
                    'Value': [
                        len(df),
                        len(df.columns),
                        df['source_file'].nunique() if 'source_file' in df.columns else 1,
                        f"{df['date'].min()} to {df['date'].max()}" if 'date' in df.columns else 'N/A'
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_csv = summary_df.to_csv(index=False)
                st.download_button(
                    "📊 Download Summary",
                    summary_csv,
                    "disaster_summary.csv",
                    "text/csv"
                )
    
    else:
        # Stato iniziale - nessun dato caricato
        st.info("👆 Upload your disaster datasets above to start the analysis")
        
        # Show example of what we expect
        st.subheader("📋 Supported Formats")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            **📦 JSON.GZ (Recommended)**
            - Automatically converted to Parquet
            - Best compression ratio
            - Fastest processing after conversion
            
            **📄 Other Formats**
            - CSV: Standard comma-separated
            - JSON: Plain JSON files  
            - Parquet: Already optimized
            """)
        
        with col2:
            st.markdown("**📊 Expected JSON Structure:**")
            example_json = {
                "disasters": [
                    {"id": 1, "type": "Earthquake", "country": "Italy", "casualties": 150},
                    {"id": 2, "type": "Flood", "country": "Germany", "casualties": 25}
                ]
            }
            st.code(json.dumps(example_json, indent=2), language='json')

if __name__ == "__main__":
    main()