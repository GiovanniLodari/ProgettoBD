#!/usr/bin/env python3
import argparse
import pandas as pd
import pyarrow.parquet as pq
import sys
import os
from pathlib import Path

def format_size(bytes_size):
    """Converte bytes in formato leggibile"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"

def get_file_info(file_path):
    """Ottiene informazioni base del file Parquet"""
    try:
        # Usa pyarrow per leggere metadati senza caricare i dati
        parquet_file = pq.ParquetFile(file_path)
        file_size = os.path.getsize(file_path)
        
        print(f"\n=== INFORMAZIONI FILE ===")
        print(f"File: {file_path}")
        print(f"Dimensione file: {format_size(file_size)}")
        print(f"Numero di righe: {parquet_file.metadata.num_rows:,}")
        print(f"Numero di colonne: {parquet_file.metadata.num_columns}")
        print(f"Numero di gruppi di righe: {parquet_file.metadata.num_row_groups}")
        
        # Schema delle colonne
        print(f"\n=== SCHEMA COLONNE ===")
        schema = parquet_file.schema.to_arrow_schema()
        for i, field in enumerate(schema):
            print(f"{i+1:2d}. {field.name:<30} | {field.type}")
            
        return parquet_file
        
    except Exception as e:
        print(f"Errore nel leggere il file: {e}")
        return None

def preview_data(file_path, rows=10, columns=None):
    """Mostra un'anteprima dei dati"""
    try:
        print(f"\n=== ANTEPRIMA DATI (prime {rows} righe) ===")
        
        # Usa pyarrow per leggere solo le prime N righe
        table = pq.read_table(file_path, columns=columns)
        df = table.to_pandas().head(rows)
        
        # Configura pandas per mostrare tutti i dati
        with pd.option_context('display.max_columns', None, 
                             'display.max_colwidth', 50,
                             'display.width', None):
            print(df)
            
        print(f"\nTipi di dati:")
        print(df.dtypes)
        
    except Exception as e:
        print(f"Errore nella lettura: {e}")

def explore_column(file_path, column_name, rows=1000):
    """Esplora una colonna specifica"""
    try:
        print(f"\n=== ANALISI COLONNA: {column_name} ===")
        
        # Usa pyarrow per leggere solo la colonna specifica
        table = pq.read_table(file_path, columns=[column_name])
        df = table.to_pandas().head(rows)
        col_data = df[column_name]
        
        print(f"Tipo di dato: {col_data.dtype}")
        print(f"Valori non nulli: {col_data.count():,}/{len(col_data):,}")
        print(f"Valori nulli: {col_data.isnull().sum():,}")
        print(f"Valori unici: {col_data.nunique():,}")
        
        if col_data.dtype in ['object', 'string']:
            print(f"\nPrimi 10 valori unici:")
            unique_vals = col_data.dropna().unique()[:10]
            for i, val in enumerate(unique_vals, 1):
                val_str = str(val)[:100] + "..." if len(str(val)) > 100 else str(val)
                print(f"{i:2d}. {val_str}")
                
        elif col_data.dtype in ['int64', 'int32', 'float64', 'float32']:
            print(f"\nStatistiche:")
            print(f"Min: {col_data.min()}")
            print(f"Max: {col_data.max()}")
            print(f"Media: {col_data.mean():.2f}")
            print(f"Mediana: {col_data.median()}")
            
    except Exception as e:
        print(f"Errore nell'analisi della colonna: {e}")

def sample_data(file_path, n_samples=100, columns=None):
    """Mostra un campione casuale dei dati"""
    try:
        print(f"\n=== CAMPIONE CASUALE ({n_samples} righe) ===")
        
        # Prima ottiene il numero totale di righe
        parquet_file = pq.ParquetFile(file_path)
        total_rows = parquet_file.metadata.num_rows
        
        if total_rows <= n_samples:
            table = pq.read_table(file_path, columns=columns)
            df = table.to_pandas()
        else:
            # Per file grandi, legge tutto e poi campiona
            # Alternativa: potresti leggere solo alcuni row groups
            table = pq.read_table(file_path, columns=columns)
            df = table.to_pandas().sample(n=min(n_samples, len(table)))
        
        with pd.option_context('display.max_columns', None, 
                             'display.max_colwidth', 50,
                             'display.width', None):
            print(df)
            
    except Exception as e:
        print(f"Errore nel campionamento: {e}")

def search_data(file_path, column, value, max_results=10):
    """Cerca un valore specifico in una colonna"""
    try:
        print(f"\n=== RICERCA: '{value}' in colonna '{column}' ===")
        
        # Legge la colonna specifica
        table = pq.read_table(file_path, columns=[column])
        df = table.to_pandas()
        
        # Cerca il valore
        if df[column].dtype == 'object':
            mask = df[column].astype(str).str.contains(str(value), case=False, na=False)
        else:
            mask = df[column] == value
            
        results = df[mask].head(max_results)
        
        print(f"Trovate {mask.sum():,} corrispondenze")
        if len(results) > 0:
            print(f"Prime {len(results)} righe:")
            print(results)
        else:
            print("Nessuna corrispondenza trovata")
            
    except Exception as e:
        print(f"Errore nella ricerca: {e}")

def main():
    parser = argparse.ArgumentParser(description='Visualizzatore Parquet da linea di comando')
    parser.add_argument('file', help='Path del file Parquet')
    parser.add_argument('--info', action='store_true', help='Mostra informazioni del file')
    parser.add_argument('--preview', type=int, default=10, help='Numero di righe da mostrare in anteprima')
    parser.add_argument('--sample', type=int, help='Mostra N righe casuali')
    parser.add_argument('--columns', nargs='+', help='Colonne specifiche da visualizzare')
    parser.add_argument('--explore', help='Analizza una colonna specifica')
    parser.add_argument('--search', nargs=2, metavar=('COLUMN', 'VALUE'), help='Cerca un valore in una colonna')
    parser.add_argument('--max-results', type=int, default=10, help='Massimo numero di risultati di ricerca')
    
    args = parser.parse_args()
    
    # Verifica che il file esista
    if not os.path.exists(args.file):
        print(f"Errore: File '{args.file}' non trovato")
        sys.exit(1)
    
    # Mostra sempre le informazioni di base se --info o nessun'altra opzione
    if args.info or not any([args.preview, args.sample, args.explore, args.search]):
        parquet_file = get_file_info(args.file)
        if not parquet_file:
            sys.exit(1)
    
    # Anteprima dati
    if args.preview and args.preview > 0:
        preview_data(args.file, args.preview, args.columns)
    
    # Campione casuale
    if args.sample:
        sample_data(args.file, args.sample, args.columns)
    
    # Esplorazione colonna
    if args.explore:
        explore_column(args.file, args.explore)
    
    # Ricerca
    if args.search:
        search_data(args.file, args.search[0], args.search[1], args.max_results)

if __name__ == "__main__":
    main()