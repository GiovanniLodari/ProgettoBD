import os
import gzip
import json
import logging
import pandas as pd

# --- Configurazione ---
INPUT_DIR = r"C:\Users\giova\Desktop\ProgettoBD\data\File_compressi"
OUTPUT_PARQUET_DIR = r"C:\Users\giova\Desktop\ProgettoBD\data\File_convertiti_in_parquet"
REPORT_FILE = r"C:\Users\giova\Desktop\ProgettoBD\schema_report.csv"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def inspect_and_convert_with_pandas():
    """
    Ispeziona una directory di file JSON compressi usando solo Pandas,
    estrae lo schema e converte i file in formato Parquet.
    """
    
    # Crea la directory di output se non esiste
    os.makedirs(OUTPUT_PARQUET_DIR, exist_ok=True)
    
    schema_report_data = []
    
    try:
        all_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.gz') or f.endswith('.json')]
        if not all_files:
            logging.warning(f"Nessun file .gz trovato in {INPUT_DIR}")
            return
        logging.info(f"Trovati {len(all_files)} file da processare.")
    except FileNotFoundError:
        logging.error(f"ERRORE: La directory di input non è stata trovata: {INPUT_DIR}")
        return

    # Processa ogni file singolarmente
    for filename in all_files:
        input_path = os.path.join(INPUT_DIR, filename)
        logging.info(f"--- Processando: {filename} ---")

        try:
            # 1. Decomprimi e leggi il file riga per riga
            # I dati Twitter sono spesso in formato JSON Lines (un JSON per riga)
            records = []

            records = []
            # Scegli come aprire il file in base all'estensione
            open_func = gzip.open if filename.endswith('.gz') else open
            
            with open_func(input_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    # Ignora righe vuote
                    if line.strip():
                        records.append(json.loads(line))
            
            if not records:
                logging.warning(f"File {filename} è vuoto o non contiene JSON validi.")
                continue

            # 2. Normalizza il JSON nidificato in una tabella piatta con Pandas
            df = pd.json_normalize(records, sep='_')
            
            # 3. Estrai lo schema (nomi delle colonne e tipi di dati)
            schema = df.dtypes
            logging.info(f"Schema rilevato con {len(schema)} colonne.")
            
            for col_name, dtype in schema.items():
                schema_report_data.append({
                    'filename': filename,
                    'column_name': col_name,
                    'data_type': str(dtype)
                })

            # 4. Converti e salva in Parquet
            output_filename = filename.replace('.json.gz', '').replace('.gz', '') + '.parquet'
            output_path = os.path.join(OUTPUT_PARQUET_DIR, output_filename)
            
            logging.info(f"Conversione in Parquet in corso -> {output_path}")
            # 'pyarrow' è il motore standard per la scrittura in Parquet
            df.to_parquet(output_path, engine='pyarrow')
            logging.info("Conversione completata con successo.")

        except json.JSONDecodeError as e:
            logging.error(f"ERRORE di decodifica JSON in {filename}: File corrotto. Verrà saltato. Dettagli: {e}")
            schema_report_data.append({
                'filename': filename,
                'column_name': '_FILE_CORROTTO_O_ILLEGGIBILE_',
                'data_type': 'JSONDecodeError'
            })
        except Exception as e:
            logging.error(f"ERRORE SCONOSCIUTO nel processare {filename}: {e}")
            schema_report_data.append({
                'filename': filename,
                'column_name': f'_ERRORE_SCONOSCIUTO_',
                'data_type': str(e)
            })

    # 5. Salva il report consolidato degli schemi
    if schema_report_data:
        logging.info(f"Creazione del report degli schemi in: {REPORT_FILE}")
        report_df = pd.DataFrame(schema_report_data)
        report_df.to_csv(REPORT_FILE, index=False)
        logging.info("Report creato.")
        
    logging.info("Processo completato.")

if __name__ == "__main__":
    inspect_and_convert_with_pandas()