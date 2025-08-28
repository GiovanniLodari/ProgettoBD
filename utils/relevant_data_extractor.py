import gzip
import json
import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog
from typing import Dict, Any, List, Optional



def select_files() -> List[str]:
    """Apre una finestra di dialogo per selezionare uno o più file."""
    root = tk.Tk()
    root.withdraw()
    file_paths = filedialog.askopenfilenames(
        title="Seleziona uno o più file .json.gz",
        filetypes=[("File JSON compressi", "*.json.gz"), ("Tutti i file", "*.*")]
    )
    return list(file_paths)

# --- PASSO 1: DEFINISCI LE COLONNE CHE VUOI E COME TROVARLE ---

def get_tweet_id(tweet: Dict[str, Any]) -> Optional[str]:
    """Estrae l'ID del tweet, preferendo sempre la versione stringa."""
    return tweet.get("id_str")

def get_full_text(tweet: Dict[str, Any]) -> Optional[str]:
    """
    Estrae il testo completo del tweet, gestendo il formato esteso.
    Questa è la logica di "coalesce" per i formati diversi.
    """
    if 'extended_tweet' in tweet and tweet['extended_tweet'] and 'full_text' in tweet['extended_tweet']:
        return tweet['extended_tweet']['full_text']
    return tweet.get("text")

def get_user_id(tweet: Dict[str, Any]) -> Optional[str]:
    """
    Estrae l'ID dell'utente dall'oggetto annidato 'user'.
    Questo è un esempio di come trattare i campi annidati.
    """
    user_obj = tweet.get("user")
    if user_obj and isinstance(user_obj, dict):
        return user_obj.get("id_str")
    return None

def get_user_name(tweet: Dict[str, Any]) -> Optional[str]:
    """Estrae lo screen_name dell'utente."""
    user_obj = tweet.get("user")
    if user_obj and isinstance(user_obj, dict):
        return user_obj.get("screen_name")
    return None
    
def get_user_followers(tweet: Dict[str, Any]) -> Optional[int]:
    """Estrae il numero di follower dell'utente."""
    user_obj = tweet.get("user")
    if user_obj and isinstance(user_obj, dict):
        return user_obj.get("followers_count")
    return None

def get_creation_date(tweet: Dict[str, Any]) -> Optional[str]:
    """Estrae la data di creazione."""
    return tweet.get("created_at")

def get_language(tweet: Dict[str, Any]) -> Optional[str]:
    """Estrae la lingua del tweet."""
    return tweet.get("lang")

def get_retweet_count(tweet: Dict[str, Any]) -> Optional[int]:
    """Estrae il conteggio dei retweet."""
    return tweet.get("retweet_count")

def get_place_fullname(tweet: Dict[str, Any]) -> Optional[str]:
    """Estrae il nome completo del luogo, se presente."""
    place_obj = tweet.get("place")
    if place_obj and isinstance(place_obj, dict):
        return place_obj.get("full_name")
    return None

# Mappa tra il nome della colonna finale e la funzione per estrarla
# Questa mappa definisce il tuo schema finale e pulito.
EXTRACTION_RULES = {
    'tweet_id': get_tweet_id,
    'data_creazione': get_creation_date,
    'testo_completo': get_full_text,
    'lingua': get_language,
    'user_id': get_user_id,
    'user_name': get_user_name,
    'user_followers': get_user_followers,
    'luogo': get_place_fullname,
    'conteggio_retweet': get_retweet_count
}

# --- PASSO 2: FUNZIONE PER PROCESSARE I FILE ---

def process_json_gz_files(json_gz_path: str) -> pd.DataFrame:
    """
    Legge una lista di file .json.gz, estrae le colonne di interesse
    e restituisce un DataFrame Pandas pulito.
    """
    cleaned_data = []
    
    print(f"Processando il file: {json_gz_path}...")
    try:
        with gzip.open(json_gz_path, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    tweet = json.loads(line)
                        
                        # Applica le regole di estrazione per questo tweet
                    clean_record = {}
                    for col_name, extraction_func in EXTRACTION_RULES.items():
                        clean_record[col_name] = extraction_func(tweet)
                        
                    cleaned_data.append(clean_record)
                            
                except json.JSONDecodeError:
                    continue # Salta le righe JSON corrotte
    except Exception as e:
        print(f"Errore durante l'elaborazione del file {json_gz_path}: {e}")
            
    print("Conversione in DataFrame...")
    return pd.DataFrame(cleaned_data)

# --- PASSO 3: ESECUZIONE ---

if __name__ == "__main__":
    OUTPUT_DIR = r"C:\\Users\\giova\\Desktop\\dati_parquet_puliti"
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"I file Parquet verranno salvati in: '{OUTPUT_DIR}'")

    file_da_processare = select_files()
    
    if file_da_processare:
        for file_path in file_da_processare:
            df_pulito = process_json_gz_files(file_path)
        
            if not df_pulito.empty:
                print("\n--- DataFrame Pulito Creato con Successo ---")
                print("Informazioni sul DataFrame:")
                df_pulito.info()
                print("\nPrime 5 righe del DataFrame:")
                print(df_pulito.head())

                base_name = os.path.basename(file_path)
                file_name_without_ext = os.path.splitext(os.path.splitext(base_name)[0])[0]
                output_file_name = f"{file_name_without_ext}.parquet"
                output_path = os.path.join(OUTPUT_DIR, output_file_name)

                try:
                    df_pulito.to_parquet(output_path, engine='pyarrow', index=False)
                    print(f"✅ File convertito e salvato con successo in: {output_path}")
                except Exception as e:
                    print(f"❌ Errore durante il salvataggio del file {output_path}: {e}")
            else:
                print(f"⚠️ DataFrame vuoto per il file {file_path}. File non salvato.")

    else:
        print("Nessun file da processare.")