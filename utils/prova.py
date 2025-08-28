import pandas as pd
import json # Importiamo la libreria json

# --- CONFIGURAZIONE ---
file_input_jsonl = r"C:\Users\giova\Desktop\prova\170826213907_hurricane_harvey_2017_20170903_vol-2.json"
file_output_parquet = r"C:\Users\giova\Desktop\ProgettoBD\hurricane_harvey_dati_FINALE.parquet"
# --------------------

def to_json_string(obj):
    """
    Funzione sicura per convertire un oggetto Python (come un dict o una lista)
    in una stringa JSON. Se l'oggetto è nullo, restituisce None.
    """
    if pd.isna(obj):
        return None
    return json.dumps(obj)

print(f"Inizio conversione finale con serializzazione...")
print(f"Lettura del file: {file_input_jsonl}...")

try:
    df = pd.read_json(file_input_jsonl, lines=True)
    print(f"Lettura completata. Trovate {len(df)} righe.")
    
    # --- ✨ FASE DI SERIALIZZAZIONE (LA VERA SOLUZIONE) ✨ ---
    # Identifichiamo le colonne che contengono oggetti complessi annidati.
    colonne_complesse = ['place', 'retweeted_status', 'user', 'entities', 'extended_entities', 'quoted_status']
    
    print("\nSerializzo le colonne complesse in stringhe JSON per evitare problemi di schema...")
    
    for col in colonne_complesse:
        if col in df.columns:
            print(f" - Processo la colonna: '{col}'")
            df[col] = df[col].apply(to_json_string)

    # --- FASE DI SALVATAGGIO ---
    
    print(f"\nConversione in formato Parquet...")
    df.to_parquet(file_output_parquet, engine='pyarrow', index=False)
    
    print(f"\n\n🚀🚀🚀 SUCCESSO! 🚀🚀🚀")
    print(f"File Parquet creato con successo in: {file_output_parquet}")

except Exception as e:
    print(f"❌ Si è verificato un errore: {e}")