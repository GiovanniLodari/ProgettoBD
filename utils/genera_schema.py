import json
import tkinter as tk
from tkinter import filedialog
from pyspark.sql import SparkSession

def select_single_file() -> str:
    """
    Apre una finestra di dialogo per permettere all'utente di selezionare un singolo file.
    """
    print("Apertura della finestra per selezionare un file campione...")
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(
        title="Seleziona UN file .json.gz rappresentativo",
        filetypes=[("File JSON compressi", "*.json.gz"), ("Tutti i file", "*.*")]
    )
    return file_path

if __name__ == "__main__":
    # 1. Inizializza una sessione Spark
    spark = SparkSession.builder \
        .appName("SchemaGenerator") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # 2. Seleziona un file campione da analizzare
    file_path_campione = select_single_file()

    if file_path_campione:
        print(f"File selezionato: {file_path_campione}")
        print("Spark sta analizzando il file per inferire lo schema... (potrebbe richiedere un po' di tempo)")
        
        try:
            # 3. LEGGI IL FILE E LASCIA CHE SPARK INFERISCA LO SCHEMA
            # Spark fa il lavoro pesante di analizzare la struttura annidata
            df_campione = spark.read.json(file_path_campione)
            
            # 4. Estrai lo schema inferito
            schema_inferito = df_campione.schema
            
            # 5. Salva lo schema in un file JSON di bozza, formattato per essere leggibile
            file_schema_bozza = "schema_bozza.json"
            with open(file_schema_bozza, "w", encoding="utf-8") as f:
                f.write(schema_inferito.prettyJson())
            
            print(f"\n✅ Successo! Una bozza dello schema è stata salvata nel file: '{file_schema_bozza}'")
            print("\nPROSSIMO PASSO: Apri questo file, correggilo e salvalo come 'golden_schema.json'")

        except Exception as e:
            print(f"\n❌ Si è verificato un errore durante l'analisi di Spark: {e}")
        finally:
            # 6. Chiudi la sessione Spark
            spark.stop()
    else:
        print("Nessun file selezionato. Esecuzione terminata.")
