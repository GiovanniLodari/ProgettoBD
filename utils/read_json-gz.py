import gzip
import json
import tkinter as tk
from tkinter import filedialog
from collections import defaultdict
from typing import Set, Dict, Any, Generator, List, Tuple

def flatten_with_types(
    data: Dict[str, Any],
    parent_key: str = '',
    separator: str = '_'
) -> Generator[Tuple[str, str], None, None]:
    """
    Funzione ricorsiva per "appiattire" le chiavi di un oggetto JSON
    e restituire per ciascuna il suo tipo di dato Python.
    """
    for key, value in data.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        type_name = type(value).__name__
        
        # Restituisce la chiave e il tipo del livello corrente
        yield new_key, type_name
        
        # Caso ricorsivo: se il valore è un dizionario, continua a scavare.
        if isinstance(value, dict) and value:
            yield from flatten_with_types(value, new_key, separator)

def analyze_files_for_schema(
    file_paths: List[str],
    separator: str = '_'
) -> Tuple[Dict[str, Set[str]], Dict[Tuple[str, str], Set[Tuple[str, str]]]]:
    """
    Analizza una lista di file .json.gz per estrarre uno schema completo
    con tipi di dato e una struttura gerarchica.
    """
    flat_schema = defaultdict(set)
    
    for file_path in file_paths:
        print(f"\nAnalizzando il file: {file_path}...")
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        if isinstance(record, dict):
                            for key, type_name in flatten_with_types(record, separator=separator):
                                flat_schema[key].add(type_name)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"Errore durante l'analisi di {file_path}: {e}")

    print("\nCostruzione della struttura gerarchica...")
    schema_tree = defaultdict(set)
    # Aggiungiamo una radice fittizia per contenere gli elementi di primo livello
    root_node_key = ('root', 'object')

    for key, types in flat_schema.items():
        type_str = ", ".join(sorted(list(types)))
        
        # Se la chiave non ha un separatore, è un figlio della radice.
        if separator not in key:
            schema_tree[root_node_key].add((key, type_str))
        else:
            # Altrimenti, trova il suo genitore
            parent_key_parts = key.split(separator)[:-1]
            parent_key = separator.join(parent_key_parts)
            child_key = key.split(separator)[-1]
            
            # Cerca il tipo del genitore
            parent_types = flat_schema.get(parent_key, {'dict'}) # Default a 'dict' se non trovato
            parent_type_str = ", ".join(sorted(list(parent_types)))
            
            parent_node_key = (parent_key, parent_type_str)
            schema_tree[parent_node_key].add((child_key, type_str))
            
    return flat_schema, schema_tree

def select_files() -> List[str]:
    """Apre una finestra di dialogo per selezionare uno o più file."""
    root = tk.Tk()
    root.withdraw()
    file_paths = filedialog.askopenfilenames(
        title="Seleziona uno o più file .json.gz",
        filetypes=[("File JSON compressi", "*.json.gz"), ("Tutti i file", "*.*")]
    )
    return list(file_paths)


# --- ESECUZIONE DELLO SCRIPT ---
if __name__ == "__main__":
    NOME_FILE_OUTPUT = "report_schema.txt"
    selected_files = select_files()
    
    if selected_files:
        flat_schema_result, schema_tree_result = analyze_files_for_schema(selected_files)
        
        if flat_schema_result:
            with open(NOME_FILE_OUTPUT, "w", encoding="utf-8") as f:
                print(f"Salvataggio dell'output nel file: {NOME_FILE_OUTPUT}...")
                
                f.write(f"\n--- 2. Dizionario della Struttura Gerarchica ---\n")
                for parent, children in sorted(schema_tree_result.items()):
                    f.write(f"\nGenitore: {parent}\n")
                    for child in sorted(list(children)):
                        f.write(f"  -> Figlio: {child}\n")
        else:
            print("\nNessuno schema trovato nei file selezionati.")
    else:
        print("Nessun file selezionato. Esecuzione terminata.")