import json
import sys

def fix_json(file_input, file_output, expected_keys=None):
    """
    Corregge un file contenente una sequenza di oggetti JSON concatenati.
    Utilizza un approccio di decoding incrementale per estrarre ogni oggetto
    JSON valido e li salva in un unico array JSON.
    """
    records_list = []
    
    try:
        with open(file_input, "r", encoding="utf-8") as f:
            # 1. Leggi l'intero contenuto del file.
            # Rimuoviamo eventuali spazi o "a capo" iniziali e finali.
            content = f.read().strip()

        # 2. Inizializziamo il decoder JSON e la posizione di partenza.
        decoder = json.JSONDecoder()
        pos = 0

        # 3. Cicliamo finché non abbiamo analizzato l'intera stringa.
        while pos < len(content):
            try:
                # 4. Tentiamo di decodificare un oggetto JSON a partire dalla posizione 'pos'.
                # raw_decode restituisce l'oggetto Python e la posizione dove si è fermato.
                record, end_pos = decoder.raw_decode(content, pos)
                
                # Logica per controllare le chiavi (identica alla tua)
                if isinstance(record, dict) and expected_keys:
                    for key in expected_keys:
                        if key not in record:
                            record[key] = None
                
                records_list.append(record)
                
                # 5. Aggiorniamo la posizione di partenza per la prossima iterazione.
                pos = end_pos
                
                # Ignoriamo eventuali spazi, virgole o "a capo" tra gli oggetti
                while pos < len(content) and content[pos].isspace():
                    pos += 1

            except json.JSONDecodeError:
                # Se troviamo un punto da cui non si può fare il parsing,
                # avanziamo di un carattere e riproviamo.
                # Questo aiuta a saltare eventuali caratteri corrotti tra oggetti validi.
                print(f"ATTENZIONE: Saltati dati corrotti alla posizione {pos}. Tento di ripartire.")
                pos += 1

    except FileNotFoundError:
        print(f"ERRORE: il file '{file_input}' non esiste.")
        sys.exit(1)
    except Exception as e:
        print(f"ERRORE IMPREVISTO: {e}")
        sys.exit(1)

    # 6. Salviamo la lista di record come un unico array JSON valido
    try:
        with open(file_output, "w", encoding="utf-8") as f:
            json.dump(records_list, f, indent=2, ensure_ascii=False)
        
        print(f"\n✔ Correzione completata. Trovati e salvati {len(records_list)} record validi.")
        print(f"✔ File salvato in: {file_output}")

    except Exception as e:
        print(f"ERRORE durante il salvataggio del file '{file_output}': {e}")


# La funzione main() rimane la stessa
def main():
    input_path = r"C:\Users\giova\Desktop\prova\170826213907_hurricane_harvey_2017_20170903_vol-2.json"
    output_path = r"C:\Users\giova\Desktop\170826213907_hurricane_harvey_2017_20170903_vol-2_CORRETTO.json"
    expected_keys = None # Imposta le chiavi se necessario, altrimenti lascialo None

    fix_json(input_path, output_path, expected_keys)


if __name__ == "__main__":
    main()