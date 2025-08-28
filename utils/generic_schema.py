import json
import os
from copy import deepcopy

def merge_types(type1, type2):
    """
    Funzione ricorsiva per unire due definizioni di tipo.
    Privilegia sempre il tipo più complesso (es. struct vince su string).
    """
    # Se il secondo tipo non è un dizionario (es. "string", "long"), è un tipo semplice.
    # Restituiamo questo, che sovrascrive il precedente.
    if not isinstance(type2, dict):
        return type2

    # Se il primo tipo è semplice ma il secondo è complesso (dict), vince il secondo.
    if not isinstance(type1, dict):
        return type2

    # Se entrambi sono dizionari, controlliamo il loro 'type' interno.
    type1_internal = type1.get('type')
    type2_internal = type2.get('type')

    # Se i tipi sono diversi (es. array vs struct), diamo priorità al secondo.
    if type1_internal != type2_internal:
        return type2

    # Se entrambi sono 'struct', dobbiamo unire le loro liste di campi.
    if type1_internal == 'struct':
        fields1 = type1.get('fields', [])
        fields2 = type2.get('fields', [])
        type1['fields'] = merge_field_lists(fields1, fields2)

    # Se entrambi sono 'array', dobbiamo unire ricorsivamente il loro 'elementType'.
    elif type1_internal == 'array':
        type1['elementType'] = merge_types(
            type1.get('elementType', {}),
            type2.get('elementType', {})
        )

    return type1

def merge_field_lists(list1, list2):
    """
    Unisce due liste di campi. Se un campo esiste in entrambe le liste,
    unisce ricorsivamente i loro tipi. Altrimenti, aggiunge i nuovi campi.
    """
    merged_fields_map = {field['name']: field for field in list1}

    for field_to_merge in list2:
        field_name = field_to_merge['name']
        
        if field_name in merged_fields_map:
            existing_field = merged_fields_map[field_name]
            existing_field['type'] = merge_types(
                existing_field.get('type', {}),
                field_to_merge.get('type', {})
            )
        else:
            merged_fields_map[field_name] = field_to_merge

    return list(merged_fields_map.values())

def get_max_depth(schema_node, current_depth=1):
    """
    Calcola ricorsivamente la profondità massima di un oggetto schema.
    """
    if not isinstance(schema_node, dict):
        return current_depth

    node_type = schema_node.get('type')
    
    if node_type == 'struct':
        fields = schema_node.get('fields', [])
        if not fields:
            return current_depth
        return max(get_max_depth(field['type'], current_depth + 1) for field in fields)
        
    elif node_type == 'array':
        return get_max_depth(schema_node.get('elementType', {}), current_depth + 1)
        
    return current_depth

def main():
    """
    Funzione principale che orchestra il processo.
    """
    try:
        desktop_path = os.path.join(os.path.join(os.path.expanduser('~')), 'Desktop')
        schemas_dir = os.path.join(desktop_path, 'schemas_output')

        if not os.path.isdir(schemas_dir):
            print(f"ERRORE: La cartella '{schemas_dir}' non è stata trovata.")
            print("Per favore, crea una cartella chiamata 'schemas' sul tuo Desktop e inserisci i file JSON al suo interno.")
            return

        schema_files = [f for f in os.listdir(schemas_dir) if f.endswith('.json')]

        if not schema_files:
            print(f"Nessun file .json trovato nella cartella '{schemas_dir}'.")
            return

        print(f"Trovati {len(schema_files)} file schema. Inizio l'unione...")

        with open(os.path.join(schemas_dir, schema_files[0]), 'r', encoding='utf-8') as f:
            general_schema = json.load(f)
        
        print(f"1. Caricato lo schema base da: {schema_files[0]}")

        for i, filename in enumerate(schema_files[1:], start=2):
            filepath = os.path.join(schemas_dir, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                current_schema = json.load(f)
            
            general_schema['fields'] = merge_field_lists(
                general_schema.get('fields', []),
                current_schema.get('fields', [])
            )
            print(f"{i}. Unito con successo lo schema da: {filename}")

        output_path = os.path.join(desktop_path, 'schema_generale.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(general_schema, f, indent=4)
            
        print(f"\n✅ Unione completata! Lo schema generale è stato salvato in:\n{output_path}")
        
        depth = get_max_depth(general_schema)
        print(f"➡️ La profondità massima di annidamento dello schema generale è: {depth}")

    except Exception as e:
        print(f"Si è verificato un errore: {e}")

if __name__ == "__main__":
    main()