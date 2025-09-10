import os
import json
import streamlit as st
from src.config import Config, DatabaseConfig
import streamlit.components.v1 as components

def force_open_sidebar():
    """
    Usa JavaScript per forzare l'apertura della sidebar se è chiusa.
    """
    components.html(
        """
        <script>
            const sidebar = window.parent.document.querySelector('[data-testid="stSidebar"]');
            if (sidebar && sidebar.getAttribute('aria-expanded') === 'false') {
                const openButton = window.parent.document.querySelector('button[aria-label="Open sidebar"]');
                if (openButton) {
                    openButton.click();
                }
            }
        </script>
        """,
        height=0,
    )

def force_close_sidebar():
    """
    Usa JavaScript per forzare la chiusura della sidebar se è aperta.
    """
    components.html(
        """
        <script>
            const sidebar = window.parent.document.querySelector('[data-testid="stSidebar"]');
            if (sidebar && sidebar.getAttribute('aria-expanded') === 'true') {
                const closeButton = window.parent.document.querySelector('button[aria-label="Close sidebar"]');
                if (closeButton) {
                    closeButton.click();
                }
            }
        </script>
        """,
        height=0,
    )

def get_twitter_query_templates():
    """
    Carica i template delle query da un file JSON, garantendo la retrocompatibilità.
    Converte al volo le query in formato stringa (vecchio) nel formato oggetto (nuovo).
    """
    file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'custom_query.json')
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            templates = json.load(f)

        for category, queries in templates.items():
            for query_name, query_value in queries.items():
                
                if isinstance(query_value, str):
                    formatted_query = query_value.format(temp_view_name=DatabaseConfig.TEMP_VIEW_NAME)
                    templates[category][query_name] = {
                        "query": formatted_query,
                        "charts": []
                    }
                elif isinstance(query_value, dict) and 'query' in query_value:
                    query_value['query'] = query_value['query'].format(
                        temp_view_name=DatabaseConfig.TEMP_VIEW_NAME
                    )
                else:
                    print(f"Attenzione: la query '{query_name}' ha un formato non valido e sarà ignorata.")
        
        return templates
    
    except FileNotFoundError:
        st.error(f"Errore: file dei template '{file_path}' non trovato.")
        return {}
    except Exception as e:
        st.error(f"Errore durante la lettura o la normalizzazione del file JSON: {e}")
        return {}