
"""
Applicazione principale per l'analisi dei disastri naturali
"""

import streamlit as st
import sys
import logging
from pathlib import Path

current_dir = Path(__file__).parent
src_path = current_dir / "src"
sys.path.insert(0, str(src_path))

from src.spark_manager import cleanup_spark
from pages_logic.home import main as home_main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Analisi Disastri Naturali",
    page_icon="🌪️",
    layout="centered"
)

def main():
    """Funzione principale"""
           
    st.markdown("""
    <style>
        /* --- SIDEBAR --- */

        /* 1. Imposta la larghezza della sidebar */
        [data-testid="stSidebar"] {
            /* Puoi usare %, ma un valore fisso in 'rem' è spesso più stabile */
            width: 25rem !important;
        }

        /* 2. Rende il contenitore principale della sidebar un flexbox */
        [data-testid="stSidebar"] > div:first-child {
            display: flex;
            flex-direction: column;
            height: 100%;
            justify-content: space-between; /* Spinge il footer in basso */
        }
        
        /* Contenitore per i pulsanti del footer */
        .sidebar-footer {
            border-top: 1px solid rgba(255, 255, 255, 0.1);
            padding-top: 1rem;
        }

        /* 3. Centra il contenuto della pagina con una larghezza massima */
        [data-testid="stAppViewContainer"] .block-container {
            max-width: 1200px !important;
            margin-left: auto !important;
            margin-right: auto !important;
        }

        /* ---- Stile per le metriche "Card" in Orizzontale (con !important) ---- */

        [data-testid="stMetric"] {
            background-color: #2E2E2E;
            border: 1px solid #444444;
            padding: 15px;
            border-radius: 10px;
            
            /* REGOLE DI ALLINEAMENTO RINFORZATE */
            display: flex !important;          /* Attiva la modalità Flexbox con priorità */
            flex-direction: row !important;    /* Dispone gli elementi in riga */
            align-items: baseline !important;  /* Allinea il testo sulla stessa linea di base */
        }

        /* Modifichiamo l'etichetta per darle spazio e un aspetto pulito */
        [data-testid="stMetricLabel"] {
            margin-right: 10px !important; /* Aggiunge spazio tra l'etichetta e il valore */
            color: #A0A0A0 !important;
            font-weight: normal !important;
        }

        /* Opzionale: Rendiamo il valore leggermente meno grande per un look bilanciato */
        [data-testid="stMetricValue"] {
            font-size: 1.75rem !important;
            color: white !important;
        }
    </style>
    """, unsafe_allow_html=True)

    home_main()

def cleanup():
    """Pulizia risorse all'uscita"""
    try:
        cleanup_spark()
    except Exception as e:
        logger.error(f"Errore nella pulizia: {str(e)}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        st.write("Applicazione interrotta dall'utente")
        #cleanup()
    except Exception as e:
        st.error(f"Errore critico: {str(e)}")
        logger.error(f"Errore critico: {str(e)}")
        #cleanup()
    finally:
        cleanup()