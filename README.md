# Disaster Analytics — Spark Edition

Applicazione **Streamlit + PySpark** per esplorare grandi raccolte di tweet sui disastri naturali (per esempio l'uragano Harvey del 2017). Carica i dati JSON, li interroga con SQL, li visualizza e applica algoritmi di machine learning distribuiti.

Progetto per il corso di Basi di Dati.

## Funzionalità

- **Caricamento dati**: file JSON o JSON Lines, anche compressi `.gz`, letti con uno schema Twitter generale (`schema_generale.json`) e, se serve, con inferenza automatica.
- **Editor SQL**: query Spark SQL sulla vista `disasters`, con modelli di query salvabili (`custom_query.json`), cronologia ed esportazione.
- **Grafici**: barre, linee, dispersione e altri, configurabili sul risultato di ogni query (Plotly).
- **Machine learning su Spark**:
  - clustering: K-Means, DBSCAN;
  - classificazione e regressione supervisionata;
  - rilevamento di anomalie: Isolation Forest.

## Requisiti

- Python 3.10+
- Java 17 (per PySpark)
- Su Windows: Hadoop `winutils` in `%USERPROFILE%\hadoop-3.3.6` (vedi sotto)

## Installazione

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Avvio

```bash
streamlit run app.py
```

Su Windows si può usare `run_app.bat`, che imposta Java, Spark e Hadoop, crea il virtualenv `.venv`, avvia lo Spark History Server e lancia Streamlit. Il file va adattato ai percorsi della propria macchina (`JAVA_HOME`, `SPARK_HOME`, `HADOOP_HOME`).

I dati non sono nel repository: caricali dall'interfaccia. Le cartelle `data/` e i file `.parquet` sono ignorati da git.

## Struttura

| Percorso | Contenuto |
|---|---|
| `app.py` | Punto d'ingresso Streamlit |
| `pages_logic/` | Pagine: home, analisi con grafici e ML, editor di query |
| `src/data_loader.py` | Lettura dei file e gestione dello schema |
| `src/spark_manager.py` | Creazione e configurazione della sessione Spark |
| `utils/` | Script per ispezionare gli schemi, convertire in Parquet e riparare JSON |
| `schema_generale.json` | Schema Twitter unificato usato per leggere i dati |
| `custom_query.json` | Query salvate |

## Utility

```bash
python utils/read_schemas.py                     # ispeziona i file in data/File_compressi e li converte in Parquet
python utils/json_fixer.py input.json output.json   # ripara un JSON malformato
```

## Note

La sessione Spark è configurata per una macchina con molta memoria (driver da 8 GB): riduci `spark.driver.memory` in `src/spark_manager.py` se serve.
