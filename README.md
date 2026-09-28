# Disaster Analytics — Spark Edition

**Streamlit + PySpark** application to explore large collections of tweets about natural disasters (for example Hurricane Harvey, 2017). It loads JSON data, queries it with SQL, visualizes it and applies distributed machine-learning algorithms.

University project for the Databases course. The interface and comments are in Italian.

## Features

- **Data loading**: JSON or JSON Lines files, also compressed as `.gz`, read with a general Twitter schema (`schema_generale.json`) and, when needed, automatic inference.
- **SQL editor**: Spark SQL queries on the `disasters` view, with saveable query templates (`custom_query.json`), history and export.
- **Charts**: configurable on the result of each query (Plotly).
- **Machine learning on Spark**:
  - clustering: K-Means, DBSCAN;
  - supervised classification and regression;
  - anomaly detection: Isolation Forest.

## Requirements

- Python 3.10+
- Java 17 (for PySpark)
- On Windows: Hadoop `winutils` in `%USERPROFILE%\hadoop-3.3.6` (see below)

## Installation

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Running

```bash
streamlit run app.py
```

On Windows you can use `run_app.bat`, which sets up Java, Spark and Hadoop, creates the `.venv` virtualenv, starts the Spark History Server and launches Streamlit. The file must be adapted to the paths on your machine (`JAVA_HOME`, `SPARK_HOME`, `HADOOP_HOME`).

The data is not in the repository: load it from the interface. The `data/` folders and `.parquet` files are ignored by git.

## Structure

| Path | Content |
|---|---|
| `app.py` | Streamlit entry point |
| `pages_logic/` | Pages: home, analysis with charts and ML, query editor |
| `src/data_loader.py` | File reading and schema handling |
| `src/spark_manager.py` | Spark session creation and configuration |
| `utils/` | Scripts to inspect schemas, convert to Parquet and repair JSON |
| `schema_generale.json` | Unified Twitter schema used to read the data |
| `custom_query.json` | Saved queries |

## Utilities

```bash
python utils/read_schemas.py                        # inspects the files in data/File_compressi and converts them to Parquet
python utils/json_fixer.py input.json output.json   # repairs a malformed JSON
```

## Notes

The Spark session is configured for a machine with plenty of memory (8 GB driver): lower `spark.driver.memory` in `src/spark_manager.py` if needed.
