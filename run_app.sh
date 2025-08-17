#!/bin/bash

# Script per avviare l'applicazione Streamlit con Spark
# Disaster Analysis App

echo "🌪️ Avvio dell'applicazione Streamlit per Analisi Disastri Naturali"
echo "=================================================================="

# Colori per output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Funzione per stampare messaggi colorati
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Controlla se siamo in WSL
if grep -q microsoft /proc/version; then
    print_status "Rilevato ambiente WSL"
    WSL_ENV=true
else
    print_status "Ambiente Linux nativo rilevato"
    WSL_ENV=false
fi

# Controlla dipendenze di sistema
print_status "Controllo dipendenze..."

# Verifica Java
if ! command -v java &> /dev/null; then
    print_error "Java non trovato. Installazione in corso..."
    sudo apt update
    sudo apt install openjdk-11-jdk -y
    if [ $? -eq 0 ]; then
        print_success "Java installato con successo"
    else
        print_error "Errore nell'installazione di Java"
        exit 1
    fi
else
    print_success "Java trovato: $(java -version 2>&1 | head -n 1)"
fi

# Verifica Python
if ! command -v python3 &> /dev/null; then
    print_error "Python3 non trovato"
    exit 1
else
    print_success "Python3 trovato: $(python3 --version)"
fi

# Configura variabili d'ambiente se non già impostate
if [ -z "$SPARK_HOME" ]; then
    print_warning "SPARK_HOME non configurato. Configurazione automatica..."
    
    # Cerca Spark nelle posizioni comuni
    SPARK_LOCATIONS=("/opt/spark" "$HOME/spark" "/usr/local/spark")
    
    for location in "${SPARK_LOCATIONS[@]}"; do
        if [ -d "$location" ]; then
            export SPARK_HOME="$location"
            print_success "SPARK_HOME impostato a: $SPARK_HOME"
            break
        fi
    done
    
    if [ -z "$SPARK_HOME" ]; then
        print_error "Spark non trovato. Installazione automatica..."
        
        # Scarica e installa Spark
        cd /tmp
        SPARK_VERSION="3.5.0"
        HADOOP_VERSION="3"
        SPARK_PACKAGE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
        
        print_status "Scaricamento Apache Spark ${SPARK_VERSION}..."
        wget -q "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz"
        
        if [ $? -eq 0 ]; then
            print_status "Estrazione e installazione..."
            tar xzf "${SPARK_PACKAGE}.tgz"
            sudo mv "$SPARK_PACKAGE" /opt/spark
            sudo chown -R $USER:$USER /opt/spark
            
            export SPARK_HOME="/opt/spark"
            print_success "Spark installato in: $SPARK_HOME"
            
            # Cleanup
            rm -f "${SPARK_PACKAGE}.tgz"
        else
            print_error "Errore nel download di Spark"
            exit 1
        fi
    fi
fi

# Configura PATH e altre variabili
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export JAVA_HOME=${JAVA_HOME:-$(readlink -f /usr/bin/java | sed "s:bin/java::")}
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Configura per WSL se necessario
if [ "$WSL_ENV" = true ]; then
    export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
    # Evita warning su WSL
    export SPARK_LOCAL_IP="127.0.0.1"
fi

print_success "Variabili d'ambiente configurate"

# Verifica/crea ambiente virtuale Python
VENV_NAME="disaster_analysis"
VENV_PATH="$HOME/$VENV_NAME"

if [ ! -d "$VENV_PATH" ]; then
    print_status "Creazione ambiente virtuale Python..."
    python3 -m venv "$VENV_PATH"
    if [ $? -eq 0 ]; then
        print_success "Ambiente virtuale creato: $VENV_PATH"
    else
        print_error "Errore nella creazione dell'ambiente virtuale"
        exit 1
    fi
fi

# Attiva ambiente virtuale
print_status "Attivazione ambiente virtuale..."
source "$VENV_PATH/bin/activate"
print_success "Ambiente virtuale attivato"

# Verifica/installa dipendenze Python
print_status "Controllo dipendenze Python..."
if [ -f "requirements.txt" ]; then
    print_status "Installazione dipendenze da requirements.txt..."
    pip install -r requirements.txt
    if [ $? -eq 0 ]; then
        print_success "Dipendenze installate con successo"
    else
        print_warning "Alcuni pacchetti potrebbero non essere stati installati correttamente"
    fi
else
    print_warning "File requirements.txt non trovato. Installazione manuale..."
    pip install streamlit pandas pyspark plotly seaborn matplotlib pyarrow
fi

# Verifica installazione Streamlit
if ! python3 -c "import streamlit" 2>/dev/null; then
    print_error "Streamlit non installato correttamente"
    exit 1
fi

# Verifica installazione PySpark
if ! python3 -c "import pyspark" 2>/dev/null; then
    print_error "PySpark non installato correttamente"
    exit 1
fi

print_success "Tutte le dipendenze verificate"

# Crea directory necessarie
print_status "Creazione directory di progetto..."
mkdir -p data
mkdir -p logs
mkdir -p temp

# Crea file __init__.py se non esistono
touch src/__init__.py
touch pages/__init__.py
touch utils/__init__.py

# Configura log Spark per ridurre verbosità
export SPARK_LOG_LEVEL="WARN"

# Verifica struttura progetto
print_status "Verifica struttura progetto..."
REQUIRED_FILES=("app.py" "src/config.py" "src/spark_manager.py")
MISSING_FILES=()

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        MISSING_FILES+=("$file")
    fi
done

if [ ${#MISSING_FILES[@]} -gt 0 ]; then
    print_error "File mancanti nel progetto:"
    for file in "${MISSING_FILES[@]}"; do
        echo "  - $file"
    done
    print_error "Assicurati di aver creato tutti i file necessari"
    exit 1
fi

print_success "Struttura progetto verificata"

# Test rapido Spark
print_status "Test connessione Spark..."
python3 -c "
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('TestApp').getOrCreate()
    print('✅ Spark connesso correttamente')
    spark.stop()
except Exception as e:
    print(f'❌ Errore Spark: {e}')
    exit(1)
" 2>/dev/null

if [ $? -ne 0 ]; then
    print_error "Test Spark fallito"
    exit 1
fi

print_success "Test Spark completato con successo"

# Determina porta disponibile
PORT=8501
while netstat -an | grep -q ":$PORT "; do
    PORT=$((PORT + 1))
done

print_success "Porta disponibile trovata: $PORT"

# Mostra informazioni pre-avvio
echo ""
echo "=================================================================="
print_success "CONFIGURAZIONE COMPLETATA"
echo "=================================================================="
echo "🚀 Ambiente: $(if [ "$WSL_ENV" = true ]; then echo "WSL"; else echo "Linux nativo"; fi)"
echo "☕ Java: $JAVA_HOME"
echo "⚡ Spark: $SPARK_HOME"
echo "🐍 Python: $VENV_PATH"
echo "🌐 Porta: $PORT"
echo "📁 Directory: $(pwd)"

if [ "$WSL_ENV" = true ]; then
    echo ""
    print_status "L'applicazione sarà accessibile da Windows all'indirizzo:"
    echo "🔗 http://localhost:$PORT"
else
    echo ""
    print_status "L'applicazione sarà accessibile all'indirizzo:"
    echo "🔗 http://localhost:$PORT"
fi

echo ""
echo "=================================================================="
print_status "Avvio Streamlit..."
echo "=================================================================="
echo ""

# Avvia l'applicazione
streamlit run app.py \
    --server.port $PORT \
    --server.address 0.0.0.0 \
    --server.headless true \
    --server.runOnSave true \
    --browser.gatherUsageStats false

# Cleanup al termine
print_status "Pulizia in corso..."
deactivate 2>/dev/null || true
print_success "Applicazione terminata correttamente"