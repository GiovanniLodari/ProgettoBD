@echo off
REM ==================================================
REM Script per avviare Streamlit + Spark
REM ==================================================

echo 🌪️ Avvio applicazione Streamlit
echo ================================================================

REM ===== Deattiva ambiente virtuale =====
call "%VENV_PATH%\Scripts\deactivate.bat"

REM ===== Configura Java =====
REM Modifica questo percorso con quello della tua Java 17.0.12
set "JAVA_HOME=C:\Program Files\Java\jdk-17"
set "PATH=%JAVA_HOME%\bin;%PATH%"

REM Verifica Java
java -version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Java non trovato nella versione desiderata. Controlla installazione.
    pause
    exit /b 1
) else (
    for /f "delims=" %%i in ('java -version 2^>^&1') do set JAVA_VERSION=%%i & goto java_done
    :java_done
    echo [SUCCESS] Java trovato: %JAVA_VERSION%
)

REM ===== Verifica Python =====
for /f "delims=" %%i in ('where python') do set PYTHON_PATH=%%i & goto python_done
:python_done
if "%PYTHON_PATH%"=="" (
    echo [ERROR] Python non trovato. Controlla installazione.
    pause
    exit /b 1
)
for /f "delims=" %%i in ('"%PYTHON_PATH%" --version 2^>^&1') do set PYTHON_VERSION=%%i
echo [SUCCESS] Python trovato: %PYTHON_VERSION% (%PYTHON_PATH%)

REM ===== Configura Spark =====
set "SPARK_HOME=%USERPROFILE%\spark-4.0.0-bin-hadoop3"
echo [INFO] SPARK_HOME impostato a: %SPARK_HOME%

set "PATH=%PATH%;%SPARK_HOME%\bin;%SPARK_HOME%\sbin"

REM Verifica Spark
REM Esegue spark-submit senza mostrare l'output
cmd /c "spark-submit --version"
if errorlevel 1 (
    echo [ERROR] Spark non trovato o SPARK_HOME errato: %SPARK_HOME%
    pause
    exit /b 1
) else (
    echo [SUCCESS] Spark trovato!
)
echo ===== DEBUG: Starting Virtual Environment Section =====

REM ===== Configura Hadoop =====
set "HADOOP_HOME=%USERPROFILE%\hadoop-3.3.6"
echo [INFO] HADOOP_HOME impostato a: %HADOOP_HOME%
set "PATH=%PATH%;%HADOOP_HOME%\bin"
echo [INFO] Aggiunto HADOOP alla variabile d'ambiente PATH

REM ===== Ambiente virtuale =====
set "VENV_PATH=%USERPROFILE%\Desktop\ProgettoBD\venv"
if not exist "%VENV_PATH%\bin\activate.bat" (
    echo [INFO] Creazione ambiente virtuale Python...
    "%PYTHON_PATH%" -m venv "%VENV_PATH%"
    if errorlevel 1 (
        echo [ERROR] Creazione ambiente virtuale fallita
        exit /b 1
    )
)
echo [INFO] Attivazione ambiente virtuale...
call "%VENV_PATH%\Scripts\activate.bat"

REM ===== Installa dipendenze =====
if exist "requirements.txt" (
    echo [INFO] Installazione dipendenze da requirements.txt...
    pip install -r requirements.txt
) else (
    echo [WARNING] requirements.txt non trovato. Installazione manuale pacchetti essenziali...
    pip install streamlit pandas pyspark plotly seaborn matplotlib pyarrow
)

REM ===== Spark History Server =====
echo [INFO] Avvio Spark History Server...
start "Spark History Server" "%SPARK_HOME%\bin\spark-class.cmd" org.apache.spark.deploy.history.HistoryServer
start http://localhost:18080

REM ===== Controllo porta =====
set PORT=8501
:check_port
netstat -ano | findstr ":%PORT%" >nul
if %ERRORLEVEL%==0 (
    set /a PORT+=1
    goto check_port
)
echo [SUCCESS] Porta disponibile trovata: %PORT%

REM ===== Avvio Streamlit =====
echo [INFO] Avvio Streamlit...
streamlit run app.py --server.port %PORT% --server.address 127.0.0.1 --server.runOnSave true --browser.gatherUsageStats false

REM ===== Deattiva ambiente virtuale =====
call "%VENV_PATH%\Scripts\deactivate.bat"
echo [SUCCESS] Applicazione terminata correttamente
pause