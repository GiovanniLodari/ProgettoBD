"""
Versione potenziata del SchemaManager che mantiene tutti gli schemi esistenti
e aggiunge il fallback automatico a String per campi problematici
"""
import os
import pandas as pd
import logging
from typing import Optional, Union, List
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    ArrayType, BooleanType, DoubleType, IntegerType, MapType
)
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from src.spark_manager import SparkManager
from src.config import Config
from functools import reduce
from pyspark.storagelevel import StorageLevel
import gzip
import json
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, ArrayType
)
import shutil

logger = logging.getLogger(__name__)

def _json_to_spark_type(json_type):
    """Funzione ausiliaria che converte ricorsivamente un tipo dal formato JSON a quello PySpark."""
    
    # Caso base per tipi semplici come "string", "long", etc.
    if isinstance(json_type, str):
        type_mapping = {
            "string": StringType(),
            "long": LongType(),
            "boolean": BooleanType()
        }
        return type_mapping.get(json_type, StringType())

    # Caso ricorsivo per tipi complessi (oggetti e array)
    if isinstance(json_type, dict):
        complex_type = json_type.get("type")
        
        if complex_type == "struct":
            # Se è un oggetto, ricrea la StructType chiamando la funzione per ogni campo figlio
            fields = [
                StructField(
                    field['name'],
                    _json_to_spark_type(field['type']),
                    field.get('nullable', True)
                ) for field in json_type.get('fields', [])
            ]
            return StructType(fields)
            
        elif complex_type == "array":
            # Se è un array, ricrea l'ArrayType chiamando la funzione per il tipo degli elementi
            element_type = _json_to_spark_type(json_type['elementType'])
            contains_null = json_type.get('containsNull', True)
            return ArrayType(element_type, contains_null)

    # Se il tipo non è riconosciuto, usa StringType come default sicuro
    return StringType()


class SchemaManager:
    """
    Gestisce la generazione dello schema Twitter leggendolo da un file JSON.
    Lo schema viene messo in cache dopo la prima lettura per efficienza.
    """
    
    _schema_cache = None # Variabile di classe per la cache

    @staticmethod
    def get_twitter_schema(json_path=r"C:\Users\giova\Desktop\ProgettoBD\schema_generale.json"):
        """
        Carica lo schema Twitter da un file JSON e lo converte in uno schema PySpark.
        
        Args:
            json_path (str): Il percorso del file schema_generale.json.
        
        Returns:
            StructType: Lo schema PySpark completo e pronto all'uso.
        """
        # Se lo schema è già stato generato, lo restituisce dalla cache
        if SchemaManager._schema_cache:
            return SchemaManager._schema_cache

        try:
            print(f"Lettura e generazione dello schema dal file: {json_path}")
            # Apre, legge e interpreta il file JSON
            with open(json_path, 'r', encoding='utf-8') as f:
                json_schema = json.load(f)
            
            # Converte la struttura JSON in uno schema PySpark
            spark_schema = _json_to_spark_type(json_schema)
            
            # Salva lo schema in cache per le prossime chiamate
            SchemaManager._schema_cache = spark_schema
            print("Schema generato e messo in cache con successo.")
            
            return spark_schema

        except FileNotFoundError:
            print(f"ERRORE: Il file di schema '{json_path}' non è stato trovato.")
            return None
        except Exception as e:
            print(f"Si è verificato un errore durante la generazione dello schema: {e}")
            return None
    
    # @staticmethod
    # def get_twitter_schema():
    #     """
    #     MANTIENE lo schema Twitter originale completo che hai sviluppato.
    #     Questo schema rimane identico - il fallback viene gestito nelle opzioni di caricamento.
    #     """
    #     # --- PASSO 1: I SOTTO-SCHEMI, CORRETTI E RESI FLESSIBILI ---

    #     # Schema per le URL, con 'unwound' opzionale
    #     url_schema = StructType([
    #         StructField("display_url", StringType(), True),
    #         StructField("expanded_url", StringType(), True),
    #         StructField("indices", ArrayType(LongType()), True),
    #         StructField("url", StringType(), True),
    #         StructField("unwound", StructType([
    #             StructField("description", StringType(), True),
    #             StructField("status", LongType(), True),
    #             StructField("title", StringType(), True),
    #             StructField("url", StringType(), True)
    #         ]), True)
    #     ])

    #     # Schema per i media, ora più completo e tollerante
    #     media_schema = StructType([
    #         StructField("display_url", StringType(), True),
    #         StructField("expanded_url", StringType(), True),
    #         StructField("id", LongType(), True),
    #         StructField("id_str", StringType(), True),
    #         StructField("indices", ArrayType(LongType()), True),
    #         StructField("media_url", StringType(), True),
    #         StructField("media_url_https", StringType(), True),
    #         StructField("sizes", StructType([
    #             StructField("large", StructType([
    #                 StructField("h", LongType(), True), 
    #                 StructField("resize", StringType(), True), 
    #                 StructField("w", LongType(), True)
    #             ]), True),
    #             StructField("medium", StructType([
    #                 StructField("h", LongType(), True), 
    #                 StructField("resize", StringType(), True), 
    #                 StructField("w", LongType(), True)
    #             ]), True),
    #             StructField("small", StructType([
    #                 StructField("h", LongType(), True), 
    #                 StructField("resize", StringType(), True), 
    #                 StructField("w", LongType(), True)
    #             ]), True),
    #             StructField("thumb", StructType([
    #                 StructField("h", LongType(), True), 
    #                 StructField("resize", StringType(), True), 
    #                 StructField("w", LongType(), True)
    #             ]), True)
    #         ]), True),
    #         StructField("type", StringType(), True),
    #         StructField("url", StringType(), True),
    #         # Campi opzionali trovati nell'errore
    #         StructField("source_status_id", LongType(), True),
    #         StructField("source_status_id_str", StringType(), True),
    #         StructField("source_user_id", LongType(), True),
    #         StructField("source_user_id_str", StringType(), True),
    #         StructField("video_info", StructType([
    #             StructField("aspect_ratio", ArrayType(LongType()), True),
    #             StructField("duration_millis", LongType(), True),
    #             StructField("variants", ArrayType(StructType([
    #                 StructField("bitrate", LongType(), True),
    #                 StructField("content_type", StringType(), True),
    #                 StructField("url", StringType(), True)
    #             ])), True)
    #         ]), True)
    #     ])

    #     # Schema per le entities, con 'symbols' corretto
    #     entities_schema = StructType([
    #         StructField("hashtags", ArrayType(StructType([
    #             StructField("indices", ArrayType(LongType()), True), 
    #             StructField("text", StringType(), True)
    #         ])), True),
    #         StructField("media", ArrayType(media_schema), True),
    #         StructField("urls", ArrayType(url_schema), True),
    #         StructField("user_mentions", ArrayType(StructType([
    #             StructField("id", LongType(), True), 
    #             StructField("id_str", StringType(), True),
    #             StructField("indices", ArrayType(LongType()), True), 
    #             StructField("name", StringType(), True),
    #             StructField("screen_name", StringType(), True)
    #         ])), True),
    #         # CORREZIONE: symbols come array di struct
    #         StructField("symbols", ArrayType(StructType([
    #             StructField("indices", ArrayType(LongType()), True), 
    #             StructField("text", StringType(), True)
    #         ])), True)
    #     ])

    #     # Schema per l'oggetto user, ora più completo e tollerante
    #     user_schema = StructType([
    #         StructField("id", LongType(), True),
    #         StructField("id_str", StringType(), True),
    #         StructField("name", StringType(), True),
    #         StructField("screen_name", StringType(), True),
    #         StructField("location", StringType(), True),
    #         StructField("description", StringType(), True),
    #         StructField("url", StringType(), True),
    #         StructField("entities", StructType([
    #             StructField("description", StructType([
    #                 StructField("urls", ArrayType(url_schema), True)
    #             ]), True),
    #             StructField("url", StructType([
    #                 StructField("urls", ArrayType(url_schema), True)
    #             ]), True)
    #         ]), True),
    #         StructField("followers_count", LongType(), True),
    #         StructField("friends_count", LongType(), True),
    #         StructField("listed_count", LongType(), True),
    #         StructField("created_at", StringType(), True),
    #         StructField("favourites_count", LongType(), True),
    #         StructField("utc_offset", LongType(), True),
    #         StructField("time_zone", StringType(), True),
    #         StructField("geo_enabled", BooleanType(), True),
    #         StructField("verified", BooleanType(), True),
    #         StructField("statuses_count", LongType(), True),
    #         StructField("lang", StringType(), True),
    #         StructField("contributors_enabled", BooleanType(), True),
    #         StructField("is_translator", BooleanType(), True),
    #         StructField("is_translation_enabled", BooleanType(), True),
    #         StructField("profile_background_color", StringType(), True),
    #         StructField("profile_background_image_url", StringType(), True),
    #         StructField("profile_background_image_url_https", StringType(), True),
    #         StructField("profile_background_tile", BooleanType(), True),
    #         StructField("profile_image_url", StringType(), True),
    #         StructField("profile_image_url_https", StringType(), True),
    #         StructField("profile_banner_url", StringType(), True),
    #         StructField("profile_link_color", StringType(), True),
    #         StructField("profile_sidebar_border_color", StringType(), True),
    #         StructField("profile_sidebar_fill_color", StringType(), True),
    #         StructField("profile_text_color", StringType(), True),
    #         StructField("profile_use_background_image", BooleanType(), True),
    #         StructField("has_extended_profile", BooleanType(), True),
    #         StructField("default_profile", BooleanType(), True),
    #         StructField("default_profile_image", BooleanType(), True),
    #         StructField("protected", BooleanType(), True),
    #         StructField("translator_type", StringType(), True),
    #         # CORREZIONE: Questi campi sono stringhe, non booleani
    #         StructField("following", StringType(), True),
    #         StructField("follow_request_sent", StringType(), True),
    #         StructField("notifications", StringType(), True)
    #     ])

    #     bounding_box_schema = StructType([
    #         StructField("coordinates", StringType(), True),
    #         StructField("type", StringType(), True)
    #     ])

    #     place_schema = StructType([
    #         StructField("id", StringType(), True),
    #         StructField("url", StringType(), True),
    #         StructField("place_type", StringType(), True),
    #         StructField("name", StringType(), True),
    #         StructField("full_name", StringType(), True),
    #         StructField("country_code", StringType(), True),
    #         StructField("country", StringType(), True),
    #         StructField("bounding_box", bounding_box_schema, True)
    #     ])
        
    #     coordinates_schema = StructType([
    #         StructField("coordinates", StringType(), True),
    #         StructField("type", StringType(), True)
    #     ])

    #     # --- SCHEMA BASE DEL TWEET (ORDINATO ALFABETICAMENTE) ---
    #     tweet_schema_base = StructType([
    #         StructField("contributors", StringType(), True),  # Campo mancante
    #         StructField("coordinates", coordinates_schema, True),
    #         StructField("created_at", StringType(), True),
    #         StructField("entities", entities_schema, True),
    #         StructField("extended_entities", entities_schema, True),
    #         StructField("favorite_count", LongType(), True),
    #         StructField("favorited", BooleanType(), True),
    #         StructField("filter_level", StringType(), True),  # Campo mancante
    #         StructField("full_text", StringType(), True),
    #         StructField("geo", StringType(), True),
    #         StructField("id", LongType(), True),
    #         StructField("id_str", StringType(), True),
    #         StructField("in_reply_to_screen_name", StringType(), True),
    #         StructField("in_reply_to_status_id", LongType(), True),
    #         StructField("in_reply_to_status_id_str", StringType(), True),  # Campo mancante
    #         StructField("in_reply_to_user_id", LongType(), True),
    #         StructField("in_reply_to_user_id_str", StringType(), True),  # Campo mancante
    #         StructField("is_quote_status", BooleanType(), True),
    #         StructField("lang", StringType(), True),
    #         StructField("place", place_schema, True),
    #         StructField("possibly_sensitive", BooleanType(), True),
    #         StructField("quote_count", LongType(), True),
    #         StructField("quoted_status_id", LongType(), True),
    #         StructField("quoted_status_id_str", StringType(), True),  # Campo mancante
    #         StructField("reply_count", LongType(), True),
    #         StructField("retweet_count", LongType(), True),
    #         StructField("retweeted", BooleanType(), True),
    #         StructField("source", StringType(), True),
    #         StructField("text", StringType(), True),
    #         StructField("timestamp_ms", StringType(), True),
    #         StructField("truncated", BooleanType(), True),
    #         StructField("user", user_schema, True),
    #     ])

    #     # Schema finale con campi ricorsivi
    #     final_tweet_schema = StructType(
    #         tweet_schema_base.fields + [
    #             StructField("quoted_status", tweet_schema_base, True),
    #             StructField("retweeted_status", tweet_schema_base, True)
    #         ]
    #     )

    #     return final_tweet_schema

    @staticmethod
    def get_generic_schema():
        """Schema generico per dati sconosciuti"""
        return None  # Lascia che Spark inferisca automaticamente
    
    @staticmethod
    def detect_data_type(sample_data: dict) -> str:
        """
        Rileva automaticamente il tipo di dati basandosi su un campione
        
        Args:
            sample_data (dict): Campione di dati
            
        Returns:
            str: Tipo di dati rilevato ('twitter', 'generic')
        """
        # Controlli per identificare dati Twitter
        twitter_indicators = ['created_at', 'user', 'entities', 'text', 'id', 'id_str', 'retweet_count']
        
        if isinstance(sample_data, dict):
            found_indicators = sum(1 for indicator in twitter_indicators if indicator in sample_data)
            if found_indicators >= 3:  # Se trova almeno 3 indicatori Twitter
                return 'twitter'
        
        return 'generic'

    @staticmethod
    def create_permissive_reader(spark, file_format='json', custom_schema=None):
        """
        NUOVA FUNZIONE: Crea un reader Spark con opzioni permissive che mantiene lo schema
        ma gestisce automaticamente i campi problematici
        """
        if file_format == 'json':
            reader = spark.read \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .option("allowUnquotedFieldNames", "true") \
                .option("allowSingleQuotes", "true") \
                .option("allowNumericLeadingZeros", "true") \
                .option("allowBackslashEscapingAnyCharacter", "true") \
                .option("allowUnquotedControlChars", "true") \
                .option("dropFieldIfAllNull", "false") \
                .option("prefersDecimal", "false")  # Evita problemi con decimali molto grandi
                #.option("multiline", "true")
                
        elif file_format == 'csv':
            reader = spark.read \
                .option("header", "true") \
                .option("encoding", "UTF-8") \
                .option("escape", '"') \
                .option("quote", '"') \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .option("ignoreLeadingWhiteSpace", "true") \
                .option("ignoreTrailingWhiteSpace", "true") \
                .option("nullValue", "") \
                .option("emptyValue", "") \
                .option("inferSchema", "true" if custom_schema is None else "false")
                #.option("multiline", "true")
                
        elif file_format == 'parquet':
            reader = spark.read
            # Parquet ha schema embedded, meno problematico
            
        else:
            # Default a JSON
            reader = SchemaManager.create_permissive_reader(spark, 'json', custom_schema)
            
        # Applica lo schema se fornito
        if custom_schema is not None:
            reader = reader.schema(custom_schema)
            logger.info(f"Schema applicato: {len(custom_schema.fields)} campi")
        
        return reader

    @staticmethod 
    def load_with_schema_and_fallback(spark, file_path: str, file_format: str, schema_type: str = 'auto'):
        """
        NUOVA FUNZIONE PRINCIPALE: Carica i dati usando gli schemi esistenti + fallback automatico
        
        Args:
            spark: Sessione Spark
            file_path: Percorso del file
            file_format: Formato del file ('json', 'csv', 'parquet')
            schema_type: Tipo di schema da usare ('twitter', 'generic', 'auto')
            
        Returns:
            SparkDataFrame: DataFrame caricato con schema ottimale
        """
        logger.info(f"Caricamento con schema e fallback: {file_path} (formato: {file_format}, schema: {schema_type})")
        
        # 1. Determina lo schema da usare (mantiene la logica esistente)
        target_schema = None
        if schema_type == 'twitter':
            target_schema = SchemaManager.get_twitter_schema()
            logger.info("Usando schema Twitter completo")
        elif schema_type == 'generic':
            target_schema = SchemaManager.get_generic_schema()
            logger.info("Usando schema generico (inferenza)")
        elif schema_type == 'auto':
            # Prova a rilevare automaticamente
            if file_format == 'json':
                try:
                    # Leggi un piccolo campione per auto-detection
                    with open(file_path, 'r', encoding='utf-8') as f:
                        import json
                        first_line = f.readline()
                        sample_data = json.loads(first_line)
                        detected_type = SchemaManager.detect_data_type(sample_data)
                        
                        if detected_type == 'twitter':
                            target_schema = SchemaManager.get_twitter_schema()
                            logger.info("Auto-rilevato: schema Twitter")
                        else:
                            target_schema = None
                            logger.info("Auto-rilevato: schema generico")
                except Exception as e:
                    logger.warning(f"Auto-detection fallita: {e}, uso inferenza")
                    target_schema = None
        
        # 2. Crea il reader permissivo con lo schema appropriato
        reader = SchemaManager.create_permissive_reader(spark, file_format, target_schema)
        
        # 3. Carica i dati
        try:
            if file_format == 'json':
                df = reader.json(file_path)
            elif file_format == 'csv':
                df = reader.csv(file_path)  
            elif file_format == 'parquet':
                df = reader.parquet(file_path)
            else:
                raise ValueError(f"Formato non supportato: {file_format}")

            if target_schema is not None:
                df = DataLoader.coerce_to_string(df, target_schema)
            
            # 4. Report sui risultati
            total_count = df.count()
            logger.info(f"Caricati {total_count} record")
            
            # Controlla record corrotti
            if "_corrupt_record" in df.columns:
                corrupt_count = df.filter(F.col("_corrupt_record").isNotNull()).count()
                if corrupt_count > 0:
                    corrupt_percentage = (corrupt_count / total_count) * 100
                    logger.warning(f"Record con problemi: {corrupt_count}/{total_count} ({corrupt_percentage:.1f}%)")
                    
                    # Mostra esempi di record corrotti per debug
                    examples = df.filter(F.col("_corrupt_record").isNotNull()) \
                                .select("_corrupt_record").limit(3).collect()
                    for i, ex in enumerate(examples):
                        logger.debug(f"Esempio problema {i+1}: {ex._corrupt_record[:200]}...")
                else:
                    logger.info("Nessun record corrotto - schema perfettamente compatibile!")
            
            # 5. Log dello schema applicato
            logger.info(f"Schema finale: {len(df.schema.fields)} colonne")
            if target_schema:
                matched_fields = sum(1 for field in target_schema.fields if field.name in df.columns)
                logger.info(f"Campi dello schema corrispondenti: {matched_fields}/{len(target_schema.fields)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Errore nel caricamento con schema: {e}")
            
            # FALLBACK FINALE: carica senza schema con massima permissività
            logger.info("FALLBACK: caricamento senza schema con massima permissività")
            try:
                fallback_reader = SchemaManager.create_permissive_reader(spark, file_format, custom_schema=None)
                
                if file_format == 'json':
                    df_fallback = fallback_reader.json(file_path)
                elif file_format == 'csv':
                    df_fallback = fallback_reader.option("inferSchema", "false").csv(file_path)  # Tutto String
                else:
                    df_fallback = fallback_reader.parquet(file_path)
                
                logger.info("Fallback riuscito - dati caricati senza schema predefinito")
                return df_fallback
                
            except Exception as e2:
                logger.error(f"Anche il fallback è fallito: {e2}")
                return None

    # MANTIENI tutte le funzioni esistenti di normalizzazione e unione
    @staticmethod
    def normalize_dataframe_to_schema(df: SparkDataFrame, target_schema: StructType) -> SparkDataFrame:
        """
        MANTIENE la funzione originale per normalizzazione - non cambia nulla
        """
        try:
            # Ottieni le colonne esistenti e target
            existing_cols = set(df.columns)
            target_fields = {field.name: field for field in target_schema.fields}
            
            # Lista delle colonne finali nell'ordine dello schema target
            select_expressions = []
            
            for field in target_schema.fields:
                col_name = field.name
                
                if col_name in existing_cols:
                    # Colonna esistente: cast al tipo corretto
                    try:
                        select_expressions.append(
                            F.col(col_name).cast(field.dataType).alias(col_name)
                        )
                    except Exception as cast_error:
                        # Se il cast fallisce, usa null
                        logger.warning(f"Cast fallito per {col_name}: {cast_error}")
                        select_expressions.append(
                            F.lit(None).cast(field.dataType).alias(col_name)
                        )
                else:
                    # Colonna mancante: aggiungi null del tipo corretto
                    select_expressions.append(
                        F.lit(None).cast(field.dataType).alias(col_name)
                    )
            
            # Seleziona tutte le colonne nell'ordine dello schema target
            normalized_df = df.select(*select_expressions)
            
            return normalized_df
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dello schema: {str(e)}")
            return df  # Fallback finale

    @staticmethod
    def safe_union_dataframes(df1: SparkDataFrame, df2: SparkDataFrame, target_schema: StructType) -> SparkDataFrame:
        """
        MANTIENE la funzione originale per unione sicura - non cambia nulla
        """
        try:
            # Normalizza entrambi i DataFrames allo stesso schema
            df1_normalized = SchemaManager.normalize_dataframe_to_schema(df1, target_schema)
            df2_normalized = SchemaManager.normalize_dataframe_to_schema(df2, target_schema)
            
            # Unisci i DataFrames
            return df1_normalized.union(df2_normalized)
            
        except Exception as e:
            logger.error(f"Errore nell'unione sicura: {str(e)}")
            return df1

class DataLoader:
    """Classe principale per il caricamento dei dati"""
    
    def __init__(self, spark_manager: SparkManager):
        """
        Inizializza il DataLoader
        
        Args:
            spark_manager (SparkManager): Istanza del manager Spark
        """
        self.spark_manager = spark_manager
        self.schema_manager = SchemaManager()
        self.current_dataset = None
        self.dataset_metadata = {}
    
    def load_single_file(self, file_path: str, file_format: str = None, schema_type: str = 'auto') -> Optional[SparkDataFrame]:
        """
        VERSIONE POTENZIATA: usa gli schemi esistenti + fallback automatico
        L'API rimane identica, ma internamente usa la nuova logica
        """
        logger.info(f"Caricamento file (versione potenziata): {file_path}")
        
        try:
            spark = self.spark_manager.get_spark_session()
            if not spark:
                logger.error("Sessione Spark non disponibile")
                return None
            
            # Determina il formato
            current_format = file_format or self._detect_format(file_path)
            logger.info(f"Formato rilevato: {current_format}")
            
            # USA LA NUOVA FUNZIONE che mantiene gli schemi + aggiunge fallback
            df = self.schema_manager.load_with_schema_and_fallback(
                spark, file_path, current_format, schema_type
            )
            
            if df is not None:
                # Ottimizza e crea vista (logica esistente)
                df = self.spark_manager.optimize_dataframe(df)
                self.spark_manager.create_temp_view(df, "temp_data")
                self._update_metadata(df, file_path)
                self.current_dataset = df
                
                logger.info(f"File caricato con successo: {df.count()} righe")
                return df
            else:
                logger.error(f"Errore nel caricamento del file: {file_path}")
                return None
                
        except Exception as e:
            logger.error(f"Errore nel caricamento del file {file_path}: {str(e)}", exc_info=True)
            return None

    # Sostituisci la tua funzione esistente in DataLoader con questa

    def load_multiple_files(self, file_paths: List[str], file_format: str = None, schema_type: str = 'auto') -> Optional[SparkDataFrame]:
        logger.info(f"Avvio caricamento di {len(file_paths)} file con strategia 'Normalizza-Post-Lettura'")
        
        spark = self.spark_manager
        if not file_paths:
            logger.error("Nessun file fornito per il caricamento.")
            return None

        # --- SETUP: Definisci lo schema master e la cartella di output ---
        master_schema = self.schema_manager.get_twitter_schema()
        if not master_schema:
            logger.error("Impossibile ottenere lo schema Twitter master. Caricamento interrotto.")
            return None
        logger.info("Utilizzo dello schema Twitter come 'master schema' per la normalizzazione.")
        
        home_directory = os.path.expanduser('~')
        schema_dir = os.path.join(home_directory, 'Desktop', 'schemas_output')
        os.makedirs(schema_dir, exist_ok=True)
        
        dataframes_normalizzati = []

        temp_parquet_dir = "temp_parquet_output"
        if os.path.exists(temp_parquet_dir):
            shutil.rmtree(temp_parquet_dir) # Pulisci la cartella da esecuzioni precedenti
        os.makedirs(temp_parquet_dir)

        # --- FASE 1: Carica e Normalizza ogni DataFrame individualmente ---
        for idx, file_path in enumerate(file_paths):
            try:
                logger.info(f"--- Processando file {idx+1}/{len(file_paths)}: {file_path} ---")
                current_format = file_format or self._detect_format(file_path)
                
                # 1a. Carica il singolo file lasciando che Spark indovini il suo schema specifico.
                df = self.schema_manager.load_with_schema_and_fallback(
                    spark, file_path, current_format, 'auto' # Forza l'inferenza per massima flessibilità
                )
                
                if df is None:
                    logger.warning(f"Salto del file {file_path} perché il caricamento ha restituito None.")
                    continue
                
                # 1b. Salva lo schema inferito per questo specifico file (per debug)
                try:
                    schema_dict = json.loads(df.schema.json())
                    schema_formatted_string = json.dumps(schema_dict, indent=4)
                    base_filename = os.path.basename(file_path)
                    schema_file_path = os.path.join(schema_dir, f"schema_{base_filename}.json")
                    with open(schema_file_path, "w", encoding="utf-8") as f:
                        f.write(schema_formatted_string)
                    logger.info(f"Schema inferito per {base_filename} salvato in: {schema_file_path}")
                except Exception as schema_error:
                    logger.error(f"Impossibile salvare lo schema per il file {file_path}: {schema_error}")

                # 1c. NORMALIZZA il DataFrame per renderlo conforme allo schema master.
                #     Questo è il passaggio cruciale che garantisce la coerenza.
                df_normalizzato = self.schema_manager.normalize_dataframe_to_schema(df, master_schema)
                
                # 1d. Aggiungi i metadati.
                df_normalizzato = df_normalizzato.withColumn("source_file", F.lit(os.path.basename(file_path))) \
                                                .withColumn("source_id", F.lit(idx + 1))
                
                #dataframes_normalizzati.append(df_normalizzato)
                df_normalizzato.write.mode("append").parquet(temp_parquet_dir)

                dataframes_normalizzati.append(df_normalizzato)

                logger.info(f"File {file_path} salvato correttamente in formato Parquet.")
                #logger.info(f"File {file_path} processato e normalizzato con successo.")

            except Exception as e:
                logger.error(f"Errore critico durante il processo del file {file_path}: {e}", exc_info=True)
                continue
                
        if not dataframes_normalizzati:
            logger.error("Nessun file è stato caricato e normalizzato con successo.")
            return None

        # --- FASE 2: Unisci i DataFrame, che ora sono tutti perfettamente compatibili ---
        logger.info(f"Unione di {len(dataframes_normalizzati)} DataFrame normalizzati...")
        # Usiamo un .union() semplice perché gli schemi sono ora identici
        combined_df = reduce(lambda df1, df2: df1.union(df2), dataframes_normalizzati)
        
        # --- FASE 3: Finalizza e ottieni il conteggio in modo sicuro ---
        # try:
        #     # Usa MEMORY_AND_DISK per evitare OutOfMemoryError.
        #     # Spark userà la RAM e, se non basta, il disco.
        #     combined_df.persist(StorageLevel.MEMORY_AND_DISK)

        #     # Ora che è persistito in modo sicuro, possiamo chiamare .count()
        #     total_records = combined_df.count()
        #     logger.info(f"Dataset combinato creato con successo: {total_records} righe totali")

        #     # Imposta il DataFrame per le query successive
        #     combined_df.createOrReplaceTempView("temp_data")
        #     self._update_metadata(combined_df, f"{len(file_paths)} files combined")
        #     self.current_dataset = combined_df
            
        #     logger.info("DataFrame finale pronto per le query.")
        #     return combined_df
        
        logger.info("Lettura del dataset Parquet unificato...")
        try:
            combined_df = spark.read.parquet(temp_parquet_dir)
            
            # Ora il conteggio è un'operazione efficiente sul formato Parquet
            total_records = combined_df.count()
            
            combined_df.show()

            logger.info(f"Conteggio finale riuscito. Totale record: {total_records}")

            combined_df.createOrReplaceTempView("temp_data")
            self._update_metadata(combined_df, f"{len(file_paths)} files combined")
            self.current_dataset = combined_df
            
            logger.info("DataFrame finale pronto per le query.")
            
            # Opzionale: pulisci la cartella temporanea alla fine
            # try:
            #     shutil.rmtree(temp_parquet_dir)
            #     logger.info(f"Cartella temporanea {temp_parquet_dir} rimossa.")
            # except Exception as e:
            #     logger.warning(f"Impossibile rimuovere la cartella temporanea: {e}")

            return combined_df


        except Exception as e:
            logger.error(f"Errore durante la finalizzazione del DataFrame (conteggio o persist): {e}", exc_info=True)
            # In caso di errore, rilascia la cache se è stata creata
            if 'combined_df' in locals():
                combined_df.unpersist()
            return None
        
    
    def _get_schema_for_type(self, schema_type: str, file_path: str = None) -> Optional[StructType]:
        """
        Ottiene lo schema appropriato basato sul tipo richiesto
        
        Args:
            schema_type (str): Tipo di schema ('auto', 'twitter', 'generic')
            file_path (str, optional): Percorso del file per auto-detect
            
        Returns:
            Optional[StructType]: Schema da applicare
        """
        if schema_type == 'twitter':
            logger.info("Usando schema Twitter")
            return self.schema_manager.get_twitter_schema()
        elif schema_type == 'generic':
            logger.info("Usando schema generico")
            return self.schema_manager.get_generic_schema()
        elif schema_type == 'auto':
            # Prova a rilevare automaticamente il tipo di dati
            if file_path and file_path.endswith('.json'):
                try:
                    # Leggi un piccolo campione per rilevare il tipo
                    with open(file_path, 'r', encoding='utf-8') as f:
                        import json
                        first_line = f.readline()
                        sample_data = json.loads(first_line)
                        detected_type = self.schema_manager.detect_data_type(sample_data)
                        
                        if detected_type == 'twitter':
                            logger.info("Rilevato automaticamente formato Twitter")
                            return self.schema_manager.get_twitter_schema()
                        
                except Exception as e:
                    logger.warning(f"Impossibile rilevare automaticamente il tipo: {e}")
            
            return None  # Default: inferenza automatica
        
        return None
    
    def _load_csv(self, spark, file_path: str, schema: Optional[StructType] = None) -> SparkDataFrame:
        """Carica un file CSV"""
        reader = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true" if schema is None else "false") \
            .option("encoding", "UTF-8") \
            .option("escape", '"')
            #.option("multiline", "true") \
        
        if schema:
            reader = reader.schema(schema)
            
        return reader.csv(file_path)
    
    def _load_json(self, spark, file_path: str, schema: Optional[StructType] = None) -> SparkDataFrame:
        """Carica un file JSON"""
        reader = spark.read.option("multiline", "true")
        
        if schema:
            reader = reader.schema(schema)
            
        return reader.json(file_path)
    
    def _load_parquet(self, spark, file_path: str, schema: Optional[StructType] = None) -> SparkDataFrame:
        """Carica un file Parquet"""
        reader = spark.read
        
        if schema:
            reader = reader.schema(schema)
            
        return reader.parquet(file_path)
    
    def _detect_format(self, file_path: str) -> str:
        """Rileva automaticamente il formato del file"""
        extension = file_path.lower().split('.')[-1]
        
        format_mapping = {
            'csv': 'csv',
            'json': 'json',
            'jsonl': 'json',
            'parquet': 'parquet',
            'pq': 'parquet'
        }
        
        return format_mapping.get(extension, 'csv')
    
    def _update_metadata(self, df: SparkDataFrame, file_path: str):
        """Aggiorna i metadata del dataset"""
        try:
            stats = SparkManager().get_dataframe_stats(df)
            if isinstance(file_path, str) and os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
            else:
                file_size = 0
            
            self.dataset_metadata = {
                'file_path': file_path,
                'file_size_bytes': file_size,
                'file_size_mb': file_size / (1024 * 1024),
                'record_count': stats.get('count', 0),
                'column_count': stats.get('columns', 0),
                'partitions': stats.get('partitions', 0),
                'schema': stats.get('schema', [])
            }
            logger.info(f"Metadati aggiornati per {file_path}.")
        except Exception as e:
            logger.warning(f"Impossibile aggiornare i metadati: {e}", exc_info=True)
    
    def get_sample_data(self, limit: int = 100) -> pd.DataFrame:
        """
        Ottieni un campione del dataset come Pandas DataFrame
        
        Args:
            limit (int): Numero massimo di righe da restituire
        
        Returns:
            pd.DataFrame: Campione dei dati
        """
        if self.current_dataset is None:
            return pd.DataFrame()
        
        try:
            return self.current_dataset.limit(limit).toPandas()
        except Exception as e:
            logger.error(f"Errore nel campionamento dei dati: {str(e)}")
            return pd.DataFrame()
    
    def get_column_info(self) -> pd.DataFrame:
        """
        Ottieni informazioni sulle colonne del dataset
        
        Returns:
            pd.DataFrame: Informazioni sulle colonne
        """
        if not self.dataset_metadata.get('schema'):
            return pd.DataFrame()
        
        schema_info = []
        for col_name, col_type in self.dataset_metadata['schema']:
            # Calcola statistiche base per colonne numeriche
            try:
                if self.current_dataset and 'int' in col_type.lower() or 'double' in col_type.lower() or 'float' in col_type.lower():
                    stats = self.current_dataset.select(col_name).describe().collect()
                    null_count = self.current_dataset.filter(f"`{col_name}` IS NULL").count()
                else:
                    stats = None
                    null_count = self.current_dataset.filter(f"`{col_name}` IS NULL").count() if self.current_dataset else 0
                
                schema_info.append({
                    'Colonna': col_name,
                    'Tipo': col_type,
                    'Valori Null': null_count,
                    'Statistiche': 'Disponibili' if stats else 'N/A'
                })
            except Exception:
                schema_info.append({
                    'Colonna': col_name,
                    'Tipo': col_type,
                    'Valori Null': 'N/A',
                    'Statistiche': 'N/A'
                })
        
        return pd.DataFrame(schema_info)
    
    def get_metadata(self) -> dict:
        """Ottieni i metadata del dataset corrente"""
        return self.dataset_metadata.copy()
    
    def validate_dataset(self) -> dict:
        """
        Valida il dataset e restituisce un report di qualità 
        
        Returns:
            dict: Report di validazione
        """
        if self.current_dataset is None:
            return {'status': 'error', 'message': 'Nessun dataset caricato'}
        
        try:
            df = self.current_dataset
            total_records = df.count()
            
            if total_records == 0:
                return {'status': 'warning', 'message': 'Dataset vuoto'}
            
            # Controlla colonne con tutti valori null
            null_columns = []
            for col_name in df.columns:
                null_count = df.filter(f"`{col_name}` IS NULL").count()
                if null_count == total_records:
                    null_columns.append(col_name)
            
            # Controlla duplicati (su tutte le colonne)
            duplicate_count = df.count() - df.dropDuplicates().count()
            
            validation_report = {
                'status': 'success',
                'total_records': total_records,
                'duplicate_records': duplicate_count,
                'columns_all_null': null_columns,
                'quality_score': self._calculate_quality_score(total_records, duplicate_count, len(null_columns), len(df.columns))
            }
            
            return validation_report
            
        except Exception as e:
            logger.error(f"Errore nella validazione: {str(e)}")
            return {'status': 'error', 'message': f'Errore durante la validazione: {str(e)}'}
    
    def _calculate_quality_score(self, total_records: int, duplicates: int, null_cols: int, total_cols: int) -> float:
        """Calcola un punteggio di qualità del dataset (0-100)"""
        if total_records == 0:
            return 0.0
        
        # Penalità per duplicati
        duplicate_penalty = min(50, (duplicates / total_records) * 100)
        
        # Penalità per colonne completamente null
        null_col_penalty = (null_cols / total_cols) * 30 if total_cols > 0 else 0
        
        # Punteggio base
        score = 100 - duplicate_penalty - null_col_penalty
        
        return max(0.0, min(100.0, score))
    
    @staticmethod
    def coerce_to_string(df, target_schema: StructType):
        """
        VERSZIONE CORRETTA: Converte in stringa i campi che potrebbero causare conflitti,
        ma solo se sono effettivamente di tipo complesso.
        """
        logger.info("Avvio coercizione intelligente dello schema...")
        select_exprs = []
        
        source_schema = {field.name: field.dataType for field in df.schema.fields}

        for target_field in target_schema.fields:
            col_name = target_field.name
            target_type = target_field.dataType

            if col_name in source_schema:
                source_type = source_schema[col_name]
                
                # Se i tipi sono già uguali, non fare nulla
                if source_type == target_type:
                    select_exprs.append(F.col(col_name))
                # Se il tipo di origine è complesso (Struct o Array)
                elif isinstance(source_type, (StructType, ArrayType)):
                    # E il target è una stringa, converti in JSON.
                    if target_type == StringType():
                        logger.warning(f"Coercizione di '{col_name}' da {source_type} a Stringa JSON.")
                        select_exprs.append(F.to_json(F.col(col_name)).alias(col_name))
                    # Altrimenti, prova un normale cast
                    else:
                        select_exprs.append(F.col(col_name).cast(target_type).alias(col_name))
                # Se il tipo di origine è semplice, fai un cast normale
                else:
                    select_exprs.append(F.col(col_name).cast(target_type).alias(col_name))
            else:
                # Se la colonna manca, aggiungila come null
                select_exprs.append(F.lit(None).cast(target_type).alias(col_name))
                
        return df.select(*select_exprs)
    
    @staticmethod
    def clean_twitter_dataframe(df: SparkDataFrame) -> SparkDataFrame | None:
        """
        Pulisce un DataFrame Spark di Twitter rimuovendo le colonne predefinite non necessarie.
        Args:
            df (DataFrame): Il DataFrame Spark di input da pulire.
        Returns:
            DataFrame | None: Il DataFrame pulito. Restituisce None se si verifica un errore.
        """
        # Lista delle colonne da eliminare (più facile da manutenere qui)
        columns_to_drop = [
            "id", "lang", "in_reply_to_status_id", "quoted_status_id", "possibly_sensitive", "source", "truncated",
            "favorited", "retweeted", "geo", "filter_level", "timestamp_ms",
            "contributors", "extended_entities", "in_reply_to_screen_name",
            "in_reply_to_status_id_str", "in_reply_to_user_id",
            "in_reply_to_user_id_str", "is_quote_status", "entities"
        ]

        user_columns_to_drop = [
            "id", "listed_count", "created_at", "utc_offset", "time_zone", "url", "entities",
            "geo_enabled", "contributors_enabled", "is_translator", "favourites_count",
            "is_translation_enabled", "profile_background_color", "statuses_count",
            "profile_background_image_url", "profile_background_image_url_https",
            "profile_background_tile", "profile_image_url", "profile_image_url_https",
            "profile_banner_url", "profile_link_color", "profile_sidebar_border_color",
            "profile_sidebar_fill_color", "profile_text_color",
            "profile_use_background_image", "has_extended_profile",
            "default_profile", "default_profile_image", "protected",
            "translator_type", "following", "follow_request_sent", "notifications"
        ]

        try:
            # 1. PULIZIA DEL LIVELLO PRINCIPALE (come prima)
            existing_top_level_cols = [col for col in columns_to_drop if col in df.columns]
            cleaned_df = df.drop(*existing_top_level_cols)
            
            # 2. PULIZIA DEI LIVELLI ANNIDATI (NUOVO PASSAGGIO)
            logger.info("Pulizia delle colonne annidate 'retweeted_status' e 'quoted_status'...")
            
            # Usiamo F.when per gestire in sicurezza i casi in cui i campi sono NULL
            cleaned_df = cleaned_df.withColumn(
                "retweeted_status",
                F.when(
                    F.col("retweeted_status").isNotNull(),
                    F.col("retweeted_status").dropFields(*columns_to_drop)
                ).otherwise(F.lit(None)) # Lascia NULL se era già NULL
            ).withColumn(
                "quoted_status",
                F.when(
                    F.col("quoted_status").isNotNull(),
                    F.col("quoted_status").dropFields(*columns_to_drop)
                ).otherwise(F.lit(None))
            ).withColumn(
                "user",
                F.when(
                    F.col("user").isNotNull(),
                    F.col("user").dropFields(*user_columns_to_drop)
                ).otherwise(F.lit(None))
            )

            logger.info("Pulizia completata con successo a tutti i livelli.")
            return cleaned_df

        except AnalysisException as e:
            # Errore specifico di Spark, ad es. se una colonna non viene trovata
            logger.error(f"Errore di analisi Spark durante la pulizia del DataFrame: {e}", exc_info=True)
            return None
        except Exception as e:
            # Qualsiasi altro errore imprevisto
            logger.error(f"Errore imprevisto durante la pulizia del DataFrame: {e}", exc_info=True)
            return None


class FileHandler:
    """Utility per gestire file uploadati"""
    
    @staticmethod
    def handle_uploaded_file(uploaded_file) -> Optional[str]:
        """
        Gestisce un file uploadato e lo salva temporaneamente
        Args:
            uploaded_file: File uploadato da Streamlit
        Returns:
            str: Percorso del file salvato o None se errore
        """
        try:
            # Crea directory temporanea se non esiste
            temp_dir = Config.TEMP_DIR
            os.makedirs(temp_dir, exist_ok=True)
            
            # Percorso del file temporaneo
            temp_path = os.path.join(temp_dir, uploaded_file.name)
            
            # Gestisci file compressi
            if uploaded_file.name.endswith('.gz'):
                # Decomprimi file gz
                with gzip.open(uploaded_file, 'rt') as gz_file:
                    content = gz_file.read()
                
                # Salva il contenuto decompresso
                decompressed_path = temp_path[:-3]  # Rimuovi .gz
                with open(decompressed_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                return decompressed_path
            else:
                # Salva file normale
                with open(temp_path, 'wb') as f:
                    f.write(uploaded_file.getbuffer())
                
                return temp_path
                
        except Exception as e:
            logger.error(f"Errore nella gestione del file: {str(e)}")
            return None
        
    @staticmethod
    def cleanup_temp_files(file_paths: list):
        """Rimuove file temporanei"""
        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"File temporaneo rimosso: {file_path}")
            except Exception as e:
                logger.warning(f"Impossibile rimuovere il file {file_path}: {str(e)}")