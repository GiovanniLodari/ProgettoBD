"""
Gestisce l'upload dei dati con schemi flessibili e fallback automatico.
"""
import os
import streamlit as st
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
    """Funzione che converte ricorsivamente un tipo dal formato JSON a quello PySpark."""
    
    if isinstance(json_type, str):
        type_mapping = {
            "string": StringType(),
            "long": LongType(),
            "boolean": BooleanType()
        }
        return type_mapping.get(json_type, StringType())

    if isinstance(json_type, dict):
        complex_type = json_type.get("type")
        
        if complex_type == "struct":
            fields = [
                StructField(
                    field['name'],
                    _json_to_spark_type(field['type']),
                    field.get('nullable', True)
                ) for field in json_type.get('fields', [])
            ]
            return StructType(fields)
            
        elif complex_type == "array":
            element_type = _json_to_spark_type(json_type['elementType'])
            contains_null = json_type.get('containsNull', True)
            return ArrayType(element_type, contains_null)

    return StringType()


class SchemaManager:
    """
    Gestisce la generazione dello schema Twitter leggendolo da un file JSON.
    Lo schema viene messo in cache dopo la prima lettura per efficienza.
    """
    
    _schema_cache = None

    @staticmethod
    def get_twitter_schema(json_path=r"C:\Users\giova\Desktop\ProgettoBD\schema_generale.json"):
        """
        Carica lo schema Twitter da un file JSON e lo converte in uno schema PySpark.
        
        Args:
            json_path (str): Il percorso del file schema_generale.json.
        
        Returns:
            StructType: Lo schema PySpark completo e pronto all'uso.
        """
        if SchemaManager._schema_cache:
            return SchemaManager._schema_cache

        try:
            print(f"Lettura e generazione dello schema dal file: {json_path}")
            with open(json_path, 'r', encoding='utf-8') as f:
                json_schema = json.load(f)
            
            spark_schema = _json_to_spark_type(json_schema)
            
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
        return None
    
    @staticmethod
    def detect_data_type(sample_data: dict) -> str:
        """
        Rileva automaticamente il tipo di dati basandosi su un campione.
        """
        twitter_indicators = ['created_at', 'user', 'entities', 'text', 'id', 'id_str', 'retweet_count']
        
        if isinstance(sample_data, dict):
            found_indicators = sum(1 for indicator in twitter_indicators if indicator in sample_data)
            if found_indicators >= 3:
                return 'twitter'
        
        return 'generic'

    @staticmethod
    def create_permissive_reader(spark, file_format='json', custom_schema=None):
        """
        Crea un reader Spark con opzioni permissive che mantiene lo schema
        ma gestisce automaticamente i campi problematici.
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
                .option("prefersDecimal", "false")
                
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
                
        elif file_format == 'parquet':
            reader = spark.read
            
        else:
            reader = SchemaManager.create_permissive_reader(spark, 'json', custom_schema)
            
        if custom_schema is not None:
            reader = reader.schema(custom_schema)
            logger.info(f"Schema applicato: {len(custom_schema.fields)} campi")
        
        return reader

    @staticmethod 
    def load_with_schema_and_fallback(spark, file_path: str, file_format: str, schema_type: str = 'auto'):
        """
        Carica i dati usando gli schemi esistenti + fallback automatico.
        """
        logger.info(f"Caricamento con schema e fallback: {file_path} (formato: {file_format}, schema: {schema_type})")
        
        target_schema = None
        if schema_type == 'twitter':
            target_schema = SchemaManager.get_twitter_schema()
            logger.info("Usando schema Twitter completo")
        elif schema_type == 'generic':
            target_schema = SchemaManager.get_generic_schema()
            logger.info("Usando schema generico (inferenza)")
        elif schema_type == 'auto':
            if file_format == 'json':
                try:
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
        
        reader = SchemaManager.create_permissive_reader(spark, file_format, target_schema)
        
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
            
            total_count = df.count()
            logger.info(f"Caricati {total_count} record")
            
            if "_corrupt_record" in df.columns:
                corrupt_count = df.filter(F.col("_corrupt_record").isNotNull()).count()
                if corrupt_count > 0:
                    corrupt_percentage = (corrupt_count / total_count) * 100
                    logger.warning(f"Record con problemi: {corrupt_count}/{total_count} ({corrupt_percentage:.1f}%)")
                    
                    examples = df.filter(F.col("_corrupt_record").isNotNull()) \
                                .select("_corrupt_record").limit(3).collect()
                    for i, ex in enumerate(examples):
                        logger.debug(f"Esempio problema {i+1}: {ex._corrupt_record[:200]}...")
                else:
                    logger.info("Nessun record corrotto - schema perfettamente compatibile!")
            
            logger.info(f"Schema finale: {len(df.schema.fields)} colonne")
            if target_schema:
                matched_fields = sum(1 for field in target_schema.fields if field.name in df.columns)
                logger.info(f"Campi dello schema corrispondenti: {matched_fields}/{len(target_schema.fields)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Errore nel caricamento con schema: {e}")            
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

    @staticmethod
    def normalize_dataframe_to_schema(df: SparkDataFrame, target_schema: StructType) -> SparkDataFrame:
        """
        MANTIENE la funzione originale per normalizzazione - non cambia nulla
        """
        try:
            existing_cols = set(df.columns)
            target_fields = {field.name: field for field in target_schema.fields}
            
            select_expressions = []
            
            for field in target_schema.fields:
                col_name = field.name
                
                if col_name in existing_cols:
                    try:
                        select_expressions.append(
                            F.col(col_name).cast(field.dataType).alias(col_name)
                        )
                    except Exception as cast_error:
                        logger.warning(f"Cast fallito per {col_name}: {cast_error}")
                        select_expressions.append(
                            F.lit(None).cast(field.dataType).alias(col_name)
                        )
                else:
                    select_expressions.append(
                        F.lit(None).cast(field.dataType).alias(col_name)
                    )            
            normalized_df = df.select(*select_expressions)
            
            return normalized_df
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione dello schema: {str(e)}")
            return df

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

    def load_multiple_files(self, file_paths: List[str], file_format: str = None, schema_type: str = 'auto') -> Optional[SparkDataFrame]:
        """"Carica e unisce più file in un unico DataFrame. Implementa inoltre, la normalizzazione dei dati 
            rispetto allo schema Twitter e la conversione di tali dati in formato Parquet per usi futuri."""
        
        with st.spinner(f"Reading {len(file_paths)} files..."):
            logger.info(f"Avvio caricamento di {len(file_paths)} file")
            
            spark = self.spark_manager
            if not file_paths:
                logger.error("Nessun file fornito per il caricamento.")
                return None

            progress_bar = st.progress(0)

            master_schema = self.schema_manager.get_twitter_schema()
            if not master_schema:
                logger.error("Impossibile ottenere lo schema Twitter master. Caricamento interrotto.")
                return None
            logger.info("Utilizzo dello schema Twitter come 'master schema' per la normalizzazione.")
            
            home_directory = os.path.expanduser('~')
            temp_parquet_dir = os.path.join(home_directory, 'Desktop', 'Files_parquet')
            os.makedirs(temp_parquet_dir, exist_ok=True)
            
            dataframes_normalizzati = []

            #Salvataggio degli schemi inferiti
            # temp_parquet_dir = "temp_parquet_output"
            # if os.path.exists(temp_parquet_dir):
            #     shutil.rmtree(temp_parquet_dir)
            # os.makedirs(temp_parquet_dir)

            for idx, file_path in enumerate(file_paths):
                try:
                    logger.info(f"--- Processando file {idx+1}/{len(file_paths)}: {file_path} ---")
                    current_format = file_format or self._detect_format(file_path)


                    if current_format == 'parquet':
                        shutil.copy(file_path, os.path.join(temp_parquet_dir, os.path.basename(file_path)))
                        logger.info(f"File Parquet '{os.path.basename(file_path)}' copiato direttamente.")

                    elif current_format == 'json':
                        df = self.schema_manager.load_with_schema_and_fallback(
                            spark, file_path, current_format, 'auto'  # Lasciamo che spark usi l'inferenza automatica 
                        )
                            
                        if df is None:
                            logger.warning(f"Salto del file {file_path} perché il caricamento ha restituito None.")
                            continue

                        df_normalizzato = self.schema_manager.normalize_dataframe_to_schema(df, master_schema)
                        df_normalizzato = df_normalizzato.withColumn("source_file", F.lit(os.path.basename(file_path))) \
                                                        .withColumn("source_id", F.lit(idx + 1))
                        df_normalizzato.write.mode("append").parquet(temp_parquet_dir)
                        dataframes_normalizzati.append(df_normalizzato)
                        logger.info(f"File {file_path} salvato correttamente in formato Parquet.")

                    progress = int((idx+1)/len(file_paths) * 100)
                    progress_bar.progress(progress, text=f"Readed {idx+1} of {len(file_paths)} files")

                except Exception as e:
                    logger.error(f"Errore critico durante il processo del file {file_path}: {e}", exc_info=True)
                    continue
                    
            if current_format == 'json' and not dataframes_normalizzati:
                logger.error("Nessun file è stato caricato e normalizzato con successo.")
                return None
            
            progress_bar.empty()
        with st.spinner(f"Merging {len(file_paths)} files..."):
            logger.info(f"Unione di {len(dataframes_normalizzati)} DataFrame normalizzati...")

            if current_format == 'json':
                combined_df = reduce(lambda df1, df2: df1.union(df2), dataframes_normalizzati)
            try:
                if current_format == 'json':
                    combined_df = spark.read.parquet(temp_parquet_dir)
                elif current_format == 'parquet':
                    combined_df = spark.read.option("mergeSchema", "true").parquet(temp_parquet_dir)

                total_records = combined_df.count()
                combined_df.show()

                logger.info(f"Conteggio finale riuscito. Totale record: {total_records}")

                combined_df.createOrReplaceTempView("temp_data")
                self._update_metadata(combined_df, f"{len(file_paths)} files combined")
                self.current_dataset = combined_df
                
                logger.info("DataFrame finale pronto per le query.")
                return combined_df


            except Exception as e:
                logger.error(f"Errore durante la finalizzazione del DataFrame (conteggio o persist): {e}", exc_info=True)
                if 'combined_df' in locals():
                    combined_df.unpersist()
                return None
    
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
        """Aggiorna i metadati del dataset"""
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
    
    
    @staticmethod
    def coerce_to_string(df, target_schema: StructType):
        """
        Converte in stringa i campi che potrebbero causare conflitti,
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
                
                if source_type == target_type:
                    select_exprs.append(F.col(col_name))
                elif isinstance(source_type, (StructType, ArrayType)):
                    if target_type == StringType():
                        logger.warning(f"Coercizione di '{col_name}' da {source_type} a Stringa JSON.")
                        select_exprs.append(F.to_json(F.col(col_name)).alias(col_name))
                    else:
                        select_exprs.append(F.col(col_name).cast(target_type).alias(col_name))
                else:
                    select_exprs.append(F.col(col_name).cast(target_type).alias(col_name))
            else:
                select_exprs.append(F.lit(None).cast(target_type).alias(col_name))
                
        return df.select(*select_exprs)
    
    @staticmethod
    def clean_twitter_dataframe(df: SparkDataFrame) -> SparkDataFrame | None:
        """
        Pulisce un DataFrame Spark di Twitter rimuovendo le colonne non necessarie.
        """
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
            existing_top_level_cols = [col for col in columns_to_drop if col in df.columns]
            cleaned_df = df.drop(*existing_top_level_cols)
            
            logger.info("Pulizia delle colonne annidate 'retweeted_status' e 'quoted_status'...")
            
            cleaned_df = cleaned_df.withColumn(
                "retweeted_status",
                F.when( #usato per gestire i valori nulli 
                    F.col("retweeted_status").isNotNull(),
                    F.col("retweeted_status").dropFields(*columns_to_drop)
                ).otherwise(F.lit(None))
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
            logger.error(f"Errore di analisi Spark durante la pulizia del DataFrame: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Errore imprevisto durante la pulizia del DataFrame: {e}", exc_info=True)
            return None


class FileHandler:
    """Classe di utility per gestire file caricati."""
    
    @staticmethod
    def handle_uploaded_file(uploaded_file) -> Optional[str]:
        """
        Gestisce un file compresso uploadato e lo salva temporaneamente
        """
        try:
            temp_dir = Config.TEMP_DIR
            os.makedirs(temp_dir, exist_ok=True)
            temp_path = os.path.join(temp_dir, uploaded_file.name)
            
            if uploaded_file.name.endswith('.gz'):
                with gzip.open(uploaded_file, 'rt') as gz_file:
                    content = gz_file.read()
                
                decompressed_path = temp_path[:-3]
                with open(decompressed_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                return decompressed_path
            else:
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