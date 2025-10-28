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