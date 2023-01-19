# Databricks notebook source
# COMMAND ----------
# DBTITLE 1,Deduplicate list of potential updates against the GSM Source of Truth
df_new_entity_id = deduplicate_df(
    df_updates=base_df,
    target_model=gsm_identifier_mappings,
    spark_session=spark,
    on_columns=[md.vendor_identifier_type, md.vendor_identifier_value]
)
# COMMAND ----------
