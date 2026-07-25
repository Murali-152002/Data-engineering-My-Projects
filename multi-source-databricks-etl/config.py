"""
config.py
---------
Central configuration for the pipeline. In an actual Azure Databricks workspace,
these paths would point to mounted ADLS Gen2 / DBFS locations (e.g. abfss://...),
and secrets (SQL connection strings, etc.) would be pulled from a Databricks
Secret Scope backed by Azure Key Vault - never hardcoded.

For local/demo purposes everything resolves to a local "lakehouse" folder so the
pipeline can be run end-to-end without any cloud resources.
"""

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- "Landing zone" for raw daily CSV drops (stand-in for Azure Blob Storage) ---
LANDING_DIR = os.path.join(BASE_DIR, "sample_data", "landing")

# --- Operational source DB (stand-in for the on-prem/Azure SQL relational source) ---
OPERATIONAL_DB_PATH = os.path.join(BASE_DIR, "sample_data", "operational_source.db")

# --- Medallion layers (stand-in for ADLS Gen2 containers: bronze/, silver/, gold/) ---
LAKEHOUSE_DIR = os.path.join(BASE_DIR, "lakehouse")
BRONZE_DIR = os.path.join(LAKEHOUSE_DIR, "bronze")
SILVER_DIR = os.path.join(LAKEHOUSE_DIR, "silver")
GOLD_DIR = os.path.join(LAKEHOUSE_DIR, "gold")

# --- Pipeline run metadata / logging ---
LOG_DIR = os.path.join(BASE_DIR, "logs")

# --- Retry policy for transient failures (mirrors ADF/Databricks Jobs retry config) ---
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5

# --- Alert "recipients" - in production this maps to an Azure Monitor Action Group
#     (email/SMS/webhook). Locally we just log what WOULD have been sent. ---
ALERT_RECIPIENTS = ["data-eng-team@example.com"]
