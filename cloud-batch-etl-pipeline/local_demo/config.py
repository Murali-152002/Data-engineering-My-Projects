"""
config.py (local_demo)
-----------------------
The sql/ folder in this project targets real Azure SQL Database (T-SQL,
MERGE, nonclustered indexes on a real SQL Server engine). This local_demo/
folder is a runnable stand-in used to validate the cleaning logic and the
index performance improvement without an Azure subscription: same star
schema, same watermark-driven incremental pattern, same index columns,
translated to SQLite so it runs anywhere with just Python.
"""

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LANDING_DIR = os.path.join(BASE_DIR, "sample_data", "landing")
DB_PATH = os.path.join(BASE_DIR, "sample_data", "warehouse.db")
