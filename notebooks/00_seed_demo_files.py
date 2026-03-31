# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Demo Files
# MAGIC
# MAGIC Copies the sample batch files from the repo's `data/sample/` directory
# MAGIC into the configured Unity Catalog volume landing path so the Lakeflow
# MAGIC pipeline can ingest them via Auto Loader.
# MAGIC
# MAGIC **Run this notebook once before your first pipeline refresh.**

# COMMAND ----------

import os

# Pipeline configuration — these should match your databricks.yml variables
LANDING_PATH = spark.conf.get("demo.landing_path", "/Volumes/main/bronze_handoff_demo/landing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the landing volume if it does not exist

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS main.bronze_handoff_demo.landing")
spark.sql("CREATE VOLUME IF NOT EXISTS main.bronze_handoff_demo.ops")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy sample batches into the landing path

# COMMAND ----------

import shutil
from pathlib import Path

# Locate the sample data relative to the repo root
# When deployed via bundle, the repo root is the current working directory
REPO_SAMPLE_DIR = Path("data/sample")

if not REPO_SAMPLE_DIR.exists():
    # Fallback: try the workspace files path
    nb_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = nb_path.rsplit("/", 2)[0]
    REPO_SAMPLE_DIR = Path(repo_root) / "data/sample"

batch_dirs = sorted(REPO_SAMPLE_DIR.glob("batch_*"))
print(f"Found {len(batch_dirs)} batch directories to seed.\n")

for batch_dir in batch_dirs:
    for json_file in batch_dir.glob("*.json"):
        dest = f"{LANDING_PATH}/{batch_dir.name}/{json_file.name}"
        dbutils.fs.cp(f"file:{json_file.resolve()}", dest)
        print(f"  Copied: {batch_dir.name}/{json_file.name} → {dest}")

print(f"\nSeeding complete. {len(batch_dirs)} batches landed at {LANDING_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify landed files

# COMMAND ----------

display(dbutils.fs.ls(LANDING_PATH))
