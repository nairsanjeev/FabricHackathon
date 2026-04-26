# =============================================================
# Notebook 01: Bronze Data Ingestion
# 
# Purpose: Read raw CSV files from Lakehouse Files and save 
#          as Delta tables in the Bronze layer
#
# Instructions:
#   1. Create a notebook in Fabric named "01 - Bronze Data Ingestion"
#   2. Attach your HealthcareLakehouse
#   3. Paste this entire file into Cell 1 and run it
# =============================================================

# Define the list of CSV files to ingest
csv_files = [
    "patients",
    "encounters",
    "conditions",
    "medications",
    "vitals",
    "clinical_notes",
    "claims"
]

# Base path for raw files in the Lakehouse
raw_path = "Files/raw"

# Ingest each CSV file as a Bronze Delta table
for file_name in csv_files:
    print(f"Ingesting {file_name}...")
    
    # Read CSV with header and infer schema
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .load(f"{raw_path}/{file_name}.csv")
    
    # Write as Delta table in the Tables section
    table_name = f"bronze_{file_name}"
    df.write.mode("overwrite").format("delta").saveAsTable(table_name)
    
    # Print summary
    count = df.count()
    print(f"  ✓ {table_name}: {count} rows, {len(df.columns)} columns")

print("\n✅ Bronze layer ingestion complete!")
