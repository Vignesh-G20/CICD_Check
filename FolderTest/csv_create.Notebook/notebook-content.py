# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a4d2f381-f601-448f-b586-56bde2a6db83",
# META       "default_lakehouse_name": "Pinelabs_LH",
# META       "default_lakehouse_workspace_id": "883ac136-9d9d-429c-a3d4-50dd3e3df5f6",
# META       "known_lakehouses": [
# META         {
# META           "id": "a4d2f381-f601-448f-b586-56bde2a6db83"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import csv
from pyspark.sql.functions import *
from pyspark.sql.window import *

EXPORT_FOLDER = "Files/auto_csv_exports"
TMP_DIR = "/tmp"
MAX_ROWS_PER_CSV = 800000   


DATE_FORMAT = "dd-MM-yyyy HH:mm:ss"

# tables = spark.sql("SHOW TABLE EXTENDED LIKE '*'").collect()
db_name = "CGW_PA_MF_WS.CGW_PA_MF_LH.Silver"
 
tables = spark.sql(
    f"SHOW TABLE EXTENDED IN {db_name} LIKE '*'"
).collect()


def store_as_csv(dataframe, table_name, year_val, part, num_parts):
    pdf = dataframe.toPandas()

    part_suffix = f"_part_{part+1}" if num_parts > 1 else ""
    file_name = f"silver_{table_name}_{year_val}{part_suffix}.csv"

    tmp_csv = f"{TMP_DIR}/{file_name}"

    pdf.to_csv(
        tmp_csv,
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\"
    )

    lakehouse_path = f"{EXPORT_FOLDER}/{file_name}"

    if mssparkutils.fs.exists(lakehouse_path):
        mssparkutils.fs.rm(lakehouse_path)

    mssparkutils.fs.cp(f"file:{tmp_csv}", lakehouse_path)

    print(f"   ✅ Exported → {lakehouse_path}")


for row in tables:
    namespace = row.namespace
    table_name = row.tableName

    if "silver" not in namespace.lower():
        continue
    if "prod" not in table_name.lower():
        continue
    if table_name.lower().endswith("_sc"):
        continue

    full_table_name = f"{namespace}.{table_name}"
    print(f"\n🚀 Processing table: {full_table_name}")

    df = spark.read.table(full_table_name)

    if df.rdd.isEmpty():
        continue

    df_record_count = df.count()

    # -------- LARGE TABLES --------
    if df_record_count > MAX_ROWS_PER_CSV:

        df = df.withColumn(
            "created_ts",
            to_timestamp(col("CreatedDate"), DATE_FORMAT)
        ).withColumn(
            "year",
            year(col("created_ts"))
        )

        years = [r.year for r in df.select("year").distinct().collect() if r.year is not None]

        for yr in years:
            print(f"📅 Processing year: {yr}")

            year_df = df.filter(col("year") == yr)

            total_rows = year_df.count()
            if total_rows == 0:
                continue

            window = Window.orderBy(col("created_ts"))
            year_df = year_df.withColumn("rn", row_number().over(window))

            num_parts = (total_rows // MAX_ROWS_PER_CSV) + (1 if total_rows % MAX_ROWS_PER_CSV else 0)

            for part in range(num_parts):
                start = part * MAX_ROWS_PER_CSV + 1
                end = start + MAX_ROWS_PER_CSV

                chunk_df = year_df.filter(
                    (col("rn") >= start) & (col("rn") <= end)
                ).drop("rn", "year", "created_ts")

                if chunk_df.rdd.isEmpty():
                    continue

                store_as_csv(chunk_df, table_name, yr, part, num_parts)

    # -------- SMALL TABLES --------
    else:
        store_as_csv(df, table_name, "all", 0, 1)

print("\n🎉 Automation completed successfully (year + row split)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import csv
from pyspark.sql.functions import *
from pyspark.sql.window import *

EXPORT_FOLDER = "Files/auto_csv_exports"
TMP_DIR = "/tmp"
MAX_ROWS_PER_CSV = 800000   


DATE_FORMAT = "dd-MM-yyyy HH:mm:ss"

# tables = spark.sql("SHOW TABLE EXTENDED LIKE '*'").collect()
db_name = "CGW_PA_MF_WS.CGW_PA_MF_LH.Silver"
 
tables = spark.sql(
    f"SHOW TABLE EXTENDED IN {db_name} LIKE '*'"
).collect()

def store_as_csv(dataframe):
    pdf = dataframe.toPandas()

    part_suffix = f"_part_{part+1}" if num_parts > 1 else ""
    file_name = f"silver_{table_name}_{yr}{part_suffix}.csv"

    tmp_csv = f"{TMP_DIR}/{file_name}"

    pdf.to_csv(
        tmp_csv,
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\"
    )

    lakehouse_path = f"{EXPORT_FOLDER}/{file_name}"

    if mssparkutils.fs.exists(lakehouse_path):
        mssparkutils.fs.rm(lakehouse_path)

    mssparkutils.fs.cp(f"file:{tmp_csv}", lakehouse_path)

    print(f"   ✅ Exported → {lakehouse_path}")


for row in tables:
    namespace = row.namespace
    table_name = row.tableName
    info = row.information or ""

    # ---------- Filters ----------

    if "silver" not in namespace.lower():
        continue

    if "prod" not in table_name.lower():
        continue

    if table_name.lower().endswith("_sc"):
        continue

    full_table_name = f"{namespace}.{table_name}"
    print(f"\n🚀 Processing table: {full_table_name}")

    # ---------- Read table ----------
    df = spark.read.table(full_table_name)
    df_record_count = df.count()

    if df.rdd.isEmpty():
        continue

    if df_record_count > MAX_ROWS_PER_CSV:

        df = df.withColumn(
            "created_ts",
            to_timestamp(col("CreatedDate"), DATE_FORMAT)
        )

        df = df.withColumn("year", year(col("created_ts")))

        years = [r.year for r in df.select("year").distinct().collect() if r.year is not None]

        for yr in years:
            print(f"📅 Processing year: {yr}")

            year_df = df.filter(col("year") == yr)

            total_rows = year_df.count()
            print(f"   Rows in {yr}: {total_rows}")

            if total_rows == 0:
                continue

            # ---------- Add row numbers for chunking ----------
            window = Window.orderBy(col("created_ts"))
            year_df = year_df.withColumn("rn", row_number().over(window))

            num_parts = (total_rows // MAX_ROWS_PER_CSV) + (1 if total_rows % MAX_ROWS_PER_CSV else 0)

            for part in range(num_parts):
                start = part * MAX_ROWS_PER_CSV + 1
                end = start + MAX_ROWS_PER_CSV - 1

                chunk_df = year_df.filter((col("rn") >= start) & (col("rn") <= end)) \
                                .drop("rn", "year", "created_ts")

                # ---------- Convert ONLY CHUNK to Pandas ----------
                if chunk_df.rdd.isEmpty():
                    continue

                store_as_csv(chunk_df)
    else:
        store_as_csv(df)

print("\n🎉 Automation completed successfully (year + row split)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import csv
 
EXPORT_FOLDER = "Files/auto_csv_exports"
TMP_DIR = "/tmp"
 
db_name = "CGW_PA_MF_WS.CGW_PA_MF_LH.Silver"
 
tables = spark.sql(
    f"SHOW TABLE EXTENDED IN {db_name} LIKE '*'"
).collect()

for row in tables:
    namespace = row.namespace        
    table_name = row.tableName
    info = row.information or ""
 
    # if "Type: MANAGED" not in info:
    #     continue

    if table_name.lower().endswith("_sc"):
        continue
 
    if "silver" not in namespace.lower():
        continue
 
    if "prod" not in table_name.lower():
        print(f"This is not a prod table so skipped {table_name}")
        continue
 
    full_table_name = f"{namespace}.{table_name}"
    print(f"🚀 Processing table: {full_table_name}")
 
 
    spark_df = spark.read.table(full_table_name)
 
    pdf = spark_df.toPandas()
 
    if pdf.empty:
        print(f"⚠️ Empty table, skipped: {full_table_name}")
        continue
 
    tmp_csv = f"{TMP_DIR}/{table_name}.csv"
 
    pdf.to_csv(
        tmp_csv,
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\"
    )
 
    lakehouse_csv_path = f"{EXPORT_FOLDER}/{table_name}.csv"
 
    if mssparkutils.fs.exists(lakehouse_csv_path):
        mssparkutils.fs.rm(lakehouse_csv_path)
 
    mssparkutils.fs.cp(
        f"file:{tmp_csv}",
        lakehouse_csv_path
    )
 
    print(f"✅ Exported → {lakehouse_csv_path}")
 
print("🎉 Automation completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
