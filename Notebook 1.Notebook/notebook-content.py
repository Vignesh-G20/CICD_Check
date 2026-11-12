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


tbl_df = spark.read.table("jira_tickets")
display(tbl_df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_path ="Files/ORS/Bulk/2023/2023.01.01.txt"

df = (
    spark.read
    .option("delimiter", "|")
    .option("infraschema", "true")
    .option("header", "false")      # set false if no header
    .csv(file_path)
)

df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("temp_ors_data")
# display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
