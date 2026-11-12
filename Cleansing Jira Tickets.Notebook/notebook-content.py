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

df = spark.read.table("jira_tickets")
# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F 

parent_df = df.filter((F.col("Subtasks").isNotNull()) & (F.col("Subtasks") != ""))
parent_df.count() 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

split_subtask_df = parent_df.withColumn("Subtask_Array", F.split(F.col("Subtasks"), ","))
split_subtask_df = split_subtask_df.withColumn("Subtask_Array", F.expr("transform(Subtask_Array, x -> trim(x))"))

flatten_subtask_df = split_subtask_df.withColumn("Subtask", F.explode(F.col("Subtask_Array"))).drop("Subtask_Array")

unique_subtask = flatten_subtask_df.select("Subtask").dropDuplicates()

child_df = df.join(unique_subtask, df.Ticket_ID == unique_subtask.Subtask, how="inner") \
             .select(df["*"]) 
             
print(unique_subtask.count())
print(child_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

whole_df = child_df.union(parent_df)
whole_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mismatched_subtask_df = unique_subtask.join(df, unique_subtask.Subtask == df.Ticket_ID, how="left_anti")
display(mismatched_subtask_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mismatched_subtask_df = df.join(whole_df, whole_df.Ticket_ID == df.Ticket_ID, how="left_anti")
mismatched_subtask_df = mismatched_subtask_df.filter(~F.col("Summary").contains("Approval"))
display(mismatched_subtask_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
