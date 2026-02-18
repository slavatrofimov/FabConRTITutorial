# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e4957ca1-b064-4d0e-906d-677c7c396cba",
# META       "default_lakehouse_name": "WebSalesData_LH",
# META       "default_lakehouse_workspace_id": "ff045177-ec4d-4450-a7c8-66ade2c2b74e",
# META       "known_lakehouses": [
# META         {
# META           "id": "e4957ca1-b064-4d0e-906d-677c7c396cba"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

Files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/products.csv")
# df now is a Spark DataFrame containing CSV data from "Files/products.csv".
display(df)


abfss://ff045177-ec4d-4450-a7c8-66ade2c2b74e@onelake.dfs.fabric.microsoft.com/e4957ca1-b064-4d0e-906d-677c7c396cba/Files/products.csv


/lakehouse/default/Files/products.csv




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.data.spark.read

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("/lakehouse/WebSalesData_LH/Files/products.csv")
df.display()




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
