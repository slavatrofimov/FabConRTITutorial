# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3b150029-cf4b-4d63-b037-76a31150cc33",
# META       "default_lakehouse_name": "test1",
# META       "default_lakehouse_workspace_id": "ff045177-ec4d-4450-a7c8-66ade2c2b74e",
# META       "known_lakehouses": [
# META         {
# META           "id": "3b150029-cf4b-4d63-b037-76a31150cc33"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM `ST-RTITutorial01`.WebSalesData_LH.products LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.
df = spark.sql("SELECT * FROM `Grid Intelligence 05`.ReferenceDataLH.feeders LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.
df = spark.sql("UPDATE `Grid Intelligence 05`.ReferenceDataLH.feeders SET scada_monitored = scada_monitored")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.
df = spark.sql("CREATE TABLE `ST-RTITutorial01`.WebSalesData_LH.feeders AS SELECT * FROM `Grid Intelligence 05`.ReferenceDataLH.feeders LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.
df = spark.sql("CREATE TABLE `Grid Intelligence 05`.SchemaTest.dbo.feeders3 AS SELECT * FROM `ST-RTITutorial01`.WebSalesData_LH.feeders LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import pyodbc
import pandas as pd
import json

connection_id = '9bc22582-a2ca-462e-ac0b-df4be84dbf0f' # connection name: "AKVReferenceTest"
connectionCredentials = notebookutils.connections.getCredential(connection_id)
credential_dict = json.loads(connectionCredentials['credential'])
user_name = next(item['value'] for item in credential_dict['credentialData'] if item['name'] == 'username')
pwd = next(item['value'] for item in credential_dict['credentialData'] if item['name'] == 'password')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{user_name}{pwd}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

server_name = 'st-sqldemos.database.windows.net'
database_name = 'sqldb_AdventureWorks'

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={server_name};"
    f"DATABASE={database_name};"
    f"UID={user_name};"
    f"PWD={pwd}"
)
cursor = conn.cursor()

query = f"SELECT 1 AS MyNumber"
df = pd.read_sql_query(query, conn)

display(df)

cursor.close()
conn.close()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
