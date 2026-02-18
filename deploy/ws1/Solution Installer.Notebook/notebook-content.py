# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": "e4957ca1-b064-4d0e-906d-677c7c396cba"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Real-Time Intelligence Tutorial Installer
# This notebook orchestrates the end-to-end deployment of **Real-Time Intelligence** solution assets into the current Microsoft Fabric workspace using the `fabric-launcher` library.
# 
# This notebook performs the following tasks:
# 1. **🚀 Deployment**: download source code, deploy Fabric items, load reference Data
# 1. **✅ Post-Deployment Tasks**: Complete post-deployment configuration tasks.

# MARKDOWN ********************

# ## 🚀 Deployment
# Download source code, deploy Fabric items, load reference Data

# CELL ********************

%pip install fabric-launcher --quiet
import notebookutils
notebookutils.session.restartPython()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from fabric_launcher import FabricLauncher

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from fabric_launcher import FabricLauncher

launcher = FabricLauncher(notebookutils,
    api_root_url = "https://api.fabric.microsoft.com" #Default is https://api.fabric.microsoft.com, but may vary depending on your environment
    )

# Deploy solution with data folders
launcher.download_and_deploy(
    repo_owner="slavatrofimov",
    repo_name="FabConRTITutorial",
    branch = "automate-deployment",
    workspace_folder="deploy/workspace",
    allow_non_empty_workspace=True,
    item_type_stages = [["KQLDatabase", "Lakehouse", "Eventhouse"],["Notebook", "KQLDashboard", "KQLQueryset", "Eventstream", "Reflex"]],
    data_folders={"ref_data": ""},
    lakehouse_name="WebSalesData_LH",
    validate_after_deployment=True,
    generate_report=True   
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## ✅ Post-Deployment Tasks

# CELL ********************

#Import required libraries
import sempy.fabric as fabric
from fabric_launcher import create_or_update_fabric_item, get_folder_id_by_name, move_item_to_folder, scan_logical_ids

# Initialize Fabric client and workspace
client = fabric.FabricRestClient()
workspace_id = fabric.get_workspace_id()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#Load products table
success = notebookutils.lakehouse.loadTable(
    {
        "relativePath": "Files/products.csv",
        "pathType": "File",
        "mode": "Overwrite",
        "recursive": False,
        "formatOptions": {
            "format": "Csv",
            "header": True,
            "delimiter": ","
        }
    }, "products", "WebSalesData_LH")

#Load productcategory table
success = notebookutils.lakehouse.loadTable(
    {
        "relativePath": "Files/productcategory.csv",
        "pathType": "File",
        "mode": "Overwrite",
        "recursive": False,
        "formatOptions": {
            "format": "Csv",
            "header": True,
            "delimiter": ","
        }
    }, "productcategory", "WebSalesData_LH")   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Create accelerated shortcuts in the KQL Database
from fabric_launcher.post_deployment_utils import (
    get_kusto_query_uri,
    create_accelerated_shortcut_in_kql_db
)

# Configuration Parameters
target_workspace_id = fabric.resolve_workspace_id()
target_eventhouse_name = 'WebEvents_EH'
target_kql_db_name = 'WebEvents_EH'
source_workspace_id = target_workspace_id 
source_lakehouse_name = 'WebSalesData_LH'

# Create shortcuts for required tables
tables = ['products', 'productcategory']

for table in tables:
    source_path = f"Tables/{table}" 
    target_shortcut_name = table
    
    print(f"Creating accelerated shortcut for table: {table}")
    
    try:
        create_accelerated_shortcut_in_kql_db(
            notebookutils=notebookutils,
            target_workspace_id=target_workspace_id,
            target_eventhouse_name=target_eventhouse_name,
            target_kql_db_name=target_kql_db_name,
            target_shortcut_name=target_shortcut_name,
            source_workspace_id=source_workspace_id,
            source_lakehouse_name=source_lakehouse_name,
            source_path=source_path,
            client=client
        )
        print(f"✅ Successfully created accelerated shortcut for '{table}'")
        
    except Exception as e:
        print(f"❌ Failed to create shortcut for '{table}': {str(e)}")
        # Continue with next table instead of stopping
        continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Execute a KQL Query to load data into the MeterContextualization table
from fabric_launcher.post_deployment_utils import (
    get_kusto_query_uri,
    exec_kql_command
)

kusto_query_uri = get_kusto_query_uri(target_workspace_id, target_eventhouse_name, client)
kql_command = f""".set-or-replace MeterContextualization <| MeterContextualizationFunction()"""
exec_kql_command(kusto_query_uri, target_kql_db_name, kql_command, notebookutils)
print('✅ Loaded data into the MeterContextualiazation table')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Next Steps
# Your automated solution deploymnet is complete!
# 
# ⚠️ Please be sure to refresh your browser window to reflect all newly-deployed items!
# 
# Review the README document for details on running simulations and exploring your solution.
