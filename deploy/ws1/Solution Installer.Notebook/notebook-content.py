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
    repo_owner="microsoft",
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

# MARKDOWN ********************

# ## Manual Post-Deployment Steps
# Your solution deploymnet is almost complete!
# 
# ⚠️ Please be sure to refresh your browser window to reflect all newly-deployed items!
# 
# Then, please complete the following post-deployment steps that must be performed manually.

# MARKDOWN ********************

# ### 1. Enable OneLake Availability
# 
# This feature is also called **"one logical copy"** and it automatically allows KQL Database tables to be accessed from a Lakehouse, Notebooks, etc in delta-parquet format via OneLake.
# 
# When activated it will constantly copy the KQL data to your Fabric OneLake in delta format. It allows you to query KQL Database tables as delta tables using Spark or SQL endpoint on the Lakehouse. We recommend enabling this feature "before" we load the more data into our KQL Database. Also, consider this feature can be enabled/disabled per table if necessary. You can read more about this feature here: [Announcing Delta Lake support in Real-Time Intelligence KQL Database](https://support.fabric.microsoft.com/blog/announcing-delta-support-in-real-time-analytics-kql-db?ft=All).
# 
# ![alt text](https://raw.githubusercontent.com/microsoft/FabConRTITutorial/refs/heads/main/docs/assets/fabrta70.png)
# 
# #### Here's how to set this up
# 
# 1. When an Eventhouse is created, a KQL Database with the same name is created as well. To open the KQL Database click on the Database **WebEvents_EH** in the section **KQL Databases**.
# 
#    ![alt text](https://raw.githubusercontent.com/microsoft/FabConRTITutorial/refs/heads/main/docs/assets/image_task04_step01.png)
# 
# 2. After selecting the KQL Database click on the switch **availibility** to activate the OneLake availibility as shown in the screenshot.
# 
#    ![alt text](https://raw.githubusercontent.com/microsoft/FabConRTITutorial/refs/heads/main/docs/assets/image_task04_step02.png)
# 
#    <div class="info" data-title="Note">
# 
#    > **Newly created tables will automatically inherit the "OneLake availability" setting from the Database level**
# 
#    </div>
# 
# 3. Now the dialog **Turn on OneLake availibility** is shown. Ensure that **Apply to existing tables** is checked and click on the button **Turn on**.
# 
#    ![alt text](https://raw.githubusercontent.com/microsoft/FabConRTITutorial/refs/heads/main/docs/assets/image_task04_step03.png)


# MARKDOWN ********************

# ### 2. Accessing Eventhouse data from the lakehouse
# 
# In this task we will make the Eventhouse tables form the KQL Database available in our Lakehouse. This will be accomplished by creating _shortcuts_.
# 
# 1. Click on the button **Get data** in the menu bar at the top. Choose **New shortcut** from the dropdown menu.
# 
#    ![alt text](https://raw.githubusercontent.com/microsoft/FabConRTITutorial/refs/heads/main/docs/assets/image_task11_step01.png)
# 
#    <div class="important" data-title="Note">
# 
#    > If your Lakehouse is using Schemas you will see the schema **dbo** under the folder **Tables**. right-click the schema **dbo** and select the option **New table shortcut** from the context menu.
# 
#    </div>
# 
# 2. Select Microsoft OneLake.
# 
#    ![alt text](https://raw.githubusercontent.com/microsoft/FabConRTITutorial/refs/heads/main/docs/assets/image_task11_step02.png)
# 
# 3. Select the KQL Database **WebEvents_EH** in the Window **Select a data source type** and click on the button **Next**.
# 
#    ![alt text](https://raw.githubusercontent.com/microsoft/FabConRTITutorial/refs/heads/main/docs/assets/image_task11_step03.png)
# 
# 4. Expand the folder **Tables** under **WebEvents_EH** in the window **New shortcut** and check both tables **BronzeClicks** and **BronzeImpressions**. Click on **Next**.
# 
#    ![alt text](https://raw.githubusercontent.com/microsoft/FabConRTITutorial/refs/heads/main/docs/assets/image_task11_step04.png)
# 
#    <div class="info" data-title="Note">
# 
#    > You may return to this step to create additional shortcuts, after running the [createAll.kql](https://github.com/microsoft/FabConRTITutorial/blob/main/kql/createAll.kql) database script which will create additional tables. For now, you may proceed by selecting just the **BronzeClicks** and **BronzeImpressions** tables.
# 
#    </div>
# 
# 5. Click on the button **Create**.
# 
#    ![alt text](https://raw.githubusercontent.com/microsoft/FabConRTITutorial/refs/heads/main/docs/assets/image_task11_step05.png)
# 
#    Now you can see the shortcuts to the tables **BronzeClicks** and **BronzeImpressions** under the folder **Tables** in the lakehouse **WebSalesData_LH**.
# 
#    ![alt text](https://raw.githubusercontent.com/microsoft/FabConRTITutorial/refs/heads/main/docs/assets/image_task11_step05b.png)
# 
#    <div class="info" data-title="Note">
# 
#    > Note that the shortcuts have another icon than the regular delta tables.
# 
#    </div>

