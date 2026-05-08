## Overview

This repository demonstrates a practical ETL pipeline that extracts CRM data from HubSpot using the official HubSpot API and HubSpot CLI, then loads the data into a SQL Server database.

The pipeline is designed to support reporting and analytics use cases and can be extended for automation, incremental loads, and integration with BI tools such as Power BI.

This project is intended for data professionals interested in building and maintaining CRM data pipelines.

<img width="520" height="146" alt="diagram" src="https://github.com/user-attachments/assets/4ab3eb37-c21d-4742-8a2c-02bb25f3630c" />

## SOP

* Create a Project in HubSpot > Development > Projects
* After creating the Project, find the project folder in your local machine
* Add required scopes from HubSpot in the projct folder > src > app > app-hsmeta.json
* Save app-hsmeta.json
* To run HubSpot CLI: Open Terminal
* hs account auth
* Choose "Enter existing personal access key"
* Go to HubSpot > Development > Keys > Personal Access Key > Copy it 
* Go back to Terminal > Paste Personal Access Key
* hs account use (If you do not create the Project, Run hs get-started, now the Project is already created)
* Select an account to use as the default (either test-developer account or production-go-live account)
* Run hs project upload
* Go to HubSpot again > Development > Projects > Select the Project that is just created 
* On the Left Pane, Select the Project 
* On the Right Pane, either Overview Tab or Auth Tab > Check the required scopes
* On the Right Pane, Distribution Tab > After checking required scopes, Install the app
* REMARK: Every time the new scopes are added to the app-hsmeta.json, Run 'hs project upload' and Uninstall and Reinstall the app
* REMARK: After doing (install/uninstall/reinstall) it, check the ACCESS TOKEN in the Distribution Tab 
* REMARK: Then Update .env file for ACCESS TOKEN

<img width="1408" height="666" alt="sop-workflow-diagram" src="https://github.com/user-attachments/assets/e7c7be54-e05d-47ba-b0d8-d0e3bb5127cc" />
