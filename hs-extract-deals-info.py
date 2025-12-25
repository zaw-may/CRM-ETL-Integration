from os import times
from hubspot import HubSpot
from hubspot.crm.deals import ApiException
import pandas as pd
import json
from datetime import datetime

# Private App or OAuth Token (From App Distribution)
ACCESS_TOKEN = "pat-na2-xxxxxxx"  

# Initialize the HubSpot client 
try:
    client = HubSpot(access_token=ACCESS_TOKEN)
except Exception as e:
    print(f"Error initializing HubSpot client: {e}")
    exit()

def get_all_deals():
    """
    Fetches all deals from HubSpot using the API client, handling pagination.
    """
    all_deals = []
    after = None
    limit = 100 

    while True:
        try:
            # Get a page of deals, requesting specific properties if needed
            api_response = client.crm.deals.basic_api.get_page(
                limit=limit,
                after=after,
                archived=False,
                properties=["dealname", "amount", "dealstage", "closedate", "pipeline", "description", "dealtype", "createdate", "days_to_close"]
            )
            
            all_deals.extend(api_response.results)
            
            # Check for the next page
            if api_response.paging and api_response.paging.next:
                after = api_response.paging.next.after
            else:
                break # No more pages
                
        except ApiException as e:
            print(f"Exception when calling basic_api->get_page: {e}\n")
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break
            
    return all_deals

# Fetch the data
deals_data = get_all_deals()

deals_details = []

# Collect the data 
if deals_data:
    deals_list = list(deals_data) 
    
    for dd in deals_list:
        deals_details.append({
            "archived": dd.archived,
            "archived_at": dd.archived_at,
            "associations": dd.associations,
            "created_at": dd.created_at,
            "id":dd.id,
            "object_write_trace_id": dd.object_write_trace_id,
            "properties": dd.properties,
            "properties_with_history": dd.properties_with_history,
            "updated_at": dd.updated_at            
            }) 

        data_to_save = {
            "archived": dd.archived,
            "archived_at": dd.archived_at,
            "associations": dd.associations,
            # "created_at": datetime.date(dd.created_at),
            "id": dd.id,
            "object_write_trace_id": dd.object_write_trace_id,
            "properties": dd.properties,
            "properties_with_history": dd.properties_with_history,
            # "updated_at": datetime.date(dd.updated_at)
            }    

    # Create a Pandas DataFrame for easy analysis
    df = pd.DataFrame(deals_details)
    filename = "output"
    
    print(f"Successfully extracted {len(deals_data)} deals.")
    print(df.head())

    # Save to the files
    df.to_csv("df" + filename + ".csv", index=False)

    try:
        with open("json" + filename + ".json", 'w') as json_file:
            json.dump(data_to_save, json_file, indent=4)
        print(f"Data successfully saved to JSON file.")
    except IOError as e:
        print(f"Error saving file: {e}")

    print("Data is successfully saved on your local machine.")

else:
    print("No deals data extracted.")