from hubspot import HubSpot
from hubspot.crm.deals import ApiException
import pandas as pd


# Private App or OAuth Token
ACCESS_TOKEN = "pat-na2-xxxxxx"  

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
    limit = 100 # can be up to 200

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

# Process the data (e.g., convert to a pandas DataFrame or CSV)
if deals_data:
    deals_list = list(deals_data) 

    # Create a Pandas DataFrame for easy analysis/export
    df = pd.DataFrame(deals_list)
    
    print(f"Successfully extracted {len(deals_data)} deals.")
    print(df.head())

    # Save to a CSV file
    df.to_csv("hubspot_deals.csv", index=False)
    print("Data saved to hubspot_deals.csv")

else:
    print("No deals data extracted.")

