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

def get_all_contacts():
    """
    Fetches all contacts from HubSpot using the API client, handling pagination.
    """
    all_contacts = []
    after = None
    limit = 100 # can be up to 200

    while True:
        try:
            # Get a page of contacts, requesting specific properties if needed
            api_response = client.crm.contacts.basic_api.get_page(
                limit=limit,
                after=after,
                archived=False,
                properties=["firstname", "lastname", "email", "phone", "company", "hs_marketable_status"]
            )
            
            all_contacts.extend(api_response.results)
            
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
            
    return all_contacts

# Fetch the data
contacts_data = get_all_contacts()

# Process the data (e.g., convert to a pandas DataFrame or CSV)
if contacts_data:
    contacts_list = list(contacts_data) 

    # Create a Pandas DataFrame for easy analysis/export
    df = pd.DataFrame(contacts_list)
    
    print(f"Successfully extracted {len(contacts_data)} contacts.")
    print(df.head())

    # Save to a CSV file
    df.to_csv("hubspot_contacts.csv", index=False)
    print("Data saved to hubspot_contacts.csv")

else:
    print("No contacts data extracted.")

