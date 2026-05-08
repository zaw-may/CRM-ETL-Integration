from hubspot import HubSpot
from hubspot.crm.contacts import ApiException
import pandas as pd
import os
from dotenv import load_dotenv
import logging
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

if not ENV_PATH.exists():
    raise FileNotFoundError(f".env file not found at {ENV_PATH}")

load_dotenv(ENV_PATH)

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("ACCESS_TOKEN not found in .env")

try:
    client = HubSpot(access_token=ACCESS_TOKEN)
except Exception as e:
    raise RuntimeError(f"Error initializing HubSpot client: {e}")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_all_crm_objects(
    object_name,
    properties=None,
    limit=100,
    archived=False
):
    """
    CRM Objects = ["contacts", "companies", "deals", "tickets", "line_items", "quotes", "custom objects"]
    """

    logging.info(f"Getting all CRM objects from HubSpot")

    all_results = []
    after = None

    while True:
        try:

            api = getattr(client.crm, object_name)

            response = api.basic_api.get_page(
                limit=limit,
                after=after,
                archived=archived,
                properties=properties
            )

            all_results.extend(response.results)

            if response.paging and response.paging.next:
                after = response.paging.next.after
            else:
                break

        except ApiException as e:
            print(f"HubSpot API Error: {e}")
            break

        except Exception as e:
            print(f"Unexpected Error: {e}")
            break

    return all_results

def get_all_owners(limit=100):

    logging.info(f"Object [Owners]: Getting all owners info")

    all_owners = []
    after = None

    while True:

        try:

            response = client.crm.owners.owners_api.get_page(
                limit=limit,
                after=after,
                archived=False
            )

            all_owners.extend(response.results)

            if response.paging and response.paging.next:
                after = response.paging.next.after
            else:
                break

        except Exception as e:
            print(f"Owner API Error: {e}")
            break

    return all_owners


def save_to_csv(data, filename):

    logging.info(f"Saving to CSV files")
        
    if not data:
        logging.info(f"No data found")
        return

    dict_data = [x.to_dict() for x in data]

    df = pd.json_normalize(dict_data)

    df.to_csv(filename, index=False)

    logging.info(f"Saved {len(df)} rows to {filename}")

contacts = get_all_crm_objects(
    object_name="contacts",
    properties=[
        "firstname",
        "lastname",
        "email",
        "phone",
        "company"
    ]
)

save_to_csv(
    contacts,
    r"C:\Users\User\Downloads\Z\python\python-generated-result-files\temp_contacts.csv"
)

companies = get_all_crm_objects(
    object_name="companies",
    properties=[
        "name",
        "domain",
        "phone",
        "city",
        "country"
    ]
)

save_to_csv(
    companies,
    r"C:\Users\User\Downloads\Z\python\python-generated-result-files\temp_companies.csv"
)


deals = get_all_crm_objects(
    object_name="deals",
    properties=[
        "dealname",
        "amount",
        "dealstage",
        "pipeline",
        "closedate",
        "hubspot_owner_id"
    ]
)

save_to_csv(
    deals,
    r"C:\Users\User\Downloads\Z\python\python-generated-result-files\temp_deals.csv"
)


tickets = get_all_crm_objects(
    object_name="tickets",
    properties=[
        "subject",
        "content",
        "hs_pipeline",
        "hs_pipeline_stage"
    ]
)

save_to_csv(
    tickets,
    r"C:\Users\User\Downloads\Z\python\python-generated-result-files\temp_tickets.csv"
)

owners = get_all_owners()

save_to_csv(
    owners,
    r"C:\Users\User\Downloads\Z\python\python-generated-result-files\temp_owners.csv"
)