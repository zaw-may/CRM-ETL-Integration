from hubspot import HubSpot
import pandas as pd

ACCESS_TOKEN = "pat-na2-xxxxxx"

client = HubSpot(access_token=ACCESS_TOKEN)

def get_properties(object_type):
    props = client.crm.properties.core_api.get_all(object_type)
    rows = []

    for p in props.results:
        rows.append({
            "object_type": object_type,
            "referenced_object_type": p.referenced_object_type,
            "property_name": p.name,
            "label": p.label,
            "type": p.type,
            "display_order": p.display_order,
            "field_type": p.field_type,
            "group_name": p.group_name,
            "description": p.description,
            "hidden": p.hidden,
            "field_type": p.field_type,
            "form_field": p.form_field,
            "calculated": p.calculated,
            "calculation_formula": p.calculation_formula,
            "has_unique_value": p.has_unique_value,
            "created_at": p.created_at,
            "created_user_id": p.created_user_id,
            "updated_at": p.updated_at,
            "updated_user_id": p.updated_user_id,
            # "options": p.options,
            "external_options": p.external_options,
            "hubspot_defined": p.hubspot_defined,
            # "modification_metadata": p.modification_metadata,
            "archived": p.archived,
            "archived_at": p.archived_at,
            "show_currency_symbol": p.show_currency_symbol,
            "is_custom": p.created_at is not None
        })

    return pd.DataFrame(rows)

# Std CRM Objects
standard_objects = [
    "contacts",
    "deals"
    # "companies",
    # "tickets"
]

dfs = []

for obj in standard_objects:
    df = get_properties(obj)
    dfs.append(df)

# Custom Objects
custom_objects = client.crm.schemas.core_api.get_all()

for schema in custom_objects.results:
    object_type_id = schema.object_type_id 
    df = get_properties(object_type_id)
    df["custom_object_name"] = schema.name
    dfs.append(df)

####

all_properties_df = pd.concat(dfs, ignore_index=True)

print(all_properties_df.shape)
print(all_properties_df.head())


all_properties_df.to_csv("hubspot_all_properties1.csv", index=False)
print("hubspot_all_properties1.csv is stored successfully.")

# for each object
# for obj, g in all_properties_df.groupby("object_type"):
    # g.to_csv(f"hubspot_properties_{obj}.csv", index=False)
