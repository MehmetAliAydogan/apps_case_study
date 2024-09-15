import pandas as pd

# Path to your JSON file
json_installs = "C:/Users/mehme/Desktop/Apps_Interview/apps_case_study/api_response/response_installs.json"
json_events = "C:/Users/mehme/Desktop/Apps_Interview/apps_case_study/api_response/response_events.json"
json_cost = "C:/Users/mehme/Desktop/Apps_Interview/apps_case_study/api_response/response_cost.json"

# Read Installs response file
install_df = pd.read_json(json_installs)

# Display the columns
print("\nInstalls structure:")
print(install_df.columns)
print("Length of Installs:", len(install_df))

# Display the first 5 rows
print("Installs data example:")
print(install_df.head())

# Read Events response file
events_df = pd.read_json(json_events)

# Display the columns
print("\nEvents structure:")
print(events_df.columns)
print("Length of Events:", len(events_df))

# Display the first 5 rows
print("Events data example:")
print(events_df.head())

print("Distinct events names:")
print(events_df.event_name.unique())

# Read Cost response file
cost_df = pd.read_json(json_cost)

# Display the columns
print("\nCosts structure:")
print(cost_df.columns)
print("Length of Costs:", len(cost_df))

# Display the first 5 rows
print("Costs data example:")
print(cost_df.head())

# Display distinct Network Names in Cost
print(cost_df.network_name.unique())

# Display distinct Network Names in Installs
print(install_df.network_name.unique())