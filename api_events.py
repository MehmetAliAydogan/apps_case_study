import pandas as pd

# Path to your JSON file
json_installs = "C:/Users/mehme/Desktop/Apps_Interview/apps_case_study/api_response/response_installs.json"
json_events = "C:/Users/mehme/Desktop/Apps_Interview/apps_case_study/api_response/response_events.json"
json_cost = "C:/Users/mehme/Desktop/Apps_Interview/apps_case_study/api_response/response_cost.json"

# Read Installs response file
df = pd.read_json(json_installs)

# Display the columns
print("Installs structure:")
print(df.columns)
print("Length of Installs:", len(df))

# Display the first 5 rows
print("Installs data example:")
print(df.head())

# Read Events response file
df = pd.read_json(json_events)

# Display the columns
print("Events structure:")
print(df.columns)
print("Length of Events:", len(df))

# Display the first 5 rows
print("Events data example:")
print(df.head())

# Read Cost response file
df = pd.read_json(json_cost)

# Display the columns
print("Costs structure:")
print(df.columns)
print("Length of Costs:", len(df))

# Display the first 5 rows
print("Costs data example:")
print(df.head())