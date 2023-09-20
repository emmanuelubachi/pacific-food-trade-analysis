import pandas as pd
import sys

sys.path.append("..")
from functions import DataCleaner as DC

# Set the maximum number of rows to display (e.g., 100)
pd.set_option("display.max_rows", 20)

# Define the path to the CSV file
file_path = "../data/staging/food_trade.csv"

try:
    # Read the CSV file into a Pandas DataFrame
    df_raw = pd.read_csv(file_path)

    # Display the first few rows of the DataFrame to verify data ingestion
    # print(df_raw.head())
    df_copy = df_raw.copy()
    df = pd.DataFrame(df_copy)
    print(df.head())

    # You can now perform data cleaning, transformation, and exploration on the DataFrame.
    # For example, you can perform operations like df.describe(), df.isnull().sum(), etc.


except FileNotFoundError:
    print(f"File not found: {file_path}")
except Exception as e:
    print(f"An error occurred: {str(e)}")

df.shape

for i in df.columns:
    print("- ", i)
    print("-----------------------------------------------")
    print([df[i].unique()])
    print(" ")
    print(" ")

df1 = df.filter(["Exporter", "Commodity", "Time", "OBS_VALUE"])
df1

pacific_countries = [
    "Vanuatu",
    "Federated State of Micronesia",
    "Niue",
    "New Zealand",
    "French Polynesia",
    "Kiribati",
    "Fiji",
    "Marshall Islands",
    "Nauru",
    "Tonga",
    "Palau",
    "Papua New Guinea",
    "Cook Islands",
    "New Caledonia",
    "Wallis and Futuna Islands",
    "Tokelau",
    "Australia",
    "American Samoa",
    "Solomon Islands",
    "Tuvalu",
    "Samoa",
    "Guam",
    "Pitcairn",
    "Northern Mariana Islands",
]

df2 = df1[df["Exporter"].isin(pacific_countries)]
df3 = df2[df["Time"] == 2018].reset_index(drop=True)

df3.head(200)

# Group by both "Exporter" and "Commodity" columns and calculate the sum of "OBS_VALUE" for each combination
grouped_df = (
    df3.groupby(["Exporter", "Commodity", "Time"])["OBS_VALUE"].sum().reset_index()
)

grouped_df.head(200)

grouped_df1 = grouped_df.rename(columns={"Commodity": "name", "OBS_VALUE": "value"})

# Group by "Exporter" and "Time" and aggregate the data as a list of dictionaries
grouped_df2 = (
    grouped_df1.groupby(["Exporter", "Time"])
    .apply(lambda x: x[["name", "value"]].to_dict("records"))
    .reset_index(name="Data")
)

# Convert the grouped DataFrame to the desired JSON format
formatted_json = grouped_df2.to_json(
    "../data/processed/food_trade.json", orient="records", default_handler=str
)

formatted_json
