import pandas as pd
import sys

sys.path.append("../..")
from functions import DataCleaner as DC

# Set the maximum number of rows to display (e.g., 100)
pd.set_option("display.max_rows", 200)

# Define the path to the CSV file
file_path = "../../data/staging/food_trade.csv"

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

df1 = df.filter(["Importer", "Commodity", "Exporter", "Time", "OBS_VALUE"])
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

df2 = df1[df["Importer"].isin(pacific_countries)]
df2.head()

df2["Importer"].unique()

df3 = df2.copy()

# ==========================================================================================================
# 

sorted(df["Importer"].unique())

df4 = df3[df3["Time"].isin([2018, 2017, 2016, 2015])].reset_index(drop=True)
df3[max(df3["Time"])]

df4.head(200)
df4.shape

# Group by both "Importer" and "Commodity" columns and calculate the sum of "OBS_VALUE" for each combination
grouped_df = (
    df4.groupby(["Importer", "Time", "Commodity"])["OBS_VALUE"].sum().reset_index()
)

grouped_df.shape

grouped_df.head(200)

grouped_df1 = grouped_df.rename(
    columns={"Commodity": "name", "OBS_VALUE": "value", "Time": "Year"}
)

df_ft = grouped_df1.copy()

df_ft.shape

# =====================================================================================

# Merge Food Trade data to Pacific County ISO3 Data

# =====================================================================================

df_pc = pd.read_csv("../data/processed/pacific_country.csv")

df_pc

df_merged = pd.merge(df_ft, df_pc, how="left", on="Importer")

df_merged

df_merged.dtypes

df_merged.isnull().sum()

df_merged["Importer"].nunique()

# Convert 'Column1' to int
# df_merged["Time"] = df_merged["Time"].astype(int)

df_merged2 = df_merged.copy()
df_merged2

# Group by "Exporter", "iso3" and "Time" and aggregate the data as a list of dictionaries
df_merged3 = (
    df_merged2.groupby(["Importer", "iso3", "Year"])
    .apply(lambda x: x[["name", "value"]].to_dict("records"))
    .reset_index(name="Data")
)

df_merged3

# Convert the grouped DataFrame to the desired JSON format
df_merged3.to_json(
    "../data/interim/food_imports_by_year.json", orient="records", default_handler=str
)

formatted_json = df_merged3.to_json(orient="records", default_handler=str)

formatted_json
