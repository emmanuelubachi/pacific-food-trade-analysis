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

# =====================================================================================

# Get commodity imports data

# =====================================================================================

df2 = df1[df["Exporter"].isin(pacific_countries)]
df2.head()
# df3 = df2[df["Time"].isin([2018, 2017, 2016, 2015])].reset_index(drop=True)

df2.head(200)
df2.shape

sorted(df2["Commodity"].unique())

# Group by both "Exporter" and "Commodity" columns and calculate the sum of "OBS_VALUE" for each combination
grouped_df = (
    df2.groupby(["Exporter", "Commodity", "Time"])["OBS_VALUE"].sum().reset_index()
)

grouped_df.shape

grouped_df.head(200)

grouped_df1 = grouped_df.rename(
    columns={
        "Exporter": "Country",
        "Commodity": "Food",
        "OBS_VALUE": "Export",
        "Time": "Year",
    }
)

df_ft = grouped_df1.copy()

df_ft

df_ft.shape

df_ft.isnull().sum()

df_ft.to_csv("../../data/staging/export_trade_trend.csv", index=False)
