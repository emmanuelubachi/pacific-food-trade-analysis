import pandas as pd
import sys

sys.path.append("../..")
from functions import Calculations as ca

# Set the maximum number of rows to display (e.g., 100)
pd.set_option("display.max_rows", 200)

# Define the path to the CSV file
file_path = "../../data/staging/food_trade_imports.csv"

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


df.describe()

df.isnull().sum()

df2 = df[
    [
        "Importer",
        "ImporterISO",
        "Year",
        "Commodity",
        "Exporter",
        "ExporterISO",
        "Quantity",
    ]
]

df2.sort_values(
    by=[
        "Importer",
        "ImporterISO",
        "Year",
        "Commodity",
        "Exporter",
        "ExporterISO",
        "Quantity",
    ]
)

df3 = (
    df2.groupby(
        ["Importer", "ImporterISO", "Year", "Exporter", "ExporterISO"]
    )["Quantity"]
    .sum()
    .reset_index()
)

df4 = df3.copy()

df4


def filter_latest_5_years(dataframe):
    latest_years = dataframe["Year"].max() - 4
    filtered_data = dataframe[dataframe["Year"] >= latest_years]
    return filtered_data


# Group by Importer and apply the filter_latest_5_years function
df5 = df4.groupby("Importer", group_keys=False).apply(filter_latest_5_years)

df6 = df5.rename(columns={"Exporter": "name", "Quantity":"value", "ExporterISO":"ISO_A3"})

df6.shape

df6

# df6_json = df6.to_json("../../data/interim/food_imports.json", orient="records")


# Group by Importer, ImporterISO and "Year" and aggregate the data as a list of dictionaries
df7 = (
    df6.groupby(["Importer", "ImporterISO", "Year"])
    .apply(lambda x: x[["name", "ISO_A3", "value"]].to_dict("records"))
    .reset_index(name="Data")
)

df7.shape
df7

# =========================================================================================


df7_json = df7.to_json("../../data/interim/food_imports.json", orient="records")
