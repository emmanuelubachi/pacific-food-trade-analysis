import pandas as pd
import sys

sys.path.append("../..")
from functions import DataCleaner as DC

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

df1 = df.filter(["Importer", "ImporterISO", "Year", "Quantity"])
df1

# =====================================================================================

# Get commodity imports data

# =====================================================================================

# Group by both "Importer" and "Year" columns and calculate the sum of "Quantity" for each combination
df2 = df1.groupby(["Importer", "ImporterISO", "Year"])["Quantity"].sum().reset_index()

df2

df2.shape

df2.head(200)

df3 = (
    df2.groupby(["Importer", "ImporterISO"])
    .apply(lambda x: x[["Year", "Quantity"]].to_dict("records"))
    .reset_index(name="Data")
)

df3_json = df3.to_json("../../data/final/import_trend.json", orient="records")
