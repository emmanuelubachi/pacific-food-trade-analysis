import pandas as pd
import sys

sys.path.append("..")
from functions import DataCleaner as DC

# Set the maximum number of rows to display (e.g., 100)
pd.set_option("display.max_rows", 100)


# Define the path to the CSV file
file_path = "../data/raw/pacific-food/trade/SPC-DF_TRADE_FOOD-1.0-all.csv"

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


# =================================================================================================

df.shape
df.describe()
df.isnull().sum()
df.dtypes

for i in df.columns:
    print("- ", i)
    print("-----------------------------------------------")
    print([df[i].unique()])
    print(" ")
    print(" ")

sorted(df["COMMODITY: Commodity"].unique())
sorted(df["TIME_PERIOD: Time"].unique())

df["IMPORTER: Importer"].nunique()
df["EXPORTER: Exporter"].nunique()

df_1 = DC.rename_columns_remove_colon(df)

# Print the DataFrame with renamed columns
print(df_1)

df1 = df_1.copy()

df1.columns

for i in df1.columns:
    print("- ", i)
    print("-----------------------------------------------")
    print([df1[i].unique()])
    print(" ")
    print(" ")


df2 = DC.process_columns_with_colon(
    df1,
    [
        "DATAFLOW",
        "Frequency",
        "Indicator",
        "Importer",
        "Exporter",
        "Commodity",
        "Unit of measure",
    ],
)

for i in df2.columns:
    print("- ", i)
    print("-----------------------------------------------")
    print([df2[i].unique()])
    print(" ")
    print(" ")

df2["Importer"].nunique()
df2["Exporter"].nunique()


df3 = df2[
    [
        "DATAFLOW",
        "Frequency",
        "Indicator",
        "Importer",
        "Exporter",
        "Commodity",
        "Time",
        "OBS_VALUE",
        "Unit of measure",
    ]
]

df3.head(100)

# df3.to_csv("../data/staging/food_trade.csv", index=False)

df3.shape
df3.isnull().sum()
df3.describe()
