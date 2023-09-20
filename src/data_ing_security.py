import pandas as pd
import sys

sys.path.append("..")
from functions import DataCleaner as DC

# Define the path to the CSV file
file_path = "data/raw/pacific-food/security/SPC-DF_FOOD_SECURITY_HIES_3-1.0-all.csv"

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
df.isnull()

df["INDICATOR: Indicator"].count()
df["INDICATOR: Indicator"].nunique()

for i in df.columns:
    print("- ", i)
    print("-----------------------------------------------")
    print([df[i].unique()])
    print(" ")
    print(" ")


df_1 = DC.rename_columns_remove_colon(df)

# Print the DataFrame with renamed columns
print(df_1)

df1 = df_1.copy()

df1 = DC.process_columns_with_colon(
    df1,
    [
        "DATAFLOW",
        "Frequency",
        "Pacific Island Countries and territories",
        "Indicator",
        "Sex",
        "Age",
        "Urbanization",
        "Poverty",
        "Food security",
        "Food category",
        "Unit of measure",
    ],
)
