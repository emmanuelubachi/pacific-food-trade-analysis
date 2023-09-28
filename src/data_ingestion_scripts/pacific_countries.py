import pandas as pd

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

df2 = df[["Importer", "ImporterISO"]].drop_duplicates().reset_index(drop=True)

df3 = df2.sort_values(by="Importer").reset_index(drop=True)

df3["id"] = range(1, len(df3) + 1)

df3 = df3[["Id", "Importer", "ImporterISO"]]

df4 = df3.rename(columns={"Importer": "country", "ImporterISO": "iso3"})

df4

df_json = df4.to_json("../../data/final/pacific_country.json", orient="records")
df_json
