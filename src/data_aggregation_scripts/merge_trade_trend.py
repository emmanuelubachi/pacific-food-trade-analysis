import pandas as pd

# Set the maximum number of rows to display (e.g., 100)
pd.set_option("display.max_rows", 200)

# Define the path to the CSV file
file_path = "../../data/staging/export_trade_trend.csv"
file_path_2 = "../../data/staging/import_trade_trend.csv"

try:
    # Read the CSV file into a Pandas DataFrame
    df_raw = pd.read_csv(file_path)
    df_raw2 = pd.read_csv(file_path_2)

    # Display the first few rows of the DataFrame to verify data ingestion
    # print(df_raw.head())
    df_copy = df_raw.copy()
    df_x = pd.DataFrame(df_copy)
    print(df_x.head())

    df_copy2 = df_raw2.copy()
    df_m = pd.DataFrame(df_copy2)
    print(df_m.head())

    # You can now perform data cleaning, transformation, and exploration on the DataFrame.
    # For example, you can perform operations like df.describe(), df.isnull().sum(), etc.
except FileNotFoundError:
    print(f"File not found: {file_path, file_path_2}")
except Exception as e:
    print(f"An error occurred: {str(e)}")

df_x.shape
df_m.shape

for i in df_x.columns:
    print("- ", i)
    print("-----------------------------------------------")
    print([df_x[i].unique()])
    print(" ")
    print(" ")

for i in df_m.columns:
    print("- ", i)
    print("-----------------------------------------------")
    print([df_m[i].unique()])
    print(" ")
    print(" ")

# =====================================================================================

# Merge Food Trade data to Pacific County ISO3 Data

# =====================================================================================

df_merged = pd.merge(df_x, df_m, how="outer", on=["Country", "Food", "Year"])

df_merged

df_merged.isnull().sum()

df_merged2 = df_merged.copy()

df_merged3 = df_merged2.fillna(0)

df_merged3.isnull().sum()


# =====================================================================================

# Merge Food Trade data to Pacific County ISO3 Data

# =====================================================================================

df_pc = pd.read_csv("../../data/lookup/pacific_country.csv")

df_pc

df_merged4 = pd.merge(df_merged3, df_pc, how="left", on="Country")

df_merged4

df_merged4.isnull().sum()

df_merged5 = df_merged4.copy()
df_merged5

# Group by "Exporter", "iso3" and "Time" and aggregate the data as a list of dictionaries
df_merged6 = (
    df_merged5.groupby(["Country", "iso3", "Food"])
    .apply(lambda x: x[["Year", "Export", "Import"]].to_dict("records"))
    .reset_index(name="Data")
)

df_merged6

# Convert the grouped DataFrame to the desired JSON format
# df_merged6.to_json(
#     "../../data/final/food_trade_trend.json", orient="records", default_handler=str
# )

formatted_json = df_merged6.to_json(orient="records", default_handler=str)

formatted_json
