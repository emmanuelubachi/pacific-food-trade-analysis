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
df.isnull().sum()

for i in df.columns:
    print("- ", i)
    print("-----------------------------------------------")
    print([df[i].unique()])
    print(" ")
    print(" ")


df1 = df.filter(["Importer", "Commodity", "Exporter", "Time", "OBS_VALUE"])
df1

pacific_countries = [
    "Australia",
    "Cook Islands",
    "Federated State of Micronesia",
    "Fiji",
    "French Polynesia",
    "Tonga",
    "Kiribati",
    "Marshall Islands",
    "Micronesia",
    "Nauru",
    "New Caledonia",
    "New Zealand",
    "Niue",
    "Palau",
    "Papua New Guinea",
    "Samoa",
    "Solomon Islands",
    "Tuvalu",
    "Vanuatu",
]

df2 = df1[df["Importer"].isin(pacific_countries)]
df2.head()

df2["Importer"].unique()

df3 = df2.copy()

df3.shape

# Rename the Importer to the offical country name

df3["Importer"] = df3["Importer"].replace(
    {"Micronesia": "Federated State of Micronesia"}
)
df3["Importer"].unique()

# Rename the Importer to the offical country name

correct_exporter_name = {
    "Bonaire, Saint Eustatius and Saba": "Bonaire, Sint Eustatius and Saba",
    "Bosnia Herzegovina": "Bosnia and Herzegovina",
    "Cocos Islands": "Cocos (Keeling) Islands",
    "France, Monaco": "Monaco",
    "Iran": "Iran (Islamic Republic of)",
    "Lao People's Dem. Rep.": "Lao People's Democratic Republic",
    "Micronesia": "Federated State of Micronesia",
    "Norway, Svalbard and Jan Mayen": "",
    "Plurinational State of Bolivia": "Bolivia (Plurinational State of)",
    "Polynesia": "French Polynesia",
    "Syria": "Syrian Arab Republic",
    "Taiwan and other Asia, nes.": "Taiwan",
    "Venezuela": "Venezuela (Bolivarian Republic of)",
    "The Former Yugoslav Republic of Macedonia": "The former Yugoslav Republic of Macedonia",
}

unknown_iso = {
    "Australia and New Zealand": "Rest of the World",
    "Belgium-Luxembourg": "Rest of the World",
    "Melanesia": "Rest of the World",
    "Netherlands Antilles": "Rest of the World",
    "Serbia and Montenegro": "Rest of the World",
    "Switzerland, Liechtenstein": "Rest of the World",
    "USA, Puerto Rico and US Virgin Islands": "Rest of the World",
    "Unknown": "Rest of the World",
}


df3["Exporter"] = df3["Exporter"].replace(correct_exporter_name)
df3["Exporter"] = df3["Exporter"].replace(unknown_iso)

df3["Exporter"].unique()

# ==========================================================================================================
# Read country iso data
df_country_iso = pd.read_csv("../../data/lookup/country-iso.csv")

df_country_iso2 = pd.DataFrame(df_country_iso)

df_country_iso2

df_country_iso2.head()

df_country_iso3 = df_country_iso2.sort_values(by="official_name")

importer_iso = df_country_iso3.rename(
    columns={"official_name": "Importer", "ISO3A": "ImporterISO"}
)

exporter_iso = df_country_iso3.rename(
    columns={"official_name": "Exporter", "ISO3A": "ExporterISO"}
)

importer_iso
exporter_iso

# ==========================================================================================================
# Merge Importer with country-iso

df_importer_iso = pd.merge(df3, importer_iso, how="left", on="Importer")

df_importer_iso.isnull().sum()

df_merged_m = df_importer_iso.copy()

# ================================
# confirm merge

missing = df_importer_iso[["Importer", "ImporterISO"]]

missing2 = missing[missing["ImporterISO"].isnull()]

missing2["Importer"].unique()


# ==========================================================================================================
#  Merge Exporter with country-iso

df_exporter_iso = pd.merge(df_merged_m, exporter_iso, how="left", on="Exporter")

df_exporter_iso.isnull().sum()

df_merged = df_exporter_iso.copy()

# confirm merge

missing = df_merged[["Exporter", "ExporterISO"]]

missing2 = missing[missing["ExporterISO"].isnull()]

sorted(missing2["Exporter"].unique())

df_merged_iso = df_merged[df_merged["ExporterISO"].notnull()]

# ===================================================================================
# df_merged_iso containes all the data with accurate Country name and ISO code
df_merged_iso.isnull().sum()

# Get data fro rest of the world without ISO3 codes
df_merged_row = df_merged[df_merged["Exporter"] == "Rest of the World"]

df_merged_iso

df_merged_row.isnull().sum()

df_food_import = pd.concat([df_merged_iso, df_merged_row])

df_merged_iso.shape
df_merged_row.shape

df_food_import = df_food_import.rename(
    columns={"Time": "Year", "OBS_VALUE": "Quantity"}
)

df_food_import.isnull().sum()
df_food_import.shape

df_food_import = df_food_import[
    [
        "Year",
        "Importer",
        "ImporterISO",
        "Exporter",
        "ExporterISO",
        "Commodity",
        "Quantity",
    ]
]

df_food_import.to_csv("../../data/staging/food_trade_imports.csv", index=False)

# ==================================================================================
# ==================================================================================
