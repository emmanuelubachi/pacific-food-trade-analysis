countries = [
    "Europe",
    "Asia",
    "Americas",
    "All",
    "Sub-Saharan Africa",
    "Latin America and the Caribbean",
    "Northern America",
    "South-eastern Asia",
    "Southern Asia",
    "Western Asia",
    "Eastern Europe",
    "Northern Europe",
    "Southern Europe",
    "Western Europe",
    "Australia",
    "Canada",
    "China, Hong Kong Special Administrative Region",
    "Singapore",
    "USA, Puerto Rico and US Virgin Islands",
    "Argentina",
    "Spain",
    "French Polynesia",
    "Thailand",
    "Japan",
    "China",
    "Taiwan and other Asia, nes.",
    "Indonesia",
    "Turkey",
    "Unknown",
    "Ecuador",
    "Lao People's Dem. Rep.",
    "Sri Lanka",
    "Kuwait",
    "New Zealand",
    "Melanesia",
    "Pacific Islands Countries and Territories",
    "Australia and New Zealand",
    "Africa",
    "Eastern Asia",
    "Malaysia",
    "Paraguay",
    "Kenya",
    "Vanuatu",
    "Portugal",
    "Viet Nam",
    "Hungary",
    "Southern African Customs Union",
    "Sudan",
    "Philippines",
    "Germany",
    "Papua New Guinea",
    "Andorra",
    "Ukraine",
    "Polynesia",
    "Oceania",
    "United Kingdom",
    "Republic of Korea",
    "Tonga",
    "Costa Rica",
    "Colombia",
    "Chile",
    "Poland",
    "Slovenia",
    "Uganda",
    "Switzerland, Liechtenstein",
    "Pakistan",
    "Mexico",
    "Ethiopia",
    "Gambia",
    "United Arab Emirates",
    "France, Monaco",
    "Cook Islands",
    "Brazil",
    "Samoa",
    "Bulgaria",
    "Fiji",
    "Guam",
    "Morocco",
    "Iran",
    "Northern Mariana Islands",
    "Solomon Islands",
    "New Caledonia",
    "Italy",
    "Republic of Moldova",
    "Belgium-Luxembourg",
    "Ireland",
    "Netherlands",
    "Marshall Islands",
    "Latvia",
    "Norway, Svalbard and Jan Mayen",
    "Greece",
    "India",
    "Austria",
    "Micronesia",
    "Denmark",
    "Kazakhstan",
    "Saudi Arabia",
    "Democratic People's Republic of Korea",
    "Palau",
    "Jamaica",
    "Bahrain",
    "Egypt",
    "Myanmar",
    "American Samoa",
    "Croatia",
    "Lithuania",
    "Chad",
    "Niger",
    "Northern Africa",
    "Czechia",
    "Saint Kitts and Nevis",
    "Panama",
    "Peru",
    "Serbia and Montenegro",
    "Algeria",
    "Syria",
    "Romania",
    "Bangladesh",
    "Lebanon",
    "Israel",
    "Trinidad and Tobago",
    "Federated State of Micronesia",
    "Estonia",
    "Russian Federation",
    "The Former Yugoslav Republic of Macedonia",
    "Plurinational State of Bolivia",
    "Barbados",
    "Dominican Republic",
    "Sweden",
    "Suriname",
    "Equatorial Guinea",
    "Senegal",
    "Tunisia",
    "British Virgin Islands",
    "El Salvador",
    "Uzbekistan",
    "Benin",
    "Malta",
    "Netherlands Antilles",
    "Cyprus",
    "Oman",
    "Antigua and Barbuda",
    "Dominica",
    "Curaçao",
    "Slovakia",
    "Armenia",
    "Madagascar",
    "Kyrgyzstan",
    "Georgia",
    "Guyana",
    "Central Asia",
    "Finland",
    "Cambodia",
    "Serbia",
    "Anguilla",
    "Uruguay",
    "Belize",
    "Côte d'Ivoire",
    "China, Macao Special Administrative Region",
    "Sao Tome and Principe",
    "Azerbaijan",
    "Mongolia",
    "Cuba",
    "Montserrat",
    "Mauritius",
    "Iraq",
    "Albania",
    "Mozambique",
    "Kiribati",
    "Liberia",
    "Comoros",
    "Mali",
    "Venezuela",
    "Sierra Leone",
    "Iceland",
    "Nepal",
    "Brunei Darussalam",
    "Tuvalu",
    "Guatemala",
    "Nicaragua",
    "Montenegro",
    "Cameroon",
    "Niue",
    "Mauritania",
    "Honduras",
    "Saint Lucia",
    "Belarus",
    "Bosnia Herzegovina",
    "Bahamas",
    "Jordan",
    "Zimbabwe",
    "Seychelles",
    "Yemen",
    "Switzerland",
    "Malawi",
    "Wallis and Futuna Islands",
    "Afghanistan",
    "Nauru",
    "United Republic of Tanzania",
    "Greenland",
    "Cocos Islands",
    "Central African Republic",
    "Bonaire, Saint Eustatius and Saba",
    "Burundi",
    "Ghana",
    "Togo",
    "Guinea",
    "British Indian Ocean Territories",
    "Cabo Verde",
    "Saint Helena",
    "Zambia",
    "Turkmenistan",
    "Nigeria",
    "Timor-Leste",
    "Rwanda",
]

# ====================================================================================

import pandas as pd
import datapackage

data_url = "https://datahub.io/core/geo-countries/datapackage.json"

# to load Data Package into storage
package = datapackage.Package(data_url)

# to load only tabular data
resources = package.resources
for resource in resources:
    if resource.tabular:
        data = pd.read_csv(resource.descriptor["path"])
        print(data)


# ====================================================================================

from datapackage import Package

package = Package("https://datahub.io/core/geo-countries/datapackage.json")

# print list of all resources:
print(package.resource_names)

# print processed tabular data (if exists any)
for resource in package.resources:
    if resource.descriptor["datahub"]["type"] == "derived/csv":
        print(resource.read())

# ====================================================================================

import pandas as pd
import sys

sys.path.append("../..")
from functions import DataCleaner as DC

# Set the maximum number of rows to display (e.g., 100)
pd.set_option("display.max_rows", 260)

# Define the path to the CSV file
file_path = "../../data/raw/country-codes_csv.csv"

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


col = [
    "official_name_en",
    "CLDR display name",
    "UNTERM English Formal",
    "UNTERM English Short",
    "ISO3166-1-Alpha-3",
    "ISO3166-1-numeric",
    "ISO3166-1-Alpha-2",
    "is_independent",
    "Region Name",
    "Sub-region Name",
    "Intermediate Region Name",
    "Intermediate Region Code",
    "Developed / Developing Countries",
]

df2 = df[col]

df2.isnull().sum()

df2.head(260)

df2[df2["ISO3166-1-Alpha-3"] == "GGY"]

# Delete rows based on a condition
df3 = df2[df2["official_name_en"] != "Sark"]
df3.shape

for i in df3.columns:
    print("- ", i)
    print("-----------------------------------------------")
    print([df3[i].unique()])
    print(" ")
    print(" ")

df4 = df3[["official_name_en", "ISO3166-1-Alpha-3"]]
df4.isnull().sum()

df4.head(260)

df4.to_csv("../../data/lookup/country-iso.csv", index=False)


pacific_countries = [
    "Afghanistan",
    "Africa",
    "All",
    "American Samoa",
    "Americas",
    "Andorra",
    "Angola",
    "Antigua and Barbuda",
    "Argentina",
    "Armenia",
    "Asia",
    "Australia",
    "Australia and New Zealand",
    "Austria",
    "Azerbaijan",
    "Bahrain",
    "Bangladesh",
    "Barbados",
    "Belarus",
    "Belgium-Luxembourg",
    "Belize",
    "Bermuda",
    "Bosnia Herzegovina",
    "Brazil",
    "Brunei Darussalam",
    "Bulgaria",
    "Cambodia",
    "Cameroon",
    "Canada",
    "Central Asia",
    "Chile",
    "China",
    "China, Hong Kong Special Administrative Region",
    "China, Macao Special Administrative Region",
    "Cocos Islands",
    "Colombia",
    "Cook Islands",
    "Costa Rica",
    "Croatia",
    "Cuba",
    "Curaçao",
    "Cyprus",
    "Czechia",
    "Democratic People's Republic of Korea",
    "Democratic Republic of the Congo",
    "Denmark",
    "Dominica",
    "Dominican Republic",
    "Eastern Asia",
    "Eastern Europe",
    "Ecuador",
    "Egypt",
    "El Salvador",
    "Estonia",
    "Europe",
    # "Federated State of Micronesia",
    "Fiji",
    "Finland",
    "France, Monaco",
    "French Polynesia",
    "Gambia",
    "Georgia",
    "Germany",
    "Ghana",
    "Greece",
    "Greenland",
    "Grenada",
    "Guam",
    "Guinea",
    "Guyana",
    "Honduras",
    "Hungary",
    "Iceland",
    "India",
    "Indonesia",
    "Iran",
    "Iraq",
    "Ireland",
    "Israel",
    "Italy",
    "Jamaica",
    "Japan",
    "Jordan",
    "Kazakhstan",
    "Kenya",
    "Kiribati",
    "Kuwait",
    "Lao People's Dem. Rep.",
    "Latin America and the Caribbean",
    "Latvia",
    "Lebanon",
    "Lithuania",
    "Madagascar",
    "Malawi",
    "Malaysia",
    "Maldives",
    "Mali",
    "Malta",
    "Marshall Islands",
    "Mauritius",
    "Melanesia",
    "Mexico",
    "Micronesia",
    "Mongolia",
    "Montenegro",
    "Morocco",
    "Mozambique",
    "Myanmar",
    "Nauru",
    "Netherlands",
    "Netherlands Antilles",
    "New Caledonia",
    "New Zealand",
    "Nicaragua",
    "Niger",
    "Nigeria",
    "Niue",
    "Norfolk Islands",
    "Northern Africa",
    "Northern America",
    "Northern Europe",
    "Northern Mariana Islands",
    "Norway, Svalbard and Jan Mayen",
    "Oceania",
    "Oman",
    "PICT Unknown",
    "Pacific Islands Countries and Territories",
    "Pakistan",
    "Palau",
    "Panama",
    "Papua New Guinea",
    "Philippines",
    "Pitcairn",
    "Poland",
    "Polynesia",
    "Portugal",
    "Qatar",
    "Republic of Korea",
    "Republic of Moldova",
    "Romania",
    "Russian Federation",
    "Saint Kitts and Nevis",
    "Saint Lucia",
    "Samoa",
    "Saudi Arabia",
    "Serbia",
    "Serbia and Montenegro",
    "Seychelles",
    "Sierra Leone",
    "Singapore",
    "Slovakia",
    "Slovenia",
    "Solomon Islands",
    "South-eastern Asia",
    "Southern African Customs Union",
    "Southern Asia",
    "Southern Europe",
    "Spain",
    "Sri Lanka",
    "State of Palestine",
    "Sub-Saharan Africa",
    "Suriname",
    "Sweden",
    "Switzerland, Liechtenstein",
    "Syria",
    "Taiwan and other Asia, nes.",
    "Thailand",
    "The Former Yugoslav Republic of Macedonia",
    "Timor-Leste",
    "Togo",
    "Tokelau",
    "Tonga",
    "Trinidad and Tobago",
    "Tunisia",
    "Turkey",
    "Tuvalu",
    "USA, Puerto Rico and US Virgin Islands",
    "Ukraine",
    "United Arab Emirates",
    "United Kingdom",
    "United Republic of Tanzania",
    "Unknown",
    "Uruguay",
    "Vanuatu",
    "Viet Nam",
    "Wallis and Futuna Islands",
    "Western Asia",
    "Western Europe",
]
