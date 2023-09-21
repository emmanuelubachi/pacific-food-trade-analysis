import pandas as pd

pacific_countries = {
    "country": [
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
}

df = pd.DataFrame(pacific_countries)
df2 = sorted(df["country"])

df3 = pd.DataFrame(df2)

df3.to_csv("../data/processed/pacific_country.csv")

df_csv = pd.read_csv("../data/processed/pacific_country.csv")

df_json = df_csv.to_json()
df_json

df_csv.to_json("../data/processed/pacific_country.json", orient="records")
