import pandas as pd

df_raw_security = pd.read_csv(
    "../data/raw/pacific-food/security/SPC-DF_FOOD_SECURITY_HIES_3-1.0-all.csv",
    encoding="utf-8",
)

df_raw_trade = pd.read_csv(
    "../data/raw/pacific-food/trade/SPC-DF_TRADE_FOOD-1.0-all.csv",
    encoding="utf-8",
)

df_raw_security.shape
df_raw_trade.shape

df_raw_security.head()
df_raw_trade.head()

df_raw_security.columns
