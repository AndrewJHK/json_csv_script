import pandas as pd
df = pd.read_csv("advantech_07_03_2025.csv")
print(df.head(1))
df.columns = ["header.counter", "header.timestamp_epoch",
              "data.N20_PRES.scaled", "data.CHAMBER_PRES.scaled",
              "data.N20.scaled", "data.FUEL.scaled",
              "data.CH4.scaled", "data.CH5.scaled",
              "data.CH6.scaled", "data.CH7.scaled",
              "data.CH8.scaled", "data.CH9.scaled",
              "data.CH10.scaled", "data.CH11.scaled",
              "data.CH12.scaled", "data.CH13.scaled",
              "data.CH14.scaled", "data.CH15.scaled"]
print(df.head(1))
df.to_csv("adv_07_03_2025_fixed.csv")