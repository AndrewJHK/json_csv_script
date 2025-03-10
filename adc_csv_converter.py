import pandas as pd

df = pd.read_csv("advantech_07_03_2025.csv")
print(df.head(1))
df.rename(columns={"num": "header.counter", "time": "header.timestamp_epoch", "channel0": "data.N20_PRES.scaled",
                   "channel1": "data.CHAMBER_PRES.scaled"
    , "channel2": "data.N20.scaled", "channel3": "data.FUEL.scaled", "channel4": "data.CH4.scaled",
                   "channel5": "data.CH5.scaled"
    , "channel6": "data.CH6.scaled", "channel7": "data.CH7.scaled", "channel8": "data.CH8.scaled",
                   "channel9": "data.CH9.scaled"
    , "channel10": "data.CH10.scaled", "channel11": "data.CH11.scaled", "channel12": "data.CH12.scaled",
                   "channel13": "data.CH13.scaled"
    , "channel14": "data.CH14.scaled", "channel15": "data.CH15.scaled"})

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
'''
import numpy as np

my_data=np.genfromtxt('biliq2_lpb.csv', delimiter=',')
print(my_data)


x = [(1, 2), (3, 4), (5, 6)]
#x = (1, 2)
y = []

if isinstance(x,tuple):
    y.append(x)
else:
    y.extend(x)

print(y)
for topic, quality in y:
    print(topic)
    print(quality)
    print("dupa")
    '''
