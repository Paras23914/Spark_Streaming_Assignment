import pandas as pd

data1= pd.read_csv('AV-GPS-Dataset-1.csv')
data2= pd.read_csv('AV-GPS-Dataset-2.csv')
data3= pd.read_csv('AV-GPS-Dataset-3.csv')

data = pd.concat([data1, data2, data3], ignore_index=True)
data["Vehicle ID"]="Vehicle 1"
data=data[["Vehicle ID",
    "Clock Date",
    "Clock Time",
    "Velocity (m/s)"]]

data = data.dropna(subset=["Clock Date", "Clock Time"])

data["Timestamp"] = pd.to_datetime(
    data["Clock Date"] + " " + data["Clock Time"]
)

data["Speed (km/h)"] = data["Velocity (m/s)"] * 3.6
data = data[["Vehicle ID", "Timestamp", "Speed (km/h)"]]

data = data.drop_duplicates()

data.to_csv("processed_data.csv", index=False)

print (data.info())

print(data.head(10))

print(data["Speed (km/h)"].describe())